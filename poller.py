import json
import os
import time
import math
import re
import socket
import requests
import datetime
import threading
from sqlalchemy.orm import Session
from database import SessionLocal, WeatherRecord

STATIONS = {
    "KALMILLP10": {"name": "Millport Primary", "lat": 33.544, "lon": -88.133, "cwop_callsign": "GW7151"},
    "KALKENNE5":  {"name": "Kennedy Station",   "lat": 33.587, "lon": -88.080, "cwop_callsign": "FW4617"},
    "KALMILLP8":  {"name": "Millport Alt",      "lat": 33.540, "lon": -88.100},
}

STATION_CONFIG_PATH = "station_config.json"

# Open-Meteo polling config
OPENMETEO_STATIONS = {"KALMILLP10", "KALKENNE5", "KALMILLP8"}
# Persist a fresh observation roughly once per minute.
POLL_INTERVAL_SECONDS = 60
STATION_REQUEST_GAP_SECONDS = 1
INTEGRITY_SLOT_MINUTES = 60  # expected observation interval (hourly from Open-Meteo)

# ── APRS-IS / CWOP config ─────────────────────────────────────────────────────
# Derived maps – rebuilt after loading station_config.json
CWOP_TO_STATION: dict = {}
CWOP_CALLSIGNS: set = set()
APRS_IS_HOST   = "rotate.aprs2.net"
APRS_IS_PORT   = 14580
# Range filter centred between the two CWOP stations (100 km radius).
# The b/ budlist filter requires a verified ham-radio login; r/ + t/w does not.
APRS_IS_FILTER = "t/w r/33.74/-88.14/100"


def _rebuild_cwop_maps():
    global CWOP_TO_STATION, CWOP_CALLSIGNS
    CWOP_TO_STATION = {info["cwop_callsign"]: sid
                       for sid, info in STATIONS.items() if info.get("cwop_callsign")}
    CWOP_CALLSIGNS = set(CWOP_TO_STATION.keys())


def load_station_config():
    """Read station_config.json and apply cwop_callsign overrides, then rebuild maps."""
    if os.path.exists(STATION_CONFIG_PATH):
        try:
            with open(STATION_CONFIG_PATH) as f:
                config = json.load(f)
            for sid, overrides in config.items():
                if sid in STATIONS and "cwop_callsign" in overrides:
                    STATIONS[sid]["cwop_callsign"] = overrides["cwop_callsign"] or ""
            print(f"[config] Loaded {STATION_CONFIG_PATH}")
        except Exception as e:
            print(f"[config] Failed to load {STATION_CONFIG_PATH}: {e}")
    _rebuild_cwop_maps()


load_station_config()


def update_cwop_link(station_id: str, cwop_callsign: str):
    """Change or clear the CWOP callsign link for a station, and persist to JSON."""
    if station_id not in STATIONS:
        return
    cs = (cwop_callsign or "").strip().upper()
    STATIONS[station_id]["cwop_callsign"] = cs
    _rebuild_cwop_maps()
    config = {}
    if os.path.exists(STATION_CONFIG_PATH):
        try:
            with open(STATION_CONFIG_PATH) as f:
                config = json.load(f)
        except Exception:
            pass
    if station_id not in config:
        config[station_id] = {}
    config[station_id]["cwop_callsign"] = cs
    with open(STATION_CONFIG_PATH, "w") as f:
        json.dump(config, f, indent=2)
    print(f"[config] {station_id} cwop_callsign = '{cs}'")


def _utcnow_naive():
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


# ── APRS packet parsing ───────────────────────────────────────────────────────
# Individual field regexes – fields may appear in any order after the _ symbol.
_RE_WDIR  = re.compile(r'_(\d{3})/')          # wind direction (degrees)
_RE_WSPD  = re.compile(r'_\d{3}/(\d{3})')     # wind speed (mph)
_RE_WGST  = re.compile(r'g(\d{3})')           # wind gust (mph)
_RE_TEMP  = re.compile(r't(-?\d{1,3})')       # temperature (°F)
_RE_RAIN1 = re.compile(r'r(\d{1,4})')         # rain last hour (0.01 in)
_RE_RAIN0 = re.compile(r'P(\d{1,4})')         # rain since midnight (0.01 in)
_RE_HUM   = re.compile(r'h(\d{2})')           # humidity (%, 00 = 100)
_RE_BARO  = re.compile(r'b(\d{4,5})')         # pressure (0.1 hPa)
_RE_LUX   = re.compile(r'[Ll](\d{1,4})')      # solar radiation (W/m²)
_RE_LAT   = re.compile(r'(\d{4}\.\d{2})([NS])')
_RE_LON   = re.compile(r'(\d{5}\.\d{2})([EW])')


def _aprs_field(pat, text, scale=1.0, default=None):
    m = pat.search(text)
    if not m:
        return default
    try:
        return float(m.group(1)) * scale
    except (ValueError, TypeError):
        return default


def _dew_point(temp_f, rh):
    """Magnus-formula dew point.  Inputs: temp °F, RH %.  Returns °F."""
    if temp_f is None or rh is None:
        return None
    tc = (temp_f - 32) * 5 / 9
    g = math.log(max(rh, 1) / 100.0) + 17.62 * tc / (243.12 + tc)
    dp_c = 243.12 * g / (17.62 - g)
    return round(dp_c * 9 / 5 + 32, 1)


def _ddmm_to_dec(ddmm_str, hemi):
    """Convert APRS DDMM.MM[NS/EW] to signed decimal degrees."""
    v = float(ddmm_str)
    deg = int(v / 100)
    dec = deg + (v - deg * 100) / 60.0
    return round(-dec if hemi in ('S', 'W') else dec, 5)


def parse_aprs_wx(callsign, packet_body):
    """
    Parse an APRS weather packet body string.
    Returns a weather-record dict suitable for save_weather_record(), or None
    if the packet has no temperature (i.e. is not a real wx packet).
    Also updates STATIONS[callsign] lat/lon the first time coordinates are seen.
    """
    if '_' not in packet_body:
        return None

    temp_f = _aprs_field(_RE_TEMP, packet_body)
    if temp_f is None:
        return None

    hum = _aprs_field(_RE_HUM, packet_body)
    if hum is not None:
        hum = 100.0 if hum == 0.0 else hum   # h00 encodes 100 %

    baro_01hpa = _aprs_field(_RE_BARO, packet_body)
    pressure   = _hpa_to_inhg(baro_01hpa / 10.0) if baro_01hpa else None

    wdir = _aprs_field(_RE_WDIR, packet_body)
    wspd = _aprs_field(_RE_WSPD, packet_body)
    wgst = _aprs_field(_RE_WGST, packet_body)

    # Auto-update station coordinates the first time we see a position fix
    sid = callsign.upper()
    if sid in STATIONS and STATIONS[sid].get("lat") is None:
        m_lat = _RE_LAT.search(packet_body)
        m_lon = _RE_LON.search(packet_body)
        if m_lat and m_lon:
            STATIONS[sid]["lat"] = _ddmm_to_dec(m_lat.group(1), m_lat.group(2))
            STATIONS[sid]["lon"] = _ddmm_to_dec(m_lon.group(1), m_lon.group(2))
            print(f"[APRS-IS] Updated coords for {sid}: "
                  f"{STATIONS[sid]['lat']}, {STATIONS[sid]['lon']}")

    return {
        "station_id":      sid,
        "timestamp":       _utcnow_naive(),
        "temperature":     temp_f,
        "humidity":        hum,
        "dew_point":       _dew_point(temp_f, hum),
        "heat_index":      None,
        "wind_chill":      None,
        "wind_speed":      wspd,
        "wind_dir":        int(wdir) if wdir is not None else None,
        "wind_gust":       wgst,
        "pressure":        pressure,
        "precip_rate":     _aprs_field(_RE_RAIN1, packet_body, scale=0.01),
        "precip_total":    _aprs_field(_RE_RAIN0, packet_body, scale=0.01),
        "solar_radiation": _aprs_field(_RE_LUX,   packet_body),
        "uv_index":        None,
    }


# ── APRS-IS listener ──────────────────────────────────────────────────────────
def aprs_listener_loop():
    """
    Persistent receive-only connection to APRS-IS.
    Uses a range+type filter (works without a verified ham-radio login).
    Filters arriving packets client-side to CWOP_CALLSIGNS only.
    Auto-reconnects on any error.
    """
    if not CWOP_CALLSIGNS:
        return

    while True:
        sock = None
        db   = None
        try:
            print(f"[APRS-IS] Connecting to {APRS_IS_HOST}:{APRS_IS_PORT} …")
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(120)           # 2-min idle timeout; server sends keepalives
            sock.connect((APRS_IS_HOST, APRS_IS_PORT))
            sock.recv(256)                 # banner

            sock.sendall(b"user N0CALL pass -1 vers WxDash 1.0\r\n")
            time.sleep(0.4)
            sock.recv(256)                 # logresp

            sock.sendall(f"#filter {APRS_IS_FILTER}\r\n".encode())
            print(f"[APRS-IS] Filter: {APRS_IS_FILTER}")
            print(f"[APRS-IS] Watching for: {', '.join(sorted(CWOP_CALLSIGNS))}")

            db  = SessionLocal()
            buf = ""
            while True:
                chunk = sock.recv(4096)
                if not chunk:
                    raise ConnectionError("Server closed connection")
                buf += chunk.decode("utf-8", errors="replace")
                lines = buf.split("\n")
                buf   = lines[-1]           # keep incomplete trailing line
                for line in lines[:-1]:
                    line = line.strip()
                    if not line or line.startswith("#"):
                        continue
                    callsign = line.split(">")[0].upper()
                    if callsign not in CWOP_CALLSIGNS:
                        continue
                    body = line.split(":", 1)[1] if ":" in line else ""
                    station_id = CWOP_TO_STATION.get(callsign, callsign)
                    record = parse_aprs_wx(station_id, body)
                    if record:
                        save_weather_record(db, record)
                        tag = f"{callsign} → {station_id}" if station_id != callsign else callsign
                        print(f"[APRS-IS] {tag}: {record['temperature']}°F  "
                              f"hum={record['humidity']}%  "
                              f"pres={record['pressure']} inHg")

        except Exception as exc:
            print(f"[APRS-IS] Lost connection: {exc}. Reconnecting in 30 s …")
        finally:
            if db:
                try: db.close()
                except Exception: pass
            if sock:
                try: sock.close()
                except Exception: pass
        time.sleep(30)


def _http_get_json(url, timeout=20, attempts=3, backoff=1.5):
    last_err = None
    for attempt in range(1, attempts + 1):
        try:
            response = requests.get(url, timeout=timeout)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            last_err = e
            if attempt < attempts:
                sleep_s = backoff ** (attempt - 1)
                time.sleep(sleep_s)
    if last_err is not None:
        raise last_err
    raise RuntimeError("HTTP request failed without exception details")


def _hpa_to_inhg(hpa):
    if hpa is None:
        return None
    return round(float(hpa) * 0.029529983071445, 2)


def _normalize_station(station_id):
    sid = (station_id or "").upper()
    info = STATIONS.get(sid)
    if not info:
        return None, None
    return sid, info


def _open_meteo_current_url(lat, lon):
    return (
        "https://api.open-meteo.com/v1/forecast"
        f"?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "wind_speed_10m,wind_direction_10m,wind_gusts_10m,surface_pressure,precipitation,"
        "shortwave_radiation,uv_index"
        "&temperature_unit=fahrenheit"
        "&wind_speed_unit=mph"
        "&precipitation_unit=inch"
        "&timezone=UTC"
    )


def _open_meteo_history_url(lat, lon, day):
    return (
        "https://archive-api.open-meteo.com/v1/archive"
        f"?latitude={lat}&longitude={lon}"
        f"&start_date={day}&end_date={day}"
        "&hourly=temperature_2m,relative_humidity_2m,dew_point_2m,apparent_temperature,"
        "wind_speed_10m,wind_direction_10m,wind_gusts_10m,surface_pressure,precipitation,"
        "shortwave_radiation,uv_index"
        "&temperature_unit=fahrenheit"
        "&wind_speed_unit=mph"
        "&precipitation_unit=inch"
        "&timezone=UTC"
    )

def fetch_current_weather(station_id):
    sid, info = _normalize_station(station_id)
    if not info:
        return None
    url = _open_meteo_current_url(info["lat"], info["lon"])
    try:
        data = _http_get_json(url, timeout=20, attempts=3, backoff=1.7)
        current = data.get("current", {})
        if current:
            return {
                "station_id": sid,
            # Use poll time to preserve a continuous local time-series even when
            # upstream "current.time" only advances infrequently.
            "timestamp": _utcnow_naive(),
                "temperature": current.get("temperature_2m"),
                "humidity": current.get("relative_humidity_2m"),
                "dew_point": current.get("dew_point_2m"),
                "heat_index": current.get("apparent_temperature"),
                "wind_chill": None,
                "wind_speed": current.get("wind_speed_10m"),
                "wind_dir": current.get("wind_direction_10m"),
                "wind_gust": current.get("wind_gusts_10m"),
                "pressure": _hpa_to_inhg(current.get("surface_pressure")),
                "precip_rate": current.get("precipitation"),
                "precip_total": current.get("precipitation"),
                "solar_radiation": current.get("shortwave_radiation"),
                "uv_index": current.get("uv_index"),
            }
    except Exception as e:
        print(f"Error fetching current weather for {sid or station_id}: {e}")
    return None

def fetch_historical_weather(station_id, date_str):
    sid, info = _normalize_station(station_id)
    if not info:
        return []

    if len(date_str) == 8:
        day = datetime.datetime.strptime(date_str, "%Y%m%d").date().isoformat()
    else:
        day = date_str

    url = _open_meteo_history_url(info["lat"], info["lon"], day)
    records = []
    try:
        data = _http_get_json(url, timeout=25, attempts=3, backoff=1.8)
        hourly = data.get("hourly", {})
        times = hourly.get("time", [])
        for i, ts in enumerate(times):
            try:
                timestamp = datetime.datetime.fromisoformat(ts).replace(tzinfo=None)
            except Exception:
                continue

            def hv(key):
                values = hourly.get(key, [])
                return values[i] if i < len(values) else None

            records.append({
                "station_id": sid,
                "timestamp": timestamp,
                "temperature": hv("temperature_2m"),
                "humidity": hv("relative_humidity_2m"),
                "dew_point": hv("dew_point_2m"),
                "heat_index": hv("apparent_temperature"),
                "wind_chill": None,
                "wind_speed": hv("wind_speed_10m"),
                "wind_dir": hv("wind_direction_10m"),
                "wind_gust": hv("wind_gusts_10m"),
                "pressure": _hpa_to_inhg(hv("surface_pressure")),
                "precip_rate": hv("precipitation"),
                "precip_total": hv("precipitation"),
                "solar_radiation": hv("shortwave_radiation"),
                "uv_index": hv("uv_index"),
            })
    except Exception as e:
        print(f"Error fetching historical weather for {sid or station_id} {date_str}: {e}")
    return records


def save_weather_record(db: Session, record_data: dict):
    existing = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == record_data.get('station_id'),
        WeatherRecord.timestamp == record_data['timestamp']
    ).first()
    if not existing:
        db_record = WeatherRecord(**record_data)
        db.add(db_record)
        db.commit()
        db.refresh(db_record)
        return db_record
    return existing

def backfill():
    db = SessionLocal()
    try:
        for station_id in STATIONS.keys():
            latest = db.query(WeatherRecord).filter(
                WeatherRecord.station_id == station_id
            ).order_by(WeatherRecord.timestamp.desc()).first()

            if latest and (_utcnow_naive() - latest.timestamp).days < 1:
                print(f"Recent data found for {station_id}. Skipping backfill.")
                continue

            print(f"Starting 5-day backfill for {station_id}...")
            today = _utcnow_naive().date()
            for i in range(5):
                past_date = today - datetime.timedelta(days=i)
                date_str = past_date.strftime("%Y%m%d")
                print(f"  Backfilling {station_id} {date_str}...")
                records = fetch_historical_weather(station_id, date_str)
                for rec in records:
                    save_weather_record(db, rec)
                time.sleep(1)
            print(f"Backfill complete for {station_id}.")
    finally:
        db.close()

def _round_to_slot(ts):
    """Floor a datetime to the nearest INTEGRITY_SLOT_MINUTES boundary."""
    slot_min = (ts.minute // INTEGRITY_SLOT_MINUTES) * INTEGRITY_SLOT_MINUTES
    return ts.replace(minute=slot_min, second=0, microsecond=0)


def check_integrity(days=5):
    """
    Scan the database for the last *days* days and return a per-station report of:
      - corrupt_count  : records where temperature IS NULL
      - missing_slots  : list of ISO-format slot timestamps with no coverage
      - missing_dates  : unique dates (YYYYMMDD) that contain missing slots

    Does NOT modify the database.
    """
    db = SessionLocal()
    try:
        now = _utcnow_naive()
        cutoff = now - datetime.timedelta(days=days)
        report = {}

        for station_id in STATIONS.keys():
            # ── Corrupt records ──────────────────────────────────────────
            corrupt_count = db.query(WeatherRecord).filter(
                WeatherRecord.station_id == station_id,
                WeatherRecord.timestamp >= cutoff,
                WeatherRecord.temperature == None,
            ).count()

            # ── Missing hourly slots ─────────────────────────────────────
            rows = db.query(WeatherRecord.timestamp).filter(
                WeatherRecord.station_id == station_id,
                WeatherRecord.timestamp >= cutoff,
                WeatherRecord.temperature != None,
            ).all()

            # Build a set of covered slot boundaries
            covered = set(_round_to_slot(r.timestamp) for r in rows)

            # Walk every expected slot from cutoff to now
            slot = _round_to_slot(cutoff)
            missing = []
            while slot < now:
                if slot not in covered:
                    missing.append(slot)
                slot += datetime.timedelta(minutes=INTEGRITY_SLOT_MINUTES)

            report[station_id] = {
                "corrupt_count": corrupt_count,
                "missing_slot_count": len(missing),
                "missing_slots": [s.strftime("%Y-%m-%dT%H:%MZ") for s in missing],
                "missing_dates": sorted(set(s.strftime("%Y%m%d") for s in missing)),
            }

        return report
    finally:
        db.close()


def repair_integrity(days=5):
    """
    1. Remove corrupt records (temperature IS NULL) for the last *days* days.
    2. Backfill every date that has at least one missing 30-min slot.

    Returns a per-station summary dict.
    """
    db = SessionLocal()
    try:
        now = _utcnow_naive()
        cutoff = now - datetime.timedelta(days=days)
        summary = {}

        for station_id in STATIONS:
            station_summary = {
                "corrupt_removed": 0,
                "dates_backfilled": [],
            }

            # ── Step 1: Delete corrupt records ───────────────────────────
            corrupt = db.query(WeatherRecord).filter(
                WeatherRecord.station_id == station_id,
                WeatherRecord.timestamp >= cutoff,
                WeatherRecord.temperature == None,
            ).all()
            if corrupt:
                print(f"[integrity] {station_id}: removing {len(corrupt)} corrupt record(s)...")
                for rec in corrupt:
                    db.delete(rec)
                db.commit()
                station_summary["corrupt_removed"] = len(corrupt)

            # ── Step 2: Find missing hourly slots ────────────────────────
            rows = db.query(WeatherRecord.timestamp).filter(
                WeatherRecord.station_id == station_id,
                WeatherRecord.timestamp >= cutoff,
                WeatherRecord.temperature != None,
            ).all()
            covered = set(_round_to_slot(r.timestamp) for r in rows)

            slot = _round_to_slot(cutoff)
            missing_dates = set()
            while slot < now:
                if slot not in covered:
                    missing_dates.add(slot.strftime("%Y%m%d"))
                slot += datetime.timedelta(minutes=INTEGRITY_SLOT_MINUTES)

            if not missing_dates:
                print(f"[integrity] {station_id}: no missing slots found.")
            else:
                print(f"[integrity] {station_id}: backfilling {len(missing_dates)} date(s): "
                      f"{sorted(missing_dates)}")
                for date_str in sorted(missing_dates):
                    print(f"[integrity] {station_id}: fetching history for {date_str}...")
                    records = fetch_historical_weather(station_id, date_str)
                    for rec in records:
                        save_weather_record(db, rec)
                    station_summary["dates_backfilled"].append(date_str)
                    time.sleep(1)

            summary[station_id] = station_summary

        return summary
    finally:
        db.close()


def poll_loop():
    threading.Thread(target=repair_integrity, args=(5,), daemon=True).start()
    threading.Thread(target=aprs_listener_loop, daemon=True).start()
    print("Starting background polling loop...")
    db = SessionLocal()
    try:
        while True:
            for station_id in OPENMETEO_STATIONS:
                record_data = fetch_current_weather(station_id)
                if record_data:
                    save_weather_record(db, record_data)
                    print(f"[{datetime.datetime.now().isoformat()}] {station_id}: {record_data['temperature']}°F")
                time.sleep(STATION_REQUEST_GAP_SECONDS)  # small delay between stations
            time.sleep(POLL_INTERVAL_SECONDS)
    except Exception as e:
        print(f"Polling loop encountered error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    poll_loop()
