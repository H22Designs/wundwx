import time
import math
import re
import socket
import requests
import datetime
import threading
from sqlalchemy.orm import Session
from database import SessionLocal, WeatherRecord, Station

# ── Default station definitions (seeded into DB on first run) ─────────────────
_DEFAULT_STATIONS = [
    {"station_id": "KALMILLP10", "name": "Millport Primary", "lat": 33.544, "lon": -88.133,
     "cwop_callsign": "GW7151", "source_type": "openmeteo"},
    {"station_id": "KALKENNE5",  "name": "Kennedy Station",  "lat": 33.587, "lon": -88.080,
     "cwop_callsign": "FW4617", "source_type": "openmeteo"},
    {"station_id": "KALMILLP8",  "name": "Millport Alt",     "lat": 33.540, "lon": -88.100,
     "cwop_callsign": "",       "source_type": "openmeteo"},
]

# ── Runtime station state (rebuilt from DB via reload_stations()) ──────────────
STATIONS: dict = {}
OPENMETEO_STATIONS: set = set()
WU_STATIONS: set = set()
STATION_API_KEYS: dict = {}        # station_id → WU API key
CWOP_TO_STATION: dict = {}
CWOP_CALLSIGNS: set = set()
_stations_lock = threading.Lock()

POLL_INTERVAL_SECONDS = 60
STATION_REQUEST_GAP_SECONDS = 1
INTEGRITY_SLOT_MINUTES = 60

# ── APRS-IS / CWOP config ─────────────────────────────────────────────────────
APRS_IS_HOST   = "rotate.aprs2.net"
APRS_IS_PORT   = 14580
APRS_IS_FILTER = "t/w r/33.74/-88.14/100"


def seed_stations_if_needed():
    """Insert default stations into the DB if the stations table is empty."""
    db = SessionLocal()
    try:
        if db.query(Station).count() == 0:
            for s in _DEFAULT_STATIONS:
                db.add(Station(**s))
            db.commit()
            print(f"[stations] Seeded {len(_DEFAULT_STATIONS)} default stations")
    finally:
        db.close()


def reload_stations():
    """Read active stations from the DB and rebuild all in-memory maps."""
    global STATIONS, OPENMETEO_STATIONS, WU_STATIONS, STATION_API_KEYS, CWOP_TO_STATION, CWOP_CALLSIGNS
    db = SessionLocal()
    try:
        rows = db.query(Station).filter(Station.is_active == True).all()
        new_stations = {}
        new_openmeteo = set()
        new_wu = set()
        new_api_keys = {}
        new_cwop_to_station = {}
        for r in rows:
            new_stations[r.station_id] = {
                "name": r.name, "lat": r.lat, "lon": r.lon,
                "cwop_callsign": r.cwop_callsign or "",
            }
            if r.source_type == "openmeteo":
                new_openmeteo.add(r.station_id)
            elif r.source_type == "wunderground":
                new_wu.add(r.station_id)
                if r.api_key:
                    new_api_keys[r.station_id] = r.api_key
            if r.cwop_callsign:
                new_cwop_to_station[r.cwop_callsign] = r.station_id

        with _stations_lock:
            STATIONS = new_stations
            OPENMETEO_STATIONS = new_openmeteo
            WU_STATIONS = new_wu
            STATION_API_KEYS = new_api_keys
            CWOP_TO_STATION = new_cwop_to_station
            CWOP_CALLSIGNS = set(new_cwop_to_station.keys())

        print(f"[stations] Loaded {len(new_stations)} active station(s) from DB "
              f"(openmeteo={len(new_openmeteo)}, wu={len(new_wu)}, cwop={len(new_cwop_to_station)})")
    finally:
        db.close()


# Initialize on import so that STATIONS is populated before main.py starts
seed_stations_if_needed()
reload_stations()


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
    with _stations_lock:
        if not CWOP_CALLSIGNS:
            print("[APRS-IS] No CWOP callsigns configured — listener not started")
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
        "&daily=precipitation_sum"
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
        "&daily=precipitation_sum"
        "&temperature_unit=fahrenheit"
        "&wind_speed_unit=mph"
        "&precipitation_unit=inch"
        "&timezone=UTC"
    )


# ── Weather Underground PWS API helpers ──────────────────────────────────────
_WU_BASE = "https://api.weather.com/v2/pws"


def _wu_current_url(station_id, api_key):
    return (
        f"{_WU_BASE}/observations/current"
        f"?stationId={station_id}&format=json&units=e&apiKey={api_key}"
    )


def _wu_history_url(station_id, day, api_key):
    """day: YYYYMMDD string"""
    return (
        f"{_WU_BASE}/history/hourly"
        f"?stationId={station_id}&date={day}&format=json&units=e&apiKey={api_key}"
    )


def fetch_current_weather_wu(station_id, api_key):
    """Fetch the current observation for a WU PWS station."""
    sid = station_id.upper()
    if not api_key:
        print(f"[WU] No API key for {sid} — skipping")
        return None
    url = _wu_current_url(sid, api_key)
    try:
        data = _http_get_json(url, timeout=20, attempts=3, backoff=1.7)
        obs_list = data.get("observations", [])
        if not obs_list:
            return None
        obs = obs_list[0]
        imp = obs.get("imperial", {})
        return {
            "station_id":      sid,
            "timestamp":       _utcnow_naive(),
            "temperature":     imp.get("temp"),
            "humidity":        obs.get("humidity"),
            "dew_point":       imp.get("dewpt"),
            "heat_index":      imp.get("heatIndex"),
            "wind_chill":      imp.get("windChill"),
            "wind_speed":      imp.get("windSpeed"),
            "wind_dir":        obs.get("winddir"),
            "wind_gust":       imp.get("windGust"),
            "pressure":        imp.get("pressure"),    # already inHg
            "precip_rate":     imp.get("precipRate"),
            "precip_total":    imp.get("precipTotal"),
            "solar_radiation": obs.get("solarRadiation"),
            "uv_index":        obs.get("uv"),
        }
    except Exception as e:
        print(f"[WU] Error fetching current for {sid}: {e}")
    return None


def fetch_historical_weather_wu(station_id, date_str, api_key):
    """
    Fetch hourly history for a WU PWS station for a given day (YYYYMMDD or YYYY-MM-DD).
    Returns a list of standardised weather record dicts.
    """
    sid = station_id.upper()
    if not api_key:
        return []
    if len(date_str) == 10:             # YYYY-MM-DD → YYYYMMDD
        date_str = date_str.replace("-", "")
    url = _wu_history_url(sid, date_str, api_key)
    records = []
    try:
        data = _http_get_json(url, timeout=25, attempts=3, backoff=1.8)
        for obs in data.get("observations", []):
            try:
                ts_str = obs.get("obsTimeUtc", "")
                timestamp = datetime.datetime.fromisoformat(
                    ts_str.replace("Z", "+00:00")
                ).replace(tzinfo=None)
            except Exception:
                continue
            imp = obs.get("imperial", {})
            records.append({
                "station_id":      sid,
                "timestamp":       timestamp,
                "temperature":     imp.get("temp"),
                "humidity":        obs.get("humidityAvg", obs.get("humidity")),
                "dew_point":       imp.get("dewpt"),
                "heat_index":      imp.get("heatIndex"),
                "wind_chill":      imp.get("windChill"),
                "wind_speed":      imp.get("windspeedAvg", imp.get("windSpeed")),
                "wind_dir":        obs.get("winddirAvg", obs.get("winddir")),
                "wind_gust":       imp.get("windgustHigh", imp.get("windGust")),
                "pressure":        imp.get("pressureMax", imp.get("pressure")),
                "precip_rate":     imp.get("precipRate"),
                "precip_total":    imp.get("precipTotal"),
                "solar_radiation": obs.get("solarRadiationHigh", obs.get("solarRadiation")),
                "uv_index":        obs.get("uvHigh", obs.get("uv")),
            })
    except Exception as e:
        print(f"[WU] Error fetching history for {sid} {date_str}: {e}")
    return _expand_hourly_to_5min(records)


def fetch_current_weather(station_id):
    sid, info = _normalize_station(station_id)
    if not info:
        return None
    url = _open_meteo_current_url(info["lat"], info["lon"])
    try:
        data = _http_get_json(url, timeout=20, attempts=3, backoff=1.7)
        current = data.get("current", {})
        if current:
            # daily.precipitation_sum[0] is today's accumulated rainfall
            daily = data.get("daily", {})
            daily_precip_sums = daily.get("precipitation_sum", [])
            precip_today = daily_precip_sums[0] if daily_precip_sums else None
            return {
                "station_id": sid,
                # Use poll time to preserve a continuous local time-series even
                # when upstream "current.time" only advances infrequently.
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
                "precip_total": precip_today,
                "solar_radiation": current.get("shortwave_radiation"),
                "uv_index": current.get("uv_index"),
            }
    except Exception as e:
        print(f"Error fetching current weather for {sid or station_id}: {e}")
    return None

_INTERP_FIELDS = [
    "temperature", "humidity", "dew_point", "heat_index", "wind_chill",
    "wind_speed", "wind_gust", "pressure", "solar_radiation", "uv_index",
    "precip_total",
]
_COPY_FIELDS = ["station_id", "wind_dir", "precip_rate"]
_HISTORY_INTERVAL_MINUTES = 5


def _expand_hourly_to_5min(records):
    """
    Expand a list of hourly weather record dicts into 5-minute interval records
    using linear interpolation for smooth fields.  Wind direction and precip
    rate are copied unchanged from each hourly record.
    """
    if not records:
        return records

    steps = 60 // _HISTORY_INTERVAL_MINUTES  # 12 sub-records per hour
    expanded = []

    for i, rec in enumerate(records):
        next_rec = records[i + 1] if i + 1 < len(records) else None
        for step in range(steps):
            frac = step / steps
            ts = rec["timestamp"] + datetime.timedelta(minutes=step * _HISTORY_INTERVAL_MINUTES)
            sub = {"timestamp": ts}

            for field in _INTERP_FIELDS:
                v0 = rec.get(field)
                if next_rec is not None and v0 is not None:
                    v1 = next_rec.get(field)
                    sub[field] = round(v0 + frac * (v1 - v0), 4) if v1 is not None else v0
                else:
                    sub[field] = v0

            for field in _COPY_FIELDS:
                sub[field] = rec.get(field)

            expanded.append(sub)

    return expanded


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
        daily_sums = data.get("daily", {}).get("precipitation_sum", [])
        daily_precip_total = daily_sums[0] if daily_sums else None
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
                "precip_total": daily_precip_total,
                "solar_radiation": hv("shortwave_radiation"),
                "uv_index": hv("uv_index"),
            })
    except Exception as e:
        print(f"Error fetching historical weather for {sid or station_id} {date_str}: {e}")
    return _expand_hourly_to_5min(records)


def save_weather_record(db: Session, record_data: dict):
    try:
        # Reject records with future timestamps (archive API returns forecast hours)
        ts = record_data.get("timestamp")
        if ts and ts > _utcnow_naive():
            return None
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
    except Exception:
        db.rollback()
        raise

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
            for i in range(1, 6):  # skip today (i=0) — archive API returns future hours
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

            # Exclude today — archive API can't backfill it, and the live poller
            # will naturally fill it in over time.
            today_str = now.strftime("%Y%m%d")
            missing = [s for s in missing if s.strftime("%Y%m%d") != today_str]

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

            # Skip today — archive API returns forecast hours that create
            # future-timestamped records which shadow real current data.
            today_str = now.strftime("%Y%m%d")
            missing_dates.discard(today_str)

            if not missing_dates:
                print(f"[integrity] {station_id}: no missing slots found.")
            else:
                print(f"[integrity] {station_id}: backfilling {len(missing_dates)} date(s): "
                      f"{sorted(missing_dates)}")
                for date_str in sorted(missing_dates):
                    print(f"[integrity] {station_id}: fetching history for {date_str}...")
                    with _stations_lock:
                        src = "wunderground" if station_id in WU_STATIONS else "openmeteo"
                        api_key = STATION_API_KEYS.get(station_id, "")
                    if src == "wunderground":
                        records = fetch_historical_weather_wu(station_id, date_str, api_key)
                    elif src == "openmeteo":
                        records = fetch_historical_weather(station_id, date_str)
                    else:
                        records = []
                    for rec in records:
                        save_weather_record(db, rec)
                    station_summary["dates_backfilled"].append(date_str)
                    time.sleep(1)

            summary[station_id] = station_summary

        return summary
    finally:
        db.close()


def get_db_stats():
    """Return per-station record counts, date ranges, corrupt counts, and DB file size."""
    import os
    from sqlalchemy import func as sa_func
    db = SessionLocal()
    try:
        rows = db.query(
            WeatherRecord.station_id,
            sa_func.count(WeatherRecord.id).label("count"),
            sa_func.min(WeatherRecord.timestamp).label("oldest"),
            sa_func.max(WeatherRecord.timestamp).label("newest"),
        ).group_by(WeatherRecord.station_id).all()

        stations_stats = []
        for row in rows:
            corrupt = db.query(sa_func.count(WeatherRecord.id)).filter(
                WeatherRecord.station_id == row.station_id,
                WeatherRecord.temperature.is_(None),
            ).scalar() or 0
            stations_stats.append({
                "station_id": row.station_id,
                "record_count": row.count,
                "oldest": row.oldest.isoformat() if row.oldest else None,
                "newest": row.newest.isoformat() if row.newest else None,
                "corrupt_count": corrupt,
            })

        db_size = os.path.getsize("weather.db") if os.path.exists("weather.db") else 0
        return {
            "db_size_bytes": db_size,
            "total_records": sum(s["record_count"] for s in stations_stats),
            "stations": stations_stats,
        }
    finally:
        db.close()


def backfill_station_date_range(station_id: str, start_date: str, end_date: str):
    """Backfill a specific station from start_date to end_date (YYYY-MM-DD inclusive).
    Returns a dict with filled_dates and errors."""
    with _stations_lock:
        if station_id not in STATIONS:
            return {"error": f"Unknown station: {station_id}"}
        is_openmeteo = station_id in OPENMETEO_STATIONS
        is_wu = station_id in WU_STATIONS
        api_key = STATION_API_KEYS.get(station_id)

    if not is_openmeteo and not is_wu:
        return {"error": f"Station {station_id} has no backfill source (CWOP stations cannot be backfilled)"}

    start = datetime.date.fromisoformat(start_date)
    end = datetime.date.fromisoformat(end_date)

    filled, errors = [], []
    db = SessionLocal()
    try:
        current = start
        while current <= end:
            date_str = current.strftime("%Y%m%d")
            try:
                if is_openmeteo:
                    records = fetch_historical_weather(station_id, date_str)
                else:
                    records = fetch_historical_weather_wu(station_id, date_str, api_key)
                for rec in records:
                    save_weather_record(db, rec)
                filled.append(date_str)
                print(f"[backfill] {station_id} {date_str}: {len(records)} records")
            except Exception as e:
                errors.append({"date": date_str, "error": str(e)})
                print(f"[backfill] {station_id} {date_str}: ERROR {e}")
            current += datetime.timedelta(days=1)
            time.sleep(1)
    finally:
        db.close()

    return {"filled_dates": filled, "errors": errors}


def purge_records(station_id: str = None, start_dt=None, end_dt=None):
    """Delete weather records matching filters. Returns count of deleted records."""
    db = SessionLocal()
    try:
        query = db.query(WeatherRecord)
        if station_id:
            query = query.filter(WeatherRecord.station_id == station_id)
        if start_dt:
            query = query.filter(WeatherRecord.timestamp >= start_dt)
        if end_dt:
            query = query.filter(WeatherRecord.timestamp <= end_dt)
        count = query.count()
        query.delete(synchronize_session=False)
        db.commit()
        print(f"[purge] Deleted {count} records (station={station_id}, start={start_dt}, end={end_dt})")
        return {"deleted": count}
    finally:
        db.close()


def rebuild_weather_records(days: int = 5):
    """Delete all weather records then trigger a full integrity repair/backfill."""
    db = SessionLocal()
    try:
        count = db.query(WeatherRecord).count()
        db.query(WeatherRecord).delete(synchronize_session=False)
        db.commit()
        print(f"[rebuild] Deleted all {count} weather records. Starting backfill…")
    finally:
        db.close()
    repair_integrity(days)
    print("[rebuild] Complete.")


def poll_loop():
    threading.Thread(target=repair_integrity, args=(5,), daemon=True).start()
    threading.Thread(target=aprs_listener_loop, daemon=True).start()
    print("Starting background polling loop...")
    db = SessionLocal()
    try:
        while True:
            with _stations_lock:
                openmeteo_poll = set(OPENMETEO_STATIONS)
                wu_poll = set(WU_STATIONS)
                api_keys_snap = dict(STATION_API_KEYS)

            # ── Open-Meteo stations ──────────────────────────────────────────
            for station_id in openmeteo_poll:
                try:
                    record_data = fetch_current_weather(station_id)
                    if record_data:
                        save_weather_record(db, record_data)
                        print(f"[poll] {station_id}: {record_data['temperature']}°F  "
                              f"hum={record_data['humidity']}%  "
                              f"pres={record_data['pressure']} inHg")
                except Exception as e:
                    print(f"[poll] Error for {station_id}: {e}")
                time.sleep(STATION_REQUEST_GAP_SECONDS)

            # ── Weather Underground PWS stations ─────────────────────────────
            for station_id in wu_poll:
                try:
                    api_key = api_keys_snap.get(station_id, "")
                    record_data = fetch_current_weather_wu(station_id, api_key)
                    if record_data:
                        save_weather_record(db, record_data)
                        print(f"[WU poll] {station_id}: {record_data['temperature']}°F  "
                              f"hum={record_data['humidity']}%  "
                              f"pres={record_data['pressure']} inHg")
                except Exception as e:
                    print(f"[WU poll] Error for {station_id}: {e}")
                time.sleep(STATION_REQUEST_GAP_SECONDS)

            time.sleep(POLL_INTERVAL_SECONDS)
    finally:
        db.close()

if __name__ == "__main__":
    poll_loop()
