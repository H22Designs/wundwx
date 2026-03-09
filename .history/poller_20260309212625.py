import time
import requests
import datetime
from sqlalchemy.orm import Session
from database import SessionLocal, WeatherRecord

STATIONS = {
    "KALMILLP10": {"name": "Millport Primary", "lat": 33.544, "lon": -88.133},
    "KALKENNE5": {"name": "Kennedy Station", "lat": 33.587, "lon": -88.080},
    "KALMILLP8": {"name": "Millport Alt", "lat": 33.540, "lon": -88.100},
}
POLL_INTERVAL_SECONDS = 30
INTEGRITY_SLOT_MINUTES = 60  # expected observation interval (hourly from Open-Meteo)


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
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        current = data.get("current", {})
        ts = current.get("time")
        if ts:
            return {
                "station_id": sid,
                "timestamp": datetime.datetime.fromisoformat(ts).replace(tzinfo=None),
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
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
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

            if latest and (datetime.datetime.utcnow() - latest.timestamp).days < 1:
                print(f"Recent data found for {station_id}. Skipping backfill.")
                continue

            print(f"Starting 5-day backfill for {station_id}...")
            today = datetime.datetime.utcnow().date()
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
        now = datetime.datetime.utcnow()
        cutoff = now - datetime.timedelta(days=days)
        report = {}

        for station_id in STATIONS.keys():
            # ── Corrupt records ──────────────────────────────────────────
            corrupt_count = db.query(WeatherRecord).filter(
                WeatherRecord.station_id == station_id,
                WeatherRecord.timestamp >= cutoff,
                WeatherRecord.temperature == None,
            ).count()

            # ── Missing 30-min slots ─────────────────────────────────────
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
        now = datetime.datetime.utcnow()
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

            # ── Step 2: Find missing 30-min slots ────────────────────────
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
    repair_integrity()
    print("Starting background polling loop...")
    db = SessionLocal()
    try:
        while True:
            for station_id in STATIONS.keys():
                record_data = fetch_current_weather(station_id)
                if record_data:
                    save_weather_record(db, record_data)
                    print(f"[{datetime.datetime.now().isoformat()}] {station_id}: {record_data['temperature']}°F")
                time.sleep(2)  # small delay between stations
            time.sleep(POLL_INTERVAL_SECONDS)
    except Exception as e:
        print(f"Polling loop encountered error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    poll_loop()
