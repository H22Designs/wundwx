import csv
import io
import os
import time
import requests
import datetime
from sqlalchemy.orm import Session
from database import SessionLocal, WeatherRecord

API_KEY = "5a1ddae9b97240469ddae9b9720046f8"
STATIONS = ["KALMILLP10", "KALKENNE5", "KALMILLP8"]
POLL_INTERVAL_SECONDS = 30
INTEGRITY_SLOT_MINUTES = 10  # expected observation interval (minutes)
INTEGRITY_DAYS = 5            # number of past days to verify

def fetch_current_weather(station_id):
    url = f"https://api.weather.com/v2/pws/observations/current?stationId={station_id}&format=json&units=e&apiKey={API_KEY}"
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if response.status_code == 204:
            return None
        data = response.json()
        if 'observations' in data and len(data['observations']) > 0:
            obs = data['observations'][0]
            metric = obs.get('imperial', {})

            return {
                "station_id": station_id,
                "timestamp": datetime.datetime.fromisoformat(obs.get('obsTimeUtc').replace('Z', '+00:00')).replace(tzinfo=None),
                "temperature": metric.get('temp'),
                "humidity": obs.get('humidity'),
                "dew_point": metric.get('dewpt'),
                "heat_index": metric.get('heatIndex'),
                "wind_chill": metric.get('windChill'),
                "wind_speed": metric.get('windSpeed'),
                "wind_dir": obs.get('winddir'),
                "wind_gust": metric.get('windGust'),
                "pressure": metric.get('pressure'),
                "precip_rate": metric.get('precipRate'),
                "precip_total": metric.get('precipTotal'),
                "solar_radiation": obs.get('solarRadiation'),
                "uv_index": obs.get('uv')
            }
    except Exception as e:
        print(f"Error fetching current weather for {station_id}: {e}")
    return None

def fetch_historical_weather(station_id, date_str):
    url = f"https://api.weather.com/v2/pws/history/all?stationId={station_id}&format=json&units=e&date={date_str}&apiKey={API_KEY}"
    records = []
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        if response.status_code == 204:
            return records
        data = response.json()
        if 'observations' in data:
            for obs in data['observations']:
                metric = obs.get('imperial', {})
                records.append({
                    "station_id": station_id,
                    "timestamp": datetime.datetime.fromisoformat(obs.get('obsTimeUtc').replace('Z', '+00:00')).replace(tzinfo=None),
                    "temperature": metric.get('tempAvg'),
                    "humidity": obs.get('humidityAvg'),
                    "dew_point": metric.get('dewptAvg'),
                    "heat_index": metric.get('heatindexAvg'),
                    "wind_chill": metric.get('windchillAvg'),
                    "wind_speed": metric.get('windspeedAvg'),
                    "wind_dir": obs.get('winddirAvg'),
                    "wind_gust": metric.get('windgustAvg'),
                    "pressure": metric.get('pressureMax'),
                    "precip_rate": metric.get('precipRate'),
                    "precip_total": metric.get('precipTotal'),
                    "solar_radiation": obs.get('solarRadiationHigh'),
                    "uv_index": obs.get('uvHigh')
                })
    except Exception as e:
        print(f"Error fetching historical weather for {station_id} {date_str}: {e}")
    return records


def fetch_historical_weather_scrape(station_id, date_str):
    """Fetch PWS historical data via the WunderGround public CSV export endpoint.

    This endpoint does not require an API key.  The response is a CSV document
    (comma-separated, with <br> used as line endings) exported by:
      https://www.wunderground.com/weatherstation/WXDailyHistory.asp
    """
    year  = int(date_str[:4])
    month = int(date_str[4:6])
    day   = int(date_str[6:8])
    url = (
        "https://www.wunderground.com/weatherstation/WXDailyHistory.asp"
        f"?ID={station_id}&day={day}&month={month}&year={year}"
        "&graphspan=day&format=1"
    )
    records = []
    try:
        headers = {"User-Agent": "Mozilla/5.0 (compatible; wundwx-poller/1.0)"}
        response = requests.get(url, timeout=15, headers=headers)
        response.raise_for_status()

        # The endpoint uses <br> as line endings; normalise to real newlines.
        text = response.text.replace("<br />", "\n").replace("<br/>", "\n").replace("<br>", "\n")
        reader = csv.DictReader(io.StringIO(text.strip()))

        def _float(val):
            """Return float or None for empty / non-numeric values."""
            try:
                return float(val) if val and val.strip() not in ("", "N/A", "-") else None
            except (ValueError, TypeError):
                return None

        for row in reader:
            date_utc_str = (row.get("DateUTC") or "").strip()
            if not date_utc_str:
                continue
            try:
                ts = datetime.datetime.strptime(date_utc_str, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                continue

            # Pressure in the CSV export is in hPa; convert to inHg to match
            # the imperial units used by the rest of the application.
            # WU stations may report the column as 'PressurehPa' or 'Pressurehpa'
            # depending on the station firmware version, so we check both casings.
            pressure_hpa = _float(row.get("PressurehPa") or row.get("Pressurehpa") or "")
            pressure_inhg = round(pressure_hpa / 33.8639, 2) if pressure_hpa is not None else None

            records.append({
                "station_id":     station_id,
                "timestamp":      ts,
                "temperature":    _float(row.get("TemperatureF")),
                "humidity":       _float(row.get("Humidity")),
                "dew_point":      _float(row.get("DewpointF")),
                "heat_index":     None,   # not provided by WXDailyHistory.asp CSV
                "wind_chill":     None,   # not provided by WXDailyHistory.asp CSV
                "wind_speed":     _float(row.get("WindSpeedMPH")),
                "wind_dir":       _float(row.get("WindDirectionDegrees")),
                "wind_gust":      _float(row.get("WindSpeedGustMPH")),
                "pressure":       pressure_inhg,
                "precip_rate":    _float(row.get("HourlyPrecipIn")),
                "precip_total":   _float(row.get("dailyrainin")),
                # Column name contains special characters exactly as WU exports it.
                "solar_radiation": _float(row.get("SolarRadiationWatts/m^2")),
                "uv_index":       None,   # not provided by WXDailyHistory.asp CSV
            })
    except Exception as e:
        print(f"Error scraping historical weather for {station_id} {date_str}: {e}")
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
    """Legacy backfill: only runs when no recent data exists. Prefer repair_integrity()."""
    db = SessionLocal()
    try:
        for station_id in STATIONS:
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
    """Floor a datetime to the nearest INTEGRITY_SLOT_MINUTES boundary.

    Naive datetimes are treated as UTC (matching how the rest of the codebase
    stores timestamps).
    """
    slot_secs = INTEGRITY_SLOT_MINUTES * 60
    # Coerce timezone-aware datetimes to naive UTC before arithmetic
    if ts.tzinfo is not None:
        ts = ts.replace(tzinfo=None) - ts.utcoffset()
    epoch_secs = int((ts - datetime.datetime(1970, 1, 1)).total_seconds())
    floored = (epoch_secs // slot_secs) * slot_secs
    return datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=floored)


def check_integrity(days=INTEGRITY_DAYS):
    """
    Scan the database for each station over the last ``days`` days.

    A station-day is flagged for repair when:
      * More than 10% of the expected 10-minute slots have no matching record, OR
      * Any record in that day has both temperature AND humidity as NULL
        (corrupt / placeholder row).

    Returns a dict  { station_id: set_of_date_strings_YYYYMMDD }.
    """
    db = SessionLocal()
    needs_repair = {}
    try:
        now = datetime.datetime.utcnow()
        for station_id in STATIONS:
            bad_dates = set()
            for i in range(days):
                day = (now - datetime.timedelta(days=i)).date()
                day_start = datetime.datetime(day.year, day.month, day.day, 0, 0, 0)
                day_end = day_start + datetime.timedelta(days=1)

                records = db.query(WeatherRecord).filter(
                    WeatherRecord.station_id == station_id,
                    WeatherRecord.timestamp >= day_start,
                    WeatherRecord.timestamp < day_end,
                ).order_by(WeatherRecord.timestamp).all()

                # --- detect corrupt rows ---
                has_corrupt = any(
                    r.temperature is None and r.humidity is None
                    for r in records
                )

                # --- detect missing 10-minute slots ---
                # Build the full set of expected slot boundaries up to 'now'
                expected = set()
                slot_cursor = day_start
                slot_delta = datetime.timedelta(minutes=INTEGRITY_SLOT_MINUTES)
                while slot_cursor < day_end and slot_cursor <= now:
                    expected.add(slot_cursor)
                    slot_cursor += slot_delta

                filled = {_round_to_slot(r.timestamp) for r in records if r.timestamp}
                missing_slots = expected - filled
                missing_pct = len(missing_slots) / len(expected) if expected else 0

                if has_corrupt or missing_pct > 0.10:
                    date_str = day.strftime("%Y%m%d")
                    bad_dates.add(date_str)
                    print(
                        f"  [{station_id}] {day}: "
                        f"{len(missing_slots)}/{len(expected)} slots missing "
                        f"({'corrupt records found' if has_corrupt else 'data gap'})"
                    )

            if bad_dates:
                needs_repair[station_id] = bad_dates
    finally:
        db.close()
    return needs_repair


def repair_integrity(days=INTEGRITY_DAYS):
    """
    Scan the database for missing or corrupt 10-minute-interval data over the
    last ``days`` days and repair any gaps by fetching from Weather Underground.
    Called automatically at application startup.
    """
    print(f"Scanning database integrity for the last {days} days (10-minute intervals per station)...")
    needs_repair = check_integrity(days)

    if not needs_repair:
        print("Integrity check passed — no repair needed.")
        return

    total = sum(len(v) for v in needs_repair.values())
    print(f"Repairing {total} station-day(s) with missing or corrupt data...")

    db = SessionLocal()
    try:
        for station_id, date_set in needs_repair.items():
            for date_str in sorted(date_set):
                print(f"  Fetching {station_id} {date_str} from Weather Underground (public CSV)...")
                records = fetch_historical_weather_scrape(station_id, date_str)
                saved = 0
                for rec in records:
                    result = save_weather_record(db, rec)
                    if result:
                        saved += 1
                print(f"  {station_id} {date_str}: {len(records)} observations fetched, "
                      f"{saved} new records saved.")
                time.sleep(1)
    finally:
        db.close()

    print("Integrity repair complete.")

def poll_loop():
    repair_integrity()
    print("Starting background polling loop...")
    db = SessionLocal()
    try:
        while True:
            for station_id in STATIONS:
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
