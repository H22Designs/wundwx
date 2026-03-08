import os
import time
import requests
import datetime
from sqlalchemy.orm import Session
from database import SessionLocal, WeatherRecord

API_KEY = "5a1ddae9b97240469ddae9b9720046f8"
STATIONS = ["KALMILLP10", "KALKENNE5", "KALMILLP8"]
POLL_INTERVAL_SECONDS = 30

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

def poll_loop():
    backfill()
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
