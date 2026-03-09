import os
import threading
import datetime
from typing import Optional
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from database import SessionLocal, WeatherRecord, engine, Base
import poller
import requests as http_requests

app = FastAPI(title="Weather Dashboard API")

STATIONS = poller.STATIONS
DEFAULT_STATION = "KALMILLP10"

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@app.on_event("startup")
def startup_event():
    thread = threading.Thread(target=poller.poll_loop, daemon=True)
    thread.start()

# ── Station list ─────────────────────────────────────────────────────────────
@app.get("/api/stations")
def list_stations():
    return [{"id": k, "name": v["name"], "lat": v["lat"], "lon": v["lon"]} for k, v in STATIONS.items()]

# ── Current observation ──────────────────────────────────────────────────────
@app.get("/api/current")
def get_current_weather(station: str = DEFAULT_STATION, db: Session = Depends(get_db)):
    record = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == station.upper()
    ).order_by(desc(WeatherRecord.timestamp)).first()
    if record is None:
        raise HTTPException(status_code=404, detail="No data yet for " + station)
    return {
        "station_id": record.station_id,
        "obs_time_utc": record.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if record.timestamp else None,
        "temp_f": record.temperature,
        "humidity_pct": record.humidity,
        "dew_point_f": record.dew_point,
        "heat_index_f": record.heat_index,
        "wind_chill_f": record.wind_chill,
        "wind_speed_mph": record.wind_speed,
        "wind_dir_deg": record.wind_dir,
        "wind_gust_mph": record.wind_gust,
        "pressure_in": record.pressure,
        "precip_rate_in_hr": record.precip_rate,
        "precip_total_in": record.precip_total,
        "solar_radiation_wm2": record.solar_radiation,
        "uv_index": record.uv_index,
    }

# ── History ──────────────────────────────────────────────────────────────────
@app.get("/api/history")
def get_weather_history(
    station: str = DEFAULT_STATION,
    hours: int = 24,
    start: Optional[str] = None,
    end: Optional[str] = None,
    limit: int = 500,
    db: Session = Depends(get_db)
):
    query = db.query(WeatherRecord).filter(WeatherRecord.station_id == station.upper())

    if start and end:
        try:
            start_dt = datetime.datetime.fromisoformat(start.replace('Z', '+00:00')).replace(tzinfo=None)
            end_dt = datetime.datetime.fromisoformat(end.replace('Z', '+00:00')).replace(tzinfo=None)
            query = query.filter(WeatherRecord.timestamp >= start_dt, WeatherRecord.timestamp <= end_dt)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid datetime format.")
    else:
        cutoff = datetime.datetime.utcnow() - datetime.timedelta(hours=hours)
        query = query.filter(WeatherRecord.timestamp >= cutoff)

    records = query.order_by(WeatherRecord.timestamp).limit(limit).all()
    return [{
        "obs_time_utc": r.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if r.timestamp else None,
        "temp_f": r.temperature,
        "humidity_pct": r.humidity,
        "dew_point_f": r.dew_point,
        "heat_index_f": r.heat_index,
        "wind_chill_f": r.wind_chill,
        "wind_speed_mph": r.wind_speed,
        "wind_dir_deg": r.wind_dir,
        "wind_gust_mph": r.wind_gust,
        "pressure_in": r.pressure,
        "precip_rate_in_hr": r.precip_rate,
        "precip_total_in": r.precip_total,
        "solar_radiation_wm2": r.solar_radiation,
        "uv_index": r.uv_index,
    } for r in records]

# ── Today summary ────────────────────────────────────────────────────────────
@app.get("/api/today")
def get_today_summary(station: str = DEFAULT_STATION, db: Session = Depends(get_db)):
    today_start = datetime.datetime.utcnow().replace(hour=0, minute=0, second=0, microsecond=0)
    records = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == station.upper(),
        WeatherRecord.timestamp >= today_start
    ).all()
    if not records:
        return None
    temps = [r.temperature for r in records if r.temperature is not None]
    humids = [r.humidity for r in records if r.humidity is not None]
    pressures = [r.pressure for r in records if r.pressure is not None]
    gusts = [r.wind_gust for r in records if r.wind_gust is not None]
    rains = [r.precip_total for r in records if r.precip_total is not None]
    uvs = [r.uv_index for r in records if r.uv_index is not None]
    solars = [r.solar_radiation for r in records if r.solar_radiation is not None]
    return {
        "temp_high_f": max(temps) if temps else None,
        "temp_low_f": min(temps) if temps else None,
        "temp_avg_f": round(float(sum(temps))/len(temps), 1) if temps else None,
        "humidity_high": max(humids) if humids else None,
        "humidity_low": min(humids) if humids else None,
        "pressure_avg": round(float(sum(pressures))/len(pressures), 2) if pressures else None,
        "wind_gust_max": max(gusts) if gusts else None,
        "rain_total": max(rains) if rains else None,
        "uv_max": max(uvs) if uvs else None,
        "solar_max": max(solars) if solars else None,
        "reading_count": len(records),
    }

# ── Daily summary (last 30 days) ────────────────────────────────────────────
@app.get("/api/daily")
def get_daily_summary(station: str = DEFAULT_STATION, days: int = 30, db: Session = Depends(get_db)):
    cutoff = datetime.datetime.utcnow() - datetime.timedelta(days=days)
    records = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == station.upper(),
        WeatherRecord.timestamp >= cutoff
    ).order_by(WeatherRecord.timestamp).all()

    from collections import defaultdict
    by_day = defaultdict(list)
    for r in records:
        if r.timestamp:
            by_day[r.timestamp.strftime("%Y-%m-%d")].append(r)

    result = []
    for day in sorted(by_day.keys(), reverse=True):
        recs = by_day[day]
        temps = [r.temperature for r in recs if r.temperature is not None]
        humids = [r.humidity for r in recs if r.humidity is not None]
        pressures = [r.pressure for r in recs if r.pressure is not None]
        gusts = [r.wind_gust for r in recs if r.wind_gust is not None]
        rains = [r.precip_total for r in recs if r.precip_total is not None]
        uvs = [r.uv_index for r in recs if r.uv_index is not None]
        result.append({
            "day": day,
            "temp_high_f": max(temps) if temps else None,
            "temp_low_f": min(temps) if temps else None,
            "temp_avg_f": round(float(sum(temps))/len(temps), 1) if temps else None,
            "humidity_high": max(humids) if humids else None,
            "humidity_low": min(humids) if humids else None,
            "pressure_avg": round(float(sum(pressures))/len(pressures), 2) if pressures else None,
            "wind_gust_max": max(gusts) if gusts else None,
            "rain_total": max(rains) if rains else None,
            "uv_max": max(uvs) if uvs else None,
            "reading_count": len(recs),
        })
    return result

# ── NWS Alerts ───────────────────────────────────────────────────────────────
@app.get("/api/alerts")
def get_nws_alerts(station: str = DEFAULT_STATION):
    info = STATIONS.get(station.upper(), STATIONS[DEFAULT_STATION])
    try:
        r = http_requests.get(
            f"https://api.weather.gov/alerts/active?point={info['lat']},{info['lon']}",
            headers={"User-Agent": "WeatherDashboard/1.0"},
            timeout=10
        )
        r.raise_for_status()
        data = r.json()
        alerts = []
        for feature in data.get("features", []):
            props = feature.get("properties", {})
            alerts.append({
                "event": props.get("event"),
                "severity": props.get("severity"),
                "headline": props.get("headline"),
                "description": props.get("description"),
                "expires": props.get("expires"),
            })
        return alerts
    except Exception:
        return []

# ── Nearby Stations ──────────────────────────────────────────────────────────
@app.get("/api/nearby")
def get_nearby_stations(station: str = DEFAULT_STATION):
    selected = station.upper()
    nearby = []
    for station_id, info in STATIONS.items():
        if station_id == selected:
            continue
        current = poller.fetch_current_weather(station_id)
        nearby.append({
            "stationID": station_id,
            "neighborhood": info.get("name"),
            "lat": info.get("lat"),
            "lon": info.get("lon"),
            "temp_f": current.get("temperature") if current else None,
            "humidity": current.get("humidity") if current else None,
            "wind_speed": current.get("wind_speed") if current else None,
        })
    return nearby

# ── Database Integrity ────────────────────────────────────────────────────────
@app.get("/api/integrity")
def get_integrity_report():
    """
    Scan the database and return a per-station integrity report showing:
    - corrupt_count      : records with a NULL temperature in the last 5 days
    - missing_slot_count : 30-minute windows with no observation in the last 5 days
    - missing_slots      : list of those slot timestamps (ISO format)
    - missing_dates      : unique dates that contain missing slots
    """
    return poller.check_integrity(days=5)


@app.post("/api/integrity/repair")
def trigger_integrity_repair(background_tasks: BackgroundTasks):
    """
    Kick off a background repair job that:
    1. Removes corrupt records (NULL temperature) from the last 5 days.
    2. Backfills any day that has at least one missing 30-minute slot.
    Returns immediately; check server logs for progress.
    """
    background_tasks.add_task(poller.repair_integrity, 5)
    return {"status": "repair started", "message": "Integrity repair running in the background."}


# ── Static files ─────────────────────────────────────────────────────────────
os.makedirs("static/css", exist_ok=True)
os.makedirs("static/js", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
def read_root():
    return FileResponse("static/index.html")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
