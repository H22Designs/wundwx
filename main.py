import os
import threading
import datetime
from contextlib import asynccontextmanager
from typing import Optional, List
import io
import os
import shutil
import sqlite3
import tempfile
from fastapi import FastAPI, Depends, HTTPException, BackgroundTasks, Request, Response, UploadFile, File
from pydantic import BaseModel
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, RedirectResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from sqlalchemy.orm import Session
from sqlalchemy import desc, func
from database import (
    SessionLocal, WeatherRecord, engine, Base,
    User, UserSettings, UserFavoriteStation, Station,
)
from auth import (
    seed_admin_if_needed, hash_password, verify_password,
    create_access_token, get_current_user, require_user, require_admin,
)
import poller
import requests as http_requests


def _utcnow_naive():
    return datetime.datetime.now(datetime.UTC).replace(tzinfo=None)


def _record_to_payload(record: WeatherRecord):
    ts = getattr(record, "timestamp", None)
    return {
        "station_id": record.station_id,
        "obs_time_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ") if ts is not None else None,
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


def _dict_to_payload(record_data: dict):
    ts = record_data.get("timestamp")
    return {
        "station_id": record_data.get("station_id"),
        "obs_time_utc": ts.strftime("%Y-%m-%dT%H:%M:%SZ") if ts else None,
        "temp_f": record_data.get("temperature"),
        "humidity_pct": record_data.get("humidity"),
        "dew_point_f": record_data.get("dew_point"),
        "heat_index_f": record_data.get("heat_index"),
        "wind_chill_f": record_data.get("wind_chill"),
        "wind_speed_mph": record_data.get("wind_speed"),
        "wind_dir_deg": record_data.get("wind_dir"),
        "wind_gust_mph": record_data.get("wind_gust"),
        "pressure_in": record_data.get("pressure"),
        "precip_rate_in_hr": record_data.get("precip_rate"),
        "precip_total_in": record_data.get("precip_total"),
        "solar_radiation_wm2": record_data.get("solar_radiation"),
        "uv_index": record_data.get("uv_index"),
    }


@asynccontextmanager
async def lifespan(app: FastAPI):
    seed_admin_if_needed()
    poller.seed_stations_if_needed()
    poller.reload_stations()
    thread = threading.Thread(target=poller.poll_loop, daemon=True)
    thread.start()
    yield


app = FastAPI(title="Weather Dashboard API", lifespan=lifespan)

templates = Jinja2Templates(directory="templates")
DEFAULT_STATION = "KALMILLP10"


def _page_ctx(request: Request, db: Session, **extra) -> dict:
    """Build the base template context for any page route."""
    user = get_current_user(request, db)
    return {"request": request, "current_user": user, **extra}


def _station_name(db: Session, station_id: str) -> str:
    st = db.query(Station).filter(Station.station_id == station_id, Station.is_active == True).first()
    return st.name if st else station_id


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ═══════════════════════════════════════════════════════════════════════════════
# AUTH ENDPOINTS
# ═══════════════════════════════════════════════════════════════════════════════

class RegisterRequest(BaseModel):
    username: str
    email: str
    password: str

class LoginRequest(BaseModel):
    username: str
    password: str

class PasswordChangeRequest(BaseModel):
    current_password: str
    new_password: str


@app.post("/api/auth/register")
def register(body: RegisterRequest, response: Response, db: Session = Depends(get_db)):
    if not body.username or not body.email or not body.password:
        raise HTTPException(400, "All fields are required")
    if len(body.password) < 4:
        raise HTTPException(400, "Password must be at least 4 characters")
    if db.query(User).filter(User.username == body.username).first():
        raise HTTPException(409, "Username already taken")
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(409, "Email already registered")

    user = User(
        username=body.username,
        email=body.email,
        hashed_password=hash_password(body.password),
    )
    db.add(user)
    db.flush()
    db.add(UserSettings(user_id=user.id))
    db.commit()
    db.refresh(user)

    token = create_access_token(user.id, user.is_admin)
    response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=72*3600)
    return {"id": user.id, "username": user.username, "email": user.email, "is_admin": user.is_admin}


@app.post("/api/auth/login")
def login(body: LoginRequest, response: Response, db: Session = Depends(get_db)):
    user = db.query(User).filter(User.username == body.username).first()
    if not user or not verify_password(body.password, user.hashed_password):
        raise HTTPException(401, "Invalid credentials")
    if not user.is_active:
        raise HTTPException(403, "Account is disabled")

    user.last_login = _utcnow_naive()
    db.commit()

    token = create_access_token(user.id, user.is_admin)
    response.set_cookie("access_token", token, httponly=True, samesite="lax", max_age=72*3600)
    return {"id": user.id, "username": user.username, "email": user.email, "is_admin": user.is_admin}


@app.post("/api/auth/logout")
def logout(response: Response):
    response.delete_cookie("access_token")
    return {"status": "ok"}


@app.get("/api/auth/me")
def auth_me(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    if not user:
        raise HTTPException(401, "Not authenticated")
    return {
        "id": user.id, "username": user.username, "email": user.email,
        "is_admin": user.is_admin, "is_active": user.is_active,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# USER ENDPOINTS (require login)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/user/settings")
def get_user_settings(request: Request, db: Session = Depends(get_db)):
    user = require_user(request, db)
    s = user.settings
    if not s:
        s = UserSettings(user_id=user.id)
        db.add(s)
        db.commit()
        db.refresh(s)
    return {
        "default_station": s.default_station,
        "temp_unit": s.temp_unit,
        "theme": s.theme,
        "refresh_interval": s.refresh_interval,
        "dashboard_layout": s.dashboard_layout,
    }


class UserSettingsUpdate(BaseModel):
    default_station: Optional[str] = None
    temp_unit: Optional[str] = None
    theme: Optional[str] = None
    refresh_interval: Optional[int] = None
    dashboard_layout: Optional[str] = None


@app.put("/api/user/settings")
def update_user_settings(body: UserSettingsUpdate, request: Request, db: Session = Depends(get_db)):
    user = require_user(request, db)
    s = user.settings
    if not s:
        s = UserSettings(user_id=user.id)
        db.add(s)
        db.flush()
    if body.default_station is not None:
        s.default_station = body.default_station
    if body.temp_unit is not None:
        s.temp_unit = body.temp_unit
    if body.theme is not None:
        s.theme = body.theme
    if body.refresh_interval is not None:
        s.refresh_interval = body.refresh_interval
    if body.dashboard_layout is not None:
        s.dashboard_layout = body.dashboard_layout
    db.commit()
    return {"status": "ok"}


@app.get("/api/user/favorites")
def get_favorites(request: Request, db: Session = Depends(get_db)):
    user = require_user(request, db)
    return [{"id": f.id, "station_id": f.station_id, "display_order": f.display_order}
            for f in user.favorites]


class FavoriteAdd(BaseModel):
    station_id: str


@app.post("/api/user/favorites")
def add_favorite(body: FavoriteAdd, request: Request, db: Session = Depends(get_db)):
    user = require_user(request, db)
    existing = db.query(UserFavoriteStation).filter(
        UserFavoriteStation.user_id == user.id,
        UserFavoriteStation.station_id == body.station_id.upper(),
    ).first()
    if existing:
        return {"id": existing.id, "station_id": existing.station_id}
    max_order = max((f.display_order for f in user.favorites), default=-1)
    fav = UserFavoriteStation(user_id=user.id, station_id=body.station_id.upper(), display_order=max_order + 1)
    db.add(fav)
    db.commit()
    db.refresh(fav)
    return {"id": fav.id, "station_id": fav.station_id}


@app.delete("/api/user/favorites/{fav_id}")
def remove_favorite(fav_id: int, request: Request, db: Session = Depends(get_db)):
    user = require_user(request, db)
    fav = db.query(UserFavoriteStation).filter(
        UserFavoriteStation.id == fav_id, UserFavoriteStation.user_id == user.id
    ).first()
    if not fav:
        raise HTTPException(404, "Favorite not found")
    db.delete(fav)
    db.commit()
    return {"status": "ok"}


@app.put("/api/user/password")
def change_password(body: PasswordChangeRequest, request: Request, db: Session = Depends(get_db)):
    user = require_user(request, db)
    if not verify_password(body.current_password, user.hashed_password):
        raise HTTPException(400, "Current password is incorrect")
    if len(body.new_password) < 4:
        raise HTTPException(400, "New password must be at least 4 characters")
    user.hashed_password = hash_password(body.new_password)
    db.commit()
    return {"status": "ok"}


# ═══════════════════════════════════════════════════════════════════════════════
# ADMIN ENDPOINTS (require admin)
# ═══════════════════════════════════════════════════════════════════════════════

# ── Admin Users ──────────────────────────────────────────────────────────────

@app.get("/api/admin/users")
def admin_list_users(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    users = db.query(User).order_by(User.id).all()
    return [{
        "id": u.id, "username": u.username, "email": u.email,
        "is_admin": u.is_admin, "is_active": u.is_active,
        "created_at": u.created_at.isoformat() if u.created_at else None,
        "last_login": u.last_login.isoformat() if u.last_login else None,
    } for u in users]


class AdminCreateUser(BaseModel):
    username: str
    email: str
    password: str
    is_admin: bool = False


@app.post("/api/admin/users")
def admin_create_user(body: AdminCreateUser, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    if db.query(User).filter(User.username == body.username).first():
        raise HTTPException(409, "Username already taken")
    if db.query(User).filter(User.email == body.email).first():
        raise HTTPException(409, "Email already registered")
    user = User(
        username=body.username, email=body.email,
        hashed_password=hash_password(body.password), is_admin=body.is_admin,
    )
    db.add(user)
    db.flush()
    db.add(UserSettings(user_id=user.id))
    db.commit()
    return {"id": user.id, "username": user.username}


class AdminUpdateUser(BaseModel):
    username: Optional[str] = None
    email: Optional[str] = None
    is_admin: Optional[bool] = None
    is_active: Optional[bool] = None


@app.put("/api/admin/users/{user_id}")
def admin_update_user(user_id: int, body: AdminUpdateUser, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    if body.username is not None:
        user.username = body.username
    if body.email is not None:
        user.email = body.email
    if body.is_admin is not None:
        user.is_admin = body.is_admin
    if body.is_active is not None:
        user.is_active = body.is_active
    db.commit()
    return {"status": "ok"}


@app.delete("/api/admin/users/{user_id}")
def admin_delete_user(user_id: int, request: Request, db: Session = Depends(get_db)):
    admin = require_admin(request, db)
    if admin.id == user_id:
        raise HTTPException(400, "Cannot delete your own account")
    user = db.query(User).filter(User.id == user_id).first()
    if not user:
        raise HTTPException(404, "User not found")
    db.delete(user)
    db.commit()
    return {"status": "ok"}


# ── Admin Stations ───────────────────────────────────────────────────────────

@app.get("/api/admin/stations")
def admin_list_stations(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    rows = db.query(Station).order_by(Station.id).all()
    return [{
        "id": r.id, "station_id": r.station_id, "name": r.name,
        "lat": r.lat, "lon": r.lon, "cwop_callsign": r.cwop_callsign or "",
        "source_type": r.source_type,
        # Mask the API key: return only whether one is set, not the actual value
        "has_api_key": bool(r.api_key),
        "is_active": r.is_active,
        "created_at": r.created_at.isoformat() if r.created_at else None,
    } for r in rows]


class AdminCreateStation(BaseModel):
    station_id: str
    name: str
    lat: float
    lon: float
    cwop_callsign: Optional[str] = ""
    source_type: str = "openmeteo"
    api_key: Optional[str] = ""     # required when source_type == "wunderground"


@app.post("/api/admin/stations")
def admin_create_station(body: AdminCreateStation, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    sid = body.station_id.upper()
    if db.query(Station).filter(Station.station_id == sid).first():
        raise HTTPException(409, f"Station '{sid}' already exists")
    if body.source_type == "wunderground" and not body.api_key:
        raise HTTPException(400, "api_key is required for Weather Underground stations")
    station = Station(
        station_id=sid, name=body.name, lat=body.lat, lon=body.lon,
        cwop_callsign=(body.cwop_callsign or "").strip().upper(),
        source_type=body.source_type,
        api_key=(body.api_key or "").strip(),
    )
    db.add(station)
    db.commit()
    poller.reload_stations()
    return {"id": station.id, "station_id": station.station_id}


class AdminUpdateStation(BaseModel):
    name: Optional[str] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    cwop_callsign: Optional[str] = None
    source_type: Optional[str] = None
    api_key: Optional[str] = None   # pass empty string to clear
    is_active: Optional[bool] = None


@app.put("/api/admin/stations/{station_id}")
def admin_update_station(station_id: int, body: AdminUpdateStation, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(404, "Station not found")
    if body.name is not None:
        station.name = body.name
    if body.lat is not None:
        station.lat = body.lat
    if body.lon is not None:
        station.lon = body.lon
    if body.cwop_callsign is not None:
        station.cwop_callsign = body.cwop_callsign.strip().upper()
    if body.source_type is not None:
        station.source_type = body.source_type
    if body.api_key is not None:
        station.api_key = body.api_key.strip()
    if body.is_active is not None:
        station.is_active = body.is_active
    db.commit()
    poller.reload_stations()
    return {"status": "ok"}


@app.delete("/api/admin/stations/{station_id}")
def admin_delete_station(station_id: int, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    station = db.query(Station).filter(Station.id == station_id).first()
    if not station:
        raise HTTPException(404, "Station not found")
    station.is_active = False
    db.commit()
    poller.reload_stations()
    return {"status": "ok", "detail": "Station deactivated"}


# ── Admin Database ───────────────────────────────────────────────────────────

@app.get("/api/admin/db/stats")
def admin_db_stats(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    return poller.get_db_stats()


class AdminDbBackfillRequest(BaseModel):
    station_id: str
    start_date: str   # YYYY-MM-DD
    end_date: str     # YYYY-MM-DD


@app.post("/api/admin/db/backfill")
def admin_db_backfill(body: AdminDbBackfillRequest, background_tasks: BackgroundTasks,
                      request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    try:
        datetime.date.fromisoformat(body.start_date)
        datetime.date.fromisoformat(body.end_date)
    except ValueError:
        raise HTTPException(400, "Dates must be YYYY-MM-DD format")
    background_tasks.add_task(
        poller.backfill_station_date_range,
        body.station_id.upper(), body.start_date, body.end_date,
    )
    return {"status": "started",
            "message": f"Backfilling {body.station_id.upper()} from {body.start_date} to {body.end_date}"}


class AdminDbPurgeRequest(BaseModel):
    station_id: Optional[str] = None
    start_date: Optional[str] = None   # YYYY-MM-DD inclusive
    end_date: Optional[str] = None     # YYYY-MM-DD inclusive


@app.post("/api/admin/db/purge")
def admin_db_purge(body: AdminDbPurgeRequest, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    start_dt, end_dt = None, None
    if body.start_date:
        try:
            start_dt = datetime.datetime.fromisoformat(body.start_date)
        except ValueError:
            raise HTTPException(400, "start_date must be YYYY-MM-DD")
    if body.end_date:
        try:
            # Include the full end day
            end_dt = datetime.datetime.fromisoformat(body.end_date) + datetime.timedelta(days=1)
        except ValueError:
            raise HTTPException(400, "end_date must be YYYY-MM-DD")
    result = poller.purge_records(
        station_id=body.station_id.upper() if body.station_id else None,
        start_dt=start_dt,
        end_dt=end_dt,
    )
    return result


@app.post("/api/admin/db/rebuild")
def admin_db_rebuild(background_tasks: BackgroundTasks, request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    background_tasks.add_task(poller.rebuild_weather_records, 5)
    return {"status": "started",
            "message": "All weather records deleted. Backfilling last 5 days in background."}


@app.get("/api/admin/db/backup")
def admin_db_backup(request: Request, db: Session = Depends(get_db)):
    require_admin(request, db)
    # Use sqlite3 backup API for a consistent, WAL-safe snapshot
    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
    os.close(tmp_fd)
    try:
        src = sqlite3.connect("weather.db")
        dst = sqlite3.connect(tmp_path)
        src.backup(dst)
        dst.close()
        src.close()
    except Exception as e:
        os.unlink(tmp_path)
        raise HTTPException(500, f"Backup failed: {e}")

    def _stream():
        try:
            with open(tmp_path, "rb") as f:
                while chunk := f.read(65536):
                    yield chunk
        finally:
            os.unlink(tmp_path)

    ts = _utcnow_naive().strftime("%Y%m%d_%H%M%S")
    return StreamingResponse(
        _stream(),
        media_type="application/octet-stream",
        headers={"Content-Disposition": f"attachment; filename=weather_backup_{ts}.db"},
    )


@app.post("/api/admin/db/import")
async def admin_db_import(request: Request, file: UploadFile = File(...),
                          db: Session = Depends(get_db)):
    require_admin(request, db)
    content = await file.read()
    # Validate SQLite magic header
    if len(content) < 16 or content[:16] != b"SQLite format 3\x00":
        raise HTTPException(400, "File does not appear to be a valid SQLite database")

    tmp_fd, tmp_path = tempfile.mkstemp(suffix=".db")
    try:
        with os.fdopen(tmp_fd, "wb") as f:
            f.write(content)
        # Restore via sqlite3 backup API (transaction-safe)
        from database import engine as _engine
        _engine.dispose()   # close all pooled connections before swapping
        src = sqlite3.connect(tmp_path)
        dst = sqlite3.connect("weather.db")
        src.backup(dst)
        dst.close()
        src.close()
    except Exception as e:
        raise HTTPException(500, f"Import failed: {e}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)

    return {"status": "ok", "message": "Database restored successfully. Data will refresh on next poll."}


# ═══════════════════════════════════════════════════════════════════════════════
# WEATHER DATA ENDPOINTS (public)
# ═══════════════════════════════════════════════════════════════════════════════

@app.get("/api/stations")
def list_stations(db: Session = Depends(get_db)):
    rows = db.query(Station).filter(Station.is_active == True).order_by(Station.id).all()
    return [{"id": r.station_id, "name": r.name, "lat": r.lat, "lon": r.lon,
             "cwop_callsign": r.cwop_callsign or ""} for r in rows]


@app.get("/api/current")
def get_current_weather(station: str = DEFAULT_STATION, db: Session = Depends(get_db)):
    record = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == station.upper()
    ).order_by(desc(WeatherRecord.timestamp)).first()
    if record is not None:
        return _record_to_payload(record)

    live = poller.fetch_current_weather(station)
    if live:
        poller.save_weather_record(db, live)
        return _dict_to_payload(live)

    raise HTTPException(status_code=503, detail="Current weather temporarily unavailable for " + station)


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
        cutoff = _utcnow_naive() - datetime.timedelta(hours=hours)
        query = query.filter(WeatherRecord.timestamp >= cutoff)

    records = query.order_by(WeatherRecord.timestamp).all()

    # Downsample to 5-minute intervals: keep the last record within each slot
    INTERVAL_MINUTES = 5
    buckets: dict = {}
    for r in records:
        if r.timestamp is None:
            continue
        slot_min = (r.timestamp.minute // INTERVAL_MINUTES) * INTERVAL_MINUTES
        slot = r.timestamp.replace(minute=slot_min, second=0, microsecond=0)
        buckets[slot] = r

    sampled = [buckets[k] for k in sorted(buckets.keys())]
    if limit:
        sampled = sampled[-limit:]

    return [{
        "obs_time_utc": r.timestamp.strftime("%Y-%m-%dT%H:%M:%SZ") if r.timestamp is not None else None,
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
    } for r in sampled]


@app.get("/api/today")
def get_today_summary(station: str = DEFAULT_STATION, db: Session = Depends(get_db)):
    today_start = _utcnow_naive().replace(hour=0, minute=0, second=0, microsecond=0)
    records = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == station.upper(),
        WeatherRecord.timestamp >= today_start
    ).all()
    if not records:
        return None
    temps = [float(r.temperature) for r in records if isinstance(r.temperature, (int, float))]
    humids = [float(r.humidity) for r in records if isinstance(r.humidity, (int, float))]
    pressures = [float(r.pressure) for r in records if isinstance(r.pressure, (int, float))]
    gusts = [float(r.wind_gust) for r in records if isinstance(r.wind_gust, (int, float))]
    rains = [float(r.precip_total) for r in records if isinstance(r.precip_total, (int, float))]
    uvs = [float(r.uv_index) for r in records if isinstance(r.uv_index, (int, float))]
    solars = [float(r.solar_radiation) for r in records if isinstance(r.solar_radiation, (int, float))]
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


@app.get("/api/daily")
def get_daily_summary(station: str = DEFAULT_STATION, days: int = 30, db: Session = Depends(get_db)):
    cutoff = _utcnow_naive() - datetime.timedelta(days=days)
    records = db.query(WeatherRecord).filter(
        WeatherRecord.station_id == station.upper(),
        WeatherRecord.timestamp >= cutoff
    ).order_by(WeatherRecord.timestamp).all()

    from collections import defaultdict
    by_day = defaultdict(list)
    for r in records:
        if r.timestamp is not None:
            by_day[r.timestamp.strftime("%Y-%m-%d")].append(r)

    result = []
    for day in sorted(by_day.keys(), reverse=True):
        recs = by_day[day]
        temps = [float(r.temperature) for r in recs if isinstance(r.temperature, (int, float))]
        humids = [float(r.humidity) for r in recs if isinstance(r.humidity, (int, float))]
        pressures = [float(r.pressure) for r in recs if isinstance(r.pressure, (int, float))]
        gusts = [float(r.wind_gust) for r in recs if isinstance(r.wind_gust, (int, float))]
        rains = [float(r.precip_total) for r in recs if isinstance(r.precip_total, (int, float))]
        uvs = [float(r.uv_index) for r in recs if isinstance(r.uv_index, (int, float))]
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


@app.get("/api/alerts")
def get_nws_alerts(station: str = DEFAULT_STATION, db: Session = Depends(get_db)):
    st = db.query(Station).filter(Station.station_id == station.upper(), Station.is_active == True).first()
    if not st:
        return []
    try:
        r = http_requests.get(
            f"https://api.weather.gov/alerts/active?point={st.lat},{st.lon}",
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


@app.get("/api/nearby")
def get_nearby_stations(station: str = DEFAULT_STATION, db: Session = Depends(get_db)):
    selected = station.upper()
    nearby = []
    with poller._stations_lock:
        stations_copy = dict(poller.STATIONS)
    for station_id, info in stations_copy.items():
        if station_id == selected:
            continue
        record = (
            db.query(WeatherRecord)
            .filter(WeatherRecord.station_id == station_id)
            .order_by(desc(WeatherRecord.timestamp))
            .first()
        )
        nearby.append({
            "stationID": station_id,
            "neighborhood": info.get("name"),
            "lat": info.get("lat"),
            "lon": info.get("lon"),
            "temp_f": record.temperature if record else None,
            "humidity": record.humidity if record else None,
            "wind_speed": record.wind_speed if record else None,
        })
    return nearby


@app.get("/api/integrity")
def get_integrity_report():
    return poller.check_integrity(days=5)


@app.post("/api/integrity/repair")
def trigger_integrity_repair(background_tasks: BackgroundTasks):
    background_tasks.add_task(poller.repair_integrity, 5)
    return {"status": "repair started", "message": "Integrity repair running in the background."}


# ═══════════════════════════════════════════════════════════════════════════════
# PAGE ROUTES + STATIC FILES
# ═══════════════════════════════════════════════════════════════════════════════

os.makedirs("static/css", exist_ok=True)
os.makedirs("static/js", exist_ok=True)
app.mount("/static", StaticFiles(directory="static"), name="static")


@app.get("/")
def read_root(request: Request, db: Session = Depends(get_db)):
    user = get_current_user(request, db)
    # Merge user's preferred station if logged in
    station = DEFAULT_STATION
    if user and user.settings and user.settings.default_station:
        station = user.settings.default_station
    ctx = _page_ctx(request, db,
                    default_station=station,
                    station_name=_station_name(db, station))
    return templates.TemplateResponse("index.html", ctx)


@app.get("/login")
def login_page(request: Request, db: Session = Depends(get_db)):
    # Server-side redirect for already-authenticated users (no extra round-trip)
    if get_current_user(request, db):
        return RedirectResponse("/", status_code=302)
    return templates.TemplateResponse("login.html", {"request": request, "current_user": None})


@app.get("/settings")
def settings_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("user.html", _page_ctx(request, db))


@app.get("/admin")
def admin_page(request: Request, db: Session = Depends(get_db)):
    return templates.TemplateResponse("admin.html", _page_ctx(request, db))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=9564)
