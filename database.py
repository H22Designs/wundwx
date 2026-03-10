from sqlalchemy import (
    create_engine, Column, Integer, Float, String, DateTime, Boolean,
    ForeignKey, event, Text,
)
from sqlalchemy.orm import declarative_base, sessionmaker, relationship
import datetime

DATABASE_URL = "sqlite:///./weather.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
)


@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _rec):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")
    cur.execute("PRAGMA busy_timeout=30000")
    cur.close()


SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()


# ── Weather data ──────────────────────────────────────────────────────────────
class WeatherRecord(Base):
    __tablename__ = "weather_records"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, index=True, default="KALMILLP10")
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)

    temperature = Column(Float)
    humidity = Column(Float)
    dew_point = Column(Float)
    heat_index = Column(Float)
    wind_chill = Column(Float)

    wind_speed = Column(Float)
    wind_dir = Column(Integer)
    wind_gust = Column(Float)

    pressure = Column(Float)
    precip_rate = Column(Float)
    precip_total = Column(Float)

    solar_radiation = Column(Float)
    uv_index = Column(Float)


# ── Users & auth ──────────────────────────────────────────────────────────────
class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(50), unique=True, nullable=False, index=True)
    email = Column(String(120), unique=True, nullable=False, index=True)
    hashed_password = Column(String, nullable=False)
    is_admin = Column(Boolean, default=False)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)
    last_login = Column(DateTime, nullable=True)

    settings = relationship("UserSettings", uselist=False, back_populates="user",
                            cascade="all, delete-orphan")
    favorites = relationship("UserFavoriteStation", back_populates="user",
                             cascade="all, delete-orphan",
                             order_by="UserFavoriteStation.display_order")


class UserSettings(Base):
    __tablename__ = "user_settings"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),
                     unique=True, nullable=False)
    default_station = Column(String, nullable=True)
    temp_unit = Column(String(1), default="f")       # "f" or "c"
    theme = Column(String(20), default="dark")
    refresh_interval = Column(Integer, default=30)    # seconds
    dashboard_layout = Column(Text, default="")       # JSON blob for card visibility/order

    user = relationship("User", back_populates="settings")


class UserFavoriteStation(Base):
    __tablename__ = "user_favorite_stations"

    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"),
                     nullable=False, index=True)
    station_id = Column(String, nullable=False)
    display_order = Column(Integer, default=0)
    added_at = Column(DateTime, default=datetime.datetime.utcnow)

    user = relationship("User", back_populates="favorites")


# ── DB-managed stations ───────────────────────────────────────────────────────
class Station(Base):
    __tablename__ = "stations"

    id = Column(Integer, primary_key=True)
    station_id = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    cwop_callsign = Column(String, nullable=True, default="")
    source_type = Column(String, default="openmeteo")  # "openmeteo", "cwop", or "wunderground"
    api_key = Column(String, nullable=True, default="")  # WU API key (wunderground source only)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.datetime.utcnow)


Base.metadata.create_all(bind=engine)


def _migrate():
    """Apply any missing schema changes to an existing database."""
    with engine.connect() as conn:
        # Add api_key column if upgrading from a pre-WU version
        try:
            conn.execute(
                __import__("sqlalchemy").text(
                    "ALTER TABLE stations ADD COLUMN api_key TEXT DEFAULT ''"
                )
            )
            conn.commit()
            print("[db] Migrated: added stations.api_key column")
        except Exception:
            pass  # Column already exists


_migrate()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
