from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime, event
from sqlalchemy.orm import declarative_base, sessionmaker
import datetime

DATABASE_URL = "sqlite:///./weather.db"

engine = create_engine(
    DATABASE_URL,
    connect_args={"check_same_thread": False, "timeout": 30},
)

@event.listens_for(engine, "connect")
def _set_sqlite_pragmas(dbapi_conn, _rec):
    cur = dbapi_conn.cursor()
    cur.execute("PRAGMA journal_mode=WAL")   # concurrent reads + writes
    cur.execute("PRAGMA busy_timeout=30000") # wait up to 30 s on lock
    cur.close()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

class WeatherRecord(Base):
    __tablename__ = "weather_records"

    id = Column(Integer, primary_key=True, index=True)
    station_id = Column(String, index=True, default="KALMILLP10")
    timestamp = Column(DateTime, default=datetime.datetime.utcnow, index=True)

    # Core Metrics
    temperature = Column(Float)
    humidity = Column(Float)
    dew_point = Column(Float)
    heat_index = Column(Float)
    wind_chill = Column(Float)

    # Wind Metrics
    wind_speed = Column(Float)
    wind_dir = Column(Integer)
    wind_gust = Column(Float)

    # Pressure & Precipitation
    pressure = Column(Float)
    precip_rate = Column(Float)
    precip_total = Column(Float)

    # Solar
    solar_radiation = Column(Float)
    uv_index = Column(Float)

Base.metadata.create_all(bind=engine)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
