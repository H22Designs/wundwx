from sqlalchemy import create_engine, Column, Integer, Float, String, DateTime
from sqlalchemy.orm import declarative_base, sessionmaker
import datetime

DATABASE_URL = "sqlite:///./weather.db"

engine = create_engine(DATABASE_URL, connect_args={"check_same_thread": False})
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
