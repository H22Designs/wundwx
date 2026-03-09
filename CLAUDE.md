# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Running the Application

```bash
# Create and activate a virtual environment
python -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Start the server (runs on http://localhost:8000)
python main.py
```

On first run, `poll_loop()` automatically triggers `repair_integrity()` which backfills 5 days of historical data per station from the Open-Meteo archive API.

## Architecture

This is a single-process Python FastAPI app with no build step for the frontend.

- **`database.py`** — SQLAlchemy model (`WeatherRecord`) and SQLite engine setup. The DB file `weather.db` is gitignored and created on first run. All timestamps are stored as naive UTC datetimes.
- **`poller.py`** — All external data fetching and DB write logic. Contains the `STATIONS` dict (the source of truth for station IDs, names, and coordinates), `fetch_current_weather()` (Open-Meteo forecast API), `fetch_historical_weather()` (Open-Meteo archive API), `save_weather_record()` (deduplicated upsert by station+timestamp), and `poll_loop()` (runs every 60 s in a daemon thread started by FastAPI's lifespan handler).
- **`main.py`** — FastAPI app with all REST endpoints. Imports `STATIONS` and data-fetch helpers directly from `poller`. The `DEFAULT_STATION` is `KALMILLP10`.
- **`static/`** — Served as-is. `index.html` is the SPA entry point; `app.js` and `js/wu_dashboard.js` are the frontend JS; `style.css` and `css/style.css` are the stylesheets. No bundler or transpiler.
- **`ref_app.js`, `ref.html`, `ref_style.css`** — Reference/prototype files; not served by the app.

## API Endpoints

| Method | Path | Key query params |
|--------|------|-----------------|
| GET | `/api/stations` | — |
| GET | `/api/current` | `station` |
| GET | `/api/history` | `station`, `hours`, `start`, `end`, `limit` |
| GET | `/api/today` | `station` |
| GET | `/api/daily` | `station`, `days` |
| GET | `/api/alerts` | `station` |
| GET | `/api/nearby` | `station` |
| GET | `/api/integrity` | — |
| POST | `/api/integrity/repair` | — |

All `station` params default to `KALMILLP10` and are uppercased server-side.

## Adding or Modifying Stations

**Open-Meteo stations**: Add to both `STATIONS` and `OPENMETEO_STATIONS` sets in `poller.py`. Each entry needs `name`, `lat`, and `lon`. The `DEFAULT_STATION` in `main.py:71` should also be updated if changing the primary station.

**CWOP/APRS stations**: Add the callsign to `STATIONS` (with estimated coords) and to `CWOP_CALLSIGNS`. The APRS listener will auto-refine coordinates from the first received packet. Update `APRS_IS_FILTER` if the new station is outside the current 100 km radius. No historical backfill is available for CWOP stations.

## Data Sources & Flow

Two parallel ingestion paths write to the same `weather_records` table:

1. **Open-Meteo** (KALMILLP10, KALKENNE5, KALMILLP8): polled every 60 s via `poll_loop()`. Hourly model updates mean values change ~once/hour despite the 60-second poll cadence.

2. **APRS-IS / CWOP** (GW7151, FW4617): `aprs_listener_loop()` holds a persistent TCP connection to `rotate.aprs2.net:14580`. Uses a range+type filter (`t/w r/lat/lon/radius`) because the budlist (`b/`) filter requires a verified ham-radio login. Client-side filters to `CWOP_CALLSIGNS`. Stations push every ~5 min; data arrives within seconds. Auto-reconnects on disconnect.

Historical gaps are filled via Open-Meteo archive API (`fetch_historical_weather()`). The integrity system (`check_integrity` / `repair_integrity`) detects NULL-temperature records and missing hourly slots; CWOP stations are skipped for Open-Meteo backfill.
