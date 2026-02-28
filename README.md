# wundwx

A lightweight Node.js REST API server that surfaces weather data from the [Weather Underground](https://www.wunderground.com/) (weather.com) API.

## Requirements

- Node.js ≥ 18
- A [Weather Underground API key](https://www.wunderground.com/member/api-keys)

## Setup

```bash
npm install
export WU_API_KEY=your_api_key_here
npm start
```

The server listens on port **3000** by default. Set the `PORT` environment variable to override.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Service info |
| GET | `/api/weather/current` | Current conditions |
| GET | `/api/weather/forecast` | 5-day forecast |

### GET `/api/weather/current`

Query parameters:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `stationId` | one of `stationId` or (`lat` + `lon`) | Personal Weather Station ID (e.g. `KNYNYC123`) |
| `lat` | one of `stationId` or (`lat` + `lon`) | Latitude |
| `lon` | one of `stationId` or (`lat` + `lon`) | Longitude |
| `units` | No | `e` (imperial, default), `m` (metric), `h` (UK hybrid) |

### GET `/api/weather/forecast`

Query parameters:

| Parameter | Required | Description |
|-----------|----------|-------------|
| `lat` | Yes | Latitude |
| `lon` | Yes | Longitude |
| `units` | No | `e` (imperial, default), `m` (metric), `h` (UK hybrid) |

## Development

```bash
npm run dev   # run with --watch for auto-restart
npm test      # run test suite
```