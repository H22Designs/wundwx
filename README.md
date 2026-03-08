# Weather Dashboard

A professional, real-time weather dashboard that pulls data from multiple Weather Underground stations, stores it in a SQLite database, and displays it on a modern, dark-mode dashboard.

## Features

- **Multi-Station Support**: Monitor multiple PWS stations (KALMILLP10, KALKENNE5, KALMILLP8, etc.).
- **Real-time Data**: Polls Weather Underground API every 30 seconds.
- **Historic Trends**: View 6h, 24h, 48h, and 5d historical data with custom range support.
- **Customizable**: Toggle individual cards and charts; persistent settings in localStorage.
- **Day/Night Graphics**: Weather icons reflect current solar conditions.
- **NWS Alerts**: Displays local watches, warnings, and advisories.

## Installation

### Prerequisites

- Python 3.9+
- A Weather Underground API Key

### Setup

1. **Clone the repository**:
   ```bash
   git clone <your-repo-url>
   cd wx
   ```

2. **Create a virtual environment**:
   ```bash
   python -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

4. **Configuration**:
   - Open `poller.py` and `main.py` to ensure your Weather Underground API key is set correctly in the `API_KEY` / `WU_API_KEY` variables.
   - (Optional) Modify the `STATIONS` list in `poller.py` and `main.py` to track your preferred stations.

### Running the Application

1. **Start the backend and poller**:
   ```bash
   python main.py
   ```
   *Note: On first run, the poller will automatically backfill 5 days of data for each station.*

2. **Access the Dashboard**:
   Open your browser and navigate to `http://localhost:8000`.

## Architecture

- **Backend**: FastAPI (Python)
- **Database**: SQLite with SQLAlchemy ORM
- **Frontend**: Vanilla HTML5, CSS3, and JavaScript
- **Charting**: Chart.js
- **Icons**: Emoji & Dynamic CSS