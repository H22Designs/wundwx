#!/usr/bin/env python3
"""
monitor.py — Live terminal dashboard for WunDWX weather stations.

Displays a color-coded metrics table per station, updated in real-time as new
data arrives, with a verbose log panel scrolling below.

Usage:
    python monitor.py
"""

import builtins
import collections
import datetime
import threading
import time

# ── Log capture (must happen before importing poller) ─────────────────────────
_LOG_QUEUE: collections.deque = collections.deque(maxlen=200)
_LOG_LOCK = threading.Lock()
_ORIG_PRINT = builtins.print


def _capturing_print(*args, sep=" ", end="\n", file=None, flush=False):
    line = sep.join(str(a) for a in args)
    ts = datetime.datetime.now().strftime("%H:%M:%S")
    with _LOG_LOCK:
        _LOG_QUEUE.append(f"[dim]{ts}[/dim]  {line}")


builtins.print = _capturing_print

# ── Import poller AFTER patching print so all poller output is captured ───────
from database import SessionLocal, WeatherRecord  # noqa: E402
from poller import STATIONS, poll_loop            # noqa: E402

from rich import box                              # noqa: E402
from rich.console import Console                  # noqa: E402
from rich.layout import Layout                    # noqa: E402
from rich.live import Live                        # noqa: E402
from rich.panel import Panel                      # noqa: E402
from rich.table import Table                      # noqa: E402
from rich.text import Text                        # noqa: E402


# ── Wind direction helper ─────────────────────────────────────────────────────
def _deg_to_compass(deg):
    if deg is None:
        return ""
    dirs = ["N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"]
    return dirs[int((deg + 11.25) / 22.5) % 16]


# ── Colorizers ────────────────────────────────────────────────────────────────
def _color_temp(val):
    if val is None:
        return Text("—", style="dim")
    if val < 32:
        style = "bold blue"
    elif val < 55:
        style = "cyan"
    elif val < 75:
        style = "bright_green"
    elif val < 90:
        style = "yellow"
    else:
        style = "bold red"
    return Text(f"{val:.1f}°F", style=style)


def _color_humidity(val):
    if val is None:
        return Text("—", style="dim")
    if val < 40:
        style = "yellow"
    elif val < 65:
        style = "bright_green"
    elif val < 80:
        style = "yellow"
    else:
        style = "bold red"
    return Text(f"{val:.0f}%", style=style)


def _color_wind(speed, gust, direction):
    if speed is None:
        return Text("—", style="dim")
    if speed < 5:
        style = "bright_green"
    elif speed < 15:
        style = "yellow"
    elif speed < 25:
        style = "orange3"
    else:
        style = "bold red"
    compass = _deg_to_compass(direction)
    gust_str = f"  G{gust:.0f}" if gust else ""
    return Text(f"{speed:.1f} mph {compass}{gust_str}", style=style)


def _color_uv(val):
    if val is None:
        return Text("—", style="dim")
    if val < 3:
        style = "bright_green"
    elif val < 6:
        style = "yellow"
    elif val < 8:
        style = "orange3"
    else:
        style = "bold red"
    return Text(f"{val:.1f}", style=style)


def _color_precip(val):
    if val is None or val == 0.0:
        return Text("0.00\"", style="dim")
    return Text(f'{val:.2f}"', style="bright_cyan")


def _fmt(val, fmt=".1f", unit=""):
    if val is None:
        return Text("—", style="dim")
    return Text(f"{val:{fmt}}{unit}")


def _age_text(ts):
    age_s = (datetime.datetime.utcnow() - ts).total_seconds()
    mins = int(age_s / 60)
    if mins < 5:
        style, label = "bright_green", f"{mins}m ago"
    elif mins < 30:
        style, label = "yellow", f"{mins}m ago"
    elif mins < 120:
        style, label = "red", f"{mins}m ago"
    else:
        hrs = mins // 60
        style, label = "bold red", f"{hrs}h ago"
    return Text(label, style=style)


# ── Database query ────────────────────────────────────────────────────────────
def fetch_latest():
    db = SessionLocal()
    try:
        result = {}
        for sid in STATIONS:
            rec = (
                db.query(WeatherRecord)
                .filter(WeatherRecord.station_id == sid)
                .order_by(WeatherRecord.timestamp.desc())
                .first()
            )
            result[sid] = rec
        return result
    finally:
        db.close()


# ── Table builder ─────────────────────────────────────────────────────────────
def build_table(latest):
    now_str = datetime.datetime.utcnow().strftime("%Y-%m-%d  %H:%M:%S UTC")
    table = Table(
        box=box.ROUNDED,
        border_style="bright_blue",
        header_style="bold white on dark_blue",
        show_header=True,
        expand=True,
        title="[bold bright_white] WunDWX  Weather  Stations [/bold bright_white]",
        caption=f"[dim]Last refresh: {now_str}[/dim]",
    )

    table.add_column("Station",     no_wrap=True, min_width=18)
    table.add_column("Updated",     no_wrap=True, min_width=8)
    table.add_column("Temp",        no_wrap=True, min_width=9)
    table.add_column("Feels Like",  no_wrap=True, min_width=9)
    table.add_column("Humidity",    no_wrap=True, min_width=8)
    table.add_column("Dew Pt",      no_wrap=True, min_width=9)
    table.add_column("Wind",        no_wrap=True, min_width=18)
    table.add_column("Pressure",    no_wrap=True, min_width=10)
    table.add_column("Precip",      no_wrap=True, min_width=7)
    table.add_column("Solar W/m²",  no_wrap=True, min_width=10)
    table.add_column("UV",          no_wrap=True, min_width=4)

    for sid, info in STATIONS.items():
        rec = latest.get(sid)
        name = info.get("name", sid)
        label = Text()
        label.append(f"{name}\n", style="bold cyan")
        label.append(sid, style="dim")

        if rec is None:
            dash = Text("—", style="dim")
            table.add_row(label, *([dash] * 10))
            continue

        feels = rec.heat_index if rec.heat_index is not None else rec.wind_chill

        table.add_row(
            label,
            _age_text(rec.timestamp),
            _color_temp(rec.temperature),
            _color_temp(feels),
            _color_humidity(rec.humidity),
            _color_temp(rec.dew_point),
            _color_wind(rec.wind_speed, rec.wind_gust, rec.wind_dir),
            _fmt(rec.pressure, ".2f", " inHg"),
            _color_precip(rec.precip_total),
            _fmt(rec.solar_radiation, ".0f", ""),
            _color_uv(rec.uv_index),
        )

    return table


# ── Log panel builder ─────────────────────────────────────────────────────────
def build_log_panel(rows=22):
    with _LOG_LOCK:
        lines = list(_LOG_QUEUE)[-(rows):]
    body = "\n".join(lines) if lines else "[dim]Waiting for log entries…[/dim]"
    return Panel(
        body,
        title="[bold] Verbose Log [/bold]",
        border_style="steel_blue",
        padding=(0, 1),
    )


# ── Layout ────────────────────────────────────────────────────────────────────
def build_layout(latest):
    layout = Layout()
    layout.split_column(
        Layout(build_table(latest), name="table", ratio=2),
        Layout(build_log_panel(22), name="log",   ratio=3),
    )
    return layout


# ── Entry point ───────────────────────────────────────────────────────────────
def main():
    threading.Thread(target=poll_loop, daemon=True).start()

    console = Console()
    with Live(console=console, refresh_per_second=2, screen=True) as live:
        while True:
            live.update(build_layout(fetch_latest()))
            time.sleep(0.5)


if __name__ == "__main__":
    main()
