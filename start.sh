#!/usr/bin/env bash
# start.sh — Start the WunDWX weather dashboard
#
# Usage:
#   ./start.sh              # foreground (Ctrl+C to stop)
#   ./start.sh -d           # daemon (background, logs → wundwx.log)
#   ./start.sh --monitor    # foreground server + terminal monitor (requires tmux)
#   ./start.sh stop         # stop a running daemon

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
VENV="$SCRIPT_DIR/.venv"
PID_FILE="$SCRIPT_DIR/wundwx.pid"
LOG_FILE="$SCRIPT_DIR/wundwx.log"

# ── Colour helpers ────────────────────────────────────────────────────────────
bold="\033[1m"; green="\033[32m"; yellow="\033[33m"; red="\033[31m"; reset="\033[0m"
info()  { echo -e "${green}[wundwx]${reset} $*"; }
warn()  { echo -e "${yellow}[wundwx]${reset} $*"; }
error() { echo -e "${red}[wundwx]${reset} $*" >&2; exit 1; }

# ── stop subcommand ───────────────────────────────────────────────────────────
if [[ "$1" == "stop" ]]; then
    if [[ ! -f "$PID_FILE" ]]; then
        warn "No PID file found — is the daemon running?"
        exit 0
    fi
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        info "Stopping WunDWX (PID $PID)…"
        kill "$PID"
        rm -f "$PID_FILE"
        info "Stopped."
    else
        warn "Process $PID not found — removing stale PID file."
        rm -f "$PID_FILE"
    fi
    exit 0
fi

# ── Parse flags ───────────────────────────────────────────────────────────────
DAEMON=0
MONITOR=0
for arg in "$@"; do
    case "$arg" in
        -d|--daemon)  DAEMON=1 ;;
        --monitor)    MONITOR=1 ;;
        *) error "Unknown argument: $arg  (usage: ./start.sh [-d] [--monitor] [stop])" ;;
    esac
done

cd "$SCRIPT_DIR"

# ── Virtual environment ───────────────────────────────────────────────────────
if [[ ! -d "$VENV" ]]; then
    info "Creating virtual environment…"
    python3 -m venv "$VENV"
fi

PYTHON="$VENV/bin/python"
PIP="$VENV/bin/pip"

# ── Dependencies ──────────────────────────────────────────────────────────────
info "Checking dependencies…"
"$PIP" install -q -r requirements.txt

# ── Already running? ─────────────────────────────────────────────────────────
if [[ -f "$PID_FILE" ]]; then
    PID=$(cat "$PID_FILE")
    if kill -0 "$PID" 2>/dev/null; then
        warn "WunDWX is already running (PID $PID). Use './start.sh stop' first."
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# ── Launch ────────────────────────────────────────────────────────────────────
if [[ "$DAEMON" == "1" ]]; then
    info "Starting WunDWX in background (logs → $LOG_FILE)…"
    nohup "$PYTHON" main.py >> "$LOG_FILE" 2>&1 &
    echo $! > "$PID_FILE"
    info "Started. PID $(cat "$PID_FILE")  |  ./start.sh stop  to terminate."

elif [[ "$MONITOR" == "1" ]]; then
    command -v tmux >/dev/null 2>&1 || error "--monitor requires tmux (apt install tmux)"
    SESSION="wundwx"
    tmux new-session -d -s "$SESSION" -x 220 -y 50
    # Left pane: web server
    tmux send-keys -t "$SESSION" "$PYTHON main.py" Enter
    # Right pane: terminal monitor
    tmux split-window -h -t "$SESSION"
    tmux send-keys -t "$SESSION" "sleep 3 && $PYTHON monitor.py" Enter
    tmux select-pane -t "$SESSION:0.0"
    info "WunDWX started in tmux session '${SESSION}'."
    info "  Attach : tmux attach -t ${SESSION}"
    info "  Detach : Ctrl+B then D"
    info "  Kill   : tmux kill-session -t ${SESSION}"
    tmux attach -t "$SESSION"

else
    info "Starting WunDWX (http://localhost:9564)  —  Ctrl+C to stop"
    exec "$PYTHON" main.py
fi
