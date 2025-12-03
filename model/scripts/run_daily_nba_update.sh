#!/bin/bash

# Absolute project root
PROJECT_ROOT="/Volumes/easystore/Projects/sportiq-app"

# Log file (optional but super helpful)
LOG_DIR="$PROJECT_ROOT/logs"
mkdir -p "$LOG_DIR"
LOG_FILE="$LOG_DIR/daily_nba_update.log"

{
  echo "=============================="
  echo "Running daily NBA update at: $(date)"

  cd "$PROJECT_ROOT" || {
    echo "Failed to cd into $PROJECT_ROOT"
    exit 1
  }

  # Use the python inside your virtualenv
  PYTHON="$PROJECT_ROOT/.venv/bin/python"

  # Safety check
  if [ ! -x "$PYTHON" ]; then
    echo "Python not found at $PYTHON"
    exit 1
  fi

  echo "Using Python: $PYTHON"

  # 1) Rebuild processed base games (historical data)
  echo "[1/3] Running build_base_games.py..."
  "$PYTHON" model/scripts/build_base_games.py

  # 2) Attach future schedule
  echo "[2/3] Running build_future_games.py..."
  "$PYTHON" model/scripts/build_future_games.py

  # 3) Add model odds to scheduled games
  echo "[3/3] Running add_model_odds.py..."
  "$PYTHON" model/scripts/add_model_odds.py

  echo "Daily NBA update finished at: $(date)"
} >> "$LOG_FILE" 2>&1