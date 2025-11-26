"""
Load NBA events into the events table from data/processed/nba_schedule.csv.

This expects a CSV with at least:
  - date
  - sport_id

Optionally:
  - game_id (we'll reuse this as event_id if event_id is missing)
  - event_id
  - venue
  - status
"""

from __future__ import annotations

import sys
from pathlib import Path

# Ensure project root (the directory that contains model_api/) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

import pandas as pd
from sqlalchemy import delete

from model_api.db import SessionLocal
from model_api.schemas import Event

# Path to the schedule CSV
DATA_PATH = (
    Path(__file__)
    .resolve()
    .parents[1]
    / "data"
    / "processed"
    / "nba_schedule.csv"
)


def clear_existing_nba_events(db) -> None:
    """Delete existing NBA events (sport_id = 1) so we can reload cleanly."""
    stmt = delete(Event).where(Event.sport_id == 1)
    db.execute(stmt)
    db.commit()


def load_nba_events() -> None:
    """Load NBA events from CSV into the events table."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Schedule file not found: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df)} rows from {DATA_PATH}")

    # Required columns for our CSV
    required_cols = ["date", "sport_id"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {DATA_PATH}: {missing}")

    # Optional columns we might want for display/debug
    for opt_col in ["venue", "status"]:
        if opt_col not in df.columns:
            df[opt_col] = ""

    session = SessionLocal()
    try:
        clear_existing_nba_events(session)

        count = 0
        for _, row in df.iterrows():
            # Decide what to use for event_id:
            # 1) If CSV has event_id, use it.
            # 2) Else, if CSV has game_id, use that.
            # 3) Else, pass None and let DB autogenerate.
            event_id_val = None
            if "event_id" in df.columns and not pd.isna(row.get("event_id")):
                event_id_val = int(row["event_id"])
            elif "game_id" in df.columns and not pd.isna(row.get("game_id")):
                event_id_val = int(row["game_id"])

            ev = Event(
                event_id=event_id_val,
                sport_id=int(row["sport_id"]),
                date=str(row["date"]),
                home_team_id=None,  # you can wire IDs later if you add them
                away_team_id=None,
                venue=str(row.get("venue") or ""),
                status=str(row.get("status") or ""),
            )

            session.add(ev)
            count += 1

        session.commit()
        print(f"Loaded {count} NBA events from {DATA_PATH}")
    finally:
        session.close()


if __name__ == "__main__":
    load_nba_events()