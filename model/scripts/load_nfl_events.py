"""
Load NFL events into the events table from data/processed/nfl_schedule.csv.

Expected CSV columns:
  - date
Optionally:
  - game_id (we'll reuse this as event_id if event_id is missing)
  - event_id
  - venue
  - status
  - sport_id (if present, otherwise we fall back to NFL_SPORT_ID)
"""

from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
from sqlalchemy import delete, Table, Column, Integer

# Ensure project root (the directory that contains model_api/) is on sys.path
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from model_api.db import SessionLocal, Base, engine
from model_api.schemas import Event  # Prediction imported if you need later

# 🔹 Set this to whatever sport_id you use for NFL in your database
NFL_SPORT_ID = 3  # <-- adjust if your sport IDs are different

# Path to the schedule CSV
DATA_PATH = (
    Path(__file__)
    .resolve()
    .parents[1]
    / "data"
    / "processed"
    / "nfl_schedule.csv"
)


def ensure_db_schema() -> None:
    """Ensure the database schema (including events) exists.

    Same pattern as load_nba_events/load_nhl_events: register a minimal
    `teams` table so foreign keys from Event can resolve, then create all tables.
    """
    if "teams" not in Base.metadata.tables:
        Table(
            "teams",
            Base.metadata,
            Column("teams_id", Integer, primary_key=True),
        )

    Base.metadata.create_all(bind=engine)


def clear_existing_nfl_events(db) -> None:
    """Delete existing NFL events so we can reload cleanly."""
    stmt = delete(Event).where(Event.sport_id == NFL_SPORT_ID)
    db.execute(stmt)
    db.commit()


def load_nfl_events() -> None:
    """Load NFL events from CSV into the events table."""
    if not DATA_PATH.exists():
        raise FileNotFoundError(f"Schedule file not found: {DATA_PATH}")

    df = pd.read_csv(DATA_PATH)
    print(f"Loaded {len(df)} rows from {DATA_PATH}")

    # We at least need a date column
    required_cols = ["date"]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in {DATA_PATH}: {missing}")

    # Optional helper columns
    for opt_col in ["venue", "status", "sport_id"]:
        if opt_col not in df.columns:
            df[opt_col] = ""

    ensure_db_schema()
    session = SessionLocal()
    try:
        clear_existing_nfl_events(session)

        count = 0
        for _, row in df.iterrows():
            # Decide event_id:
            event_id_val = None

            # Prefer a numeric event_id column if present
            raw_event_id = row.get("event_id")
            if pd.notna(raw_event_id) and str(raw_event_id).strip() != "":
                try:
                    event_id_val = int(raw_event_id)
                except (TypeError, ValueError):
                    event_id_val = None

            # Fallback: try game_id *only if* it looks numeric
            if event_id_val is None and "game_id" in df.columns:
                raw_game_id = row.get("game_id")
                if pd.notna(raw_game_id) and str(raw_game_id).strip().isdigit():
                    try:
                        event_id_val = int(raw_game_id)
                    except (TypeError, ValueError):
                        event_id_val = None

            # sport_id from CSV if present, else default to NFL_SPORT_ID
            sport_id_val = (
                int(row["sport_id"])
                if "sport_id" in df.columns and str(row.get("sport_id")).strip() != ""
                else NFL_SPORT_ID
            )

            ev = Event(
                event_id=event_id_val,
                sport_id=sport_id_val,
                date=str(row["date"]),
                home_team_id=None,  # you can wire real team IDs later
                away_team_id=None,
                venue=str(row.get("venue") or ""),
                status=str(row.get("status") or ""),
            )
            session.add(ev)
            count += 1

        session.commit()
        print(f"Loaded {count} NFL events from {DATA_PATH}")
    finally:
        session.close()


if __name__ == "__main__":
    load_nfl_events()