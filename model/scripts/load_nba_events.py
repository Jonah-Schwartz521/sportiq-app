# model/scripts/load_nba_events.py

import csv
import sys
from pathlib import Path
from typing import Optional

from sqlalchemy import delete
from sqlalchemy.orm import Session

# --- make sure the project root is on sys.path ---
ROOT = Path(__file__).resolve().parent.parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

# ⬇️ import SessionLocal + Base + engine from your db module
from model_api.db import SessionLocal, Base, engine
from model_api.schemas import Event


# NOTE: your schedule file lives in data/processed/nba_schedule.csv
DATA_PATH = (
    Path(__file__)
    .resolve()
    .parent
    .parent
    / "data"
    / "processed"
    / "nba_schedule.csv"
)

NBA_SPORT_ID = 1  # adjust if your NBA sport_id is different


def clear_existing_nba_events(db: Session) -> None:
  """Delete existing NBA events so we can reload fresh."""
  stmt = delete(Event).where(Event.sport_id == NBA_SPORT_ID)
  db.execute(stmt)
  db.commit()


def parse_int(value: str) -> Optional[int]:
  value = value.strip()
  if not value:
    return None
  return int(value)


def load_nba_events() -> None:
  if not DATA_PATH.exists():
    raise FileNotFoundError(f"Schedule file not found: {DATA_PATH}")

  # ✅ make sure tables exist in the database
  Base.metadata.create_all(bind=engine)

  db = SessionLocal()
  try:
    clear_existing_nba_events(db)

    with DATA_PATH.open("r", newline="") as f:
      reader = csv.DictReader(f)
      rows = list(reader)

    for row in rows:
      event = Event(
        event_id=int(row["event_id"]),
        sport_id=int(row["sport_id"]),
        date=row["date"],
        home_team_id=parse_int(row["home_team_id"]),
        away_team_id=parse_int(row["away_team_id"]),
        venue=row.get("venue") or None,
        status=row.get("status") or None,
      )
      db.add(event)

    db.commit()
    print(f"Loaded {len(rows)} NBA events from {DATA_PATH}")
  finally:
    db.close()


if __name__ == "__main__":
  load_nba_events()