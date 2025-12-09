from __future__ import annotations

import sys
from pathlib import Path

from sqlalchemy import select

# Find project root that contains model_api/
CURR_PATH = Path(__file__).resolve()
ROOT = None
for parent in CURR_PATH.parents:
    if (parent / "model_api").is_dir():
        ROOT = parent
        break

if ROOT is None:
    raise RuntimeError(f"Could not find project root containing 'model_api' starting from {CURR_PATH}")

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from model_api.db import SessionLocal
from model_api.schemas import Event

NHL_SPORT_ID = 4  # ← MUST match load_nhl_events.py

def main() -> None:
    session = SessionLocal()
    try:
        stmt = select(Event).where(Event.sport_id == NHL_SPORT_ID)
        nhl_events = session.execute(stmt).scalars().all()
        print("NHL events in DB:", len(nhl_events))

        for e in nhl_events[:5]:
            print({
                "event_id": e.event_id,
                "date": e.date,
                "home_team_id": e.home_team_id,
                "away_team_id": e.away_team_id,
                "status": e.status,
                "sport_id": e.sport_id,
            })
    finally:
        session.close()

if __name__ == "__main__":
    main()