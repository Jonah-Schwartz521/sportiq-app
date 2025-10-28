# scripts/seed_demo_events.py
# scripts/seed_demo_events.py
import sys, pathlib
# make repo root importable BEFORE any "from apps..." imports
sys.path.append(str(pathlib.Path(__file__).resolve().parents[1]))

from datetime import datetime
import psycopg
from apps.api.app.core.config import POSTGRES_DSN

EVENT_ROWS = [
    # event_id, sport_id, season, home_team_id, away_team_id, venue
    (2, 1, 2025, 3, 4, "Demo Arena 2"),
    (3, 1, 2025, 1, 6, "Demo Arena 3"),
    (4, 1, 2025, 5, 2, "Demo Arena 4"),
    (5, 1, 2025, 7, 8, "Demo Arena 5"),
    (6, 1, 2025, 1, 8, "Demo Arena 6"),
]

UPSERT_SQL = """
INSERT INTO core.events
(event_id, sport_id, season, date, home_team_id, away_team_id, venue, status, start_time)
VALUES (%s, %s, %s, CURRENT_DATE, %s, %s, %s, 'scheduled', NOW())
ON CONFLICT (event_id) DO UPDATE
SET sport_id = EXCLUDED.sport_id,
    season = EXCLUDED.season,
    date = EXCLUDED.date,
    home_team_id = EXCLUDED.home_team_id,
    away_team_id = EXCLUDED.away_team_id,
    venue = EXCLUDED.venue,
    status = EXCLUDED.status,
    start_time = EXCLUDED.start_time;
"""

def main():
    print(f"[{datetime.utcnow().isoformat()}] Seeding demo events 2..6")
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        # sanity: list teams
        cur.execute("SELECT team_id, name FROM core.teams ORDER BY team_id;")
        teams = cur.fetchall()
        print("Teams:", teams)

        # upsert demo events
        for ev in EVENT_ROWS:
            cur.execute(UPSERT_SQL, ev)
        conn.commit()
        print("Upserted events:", [e[0] for e in EVENT_ROWS])

        # show what we have
        cur.execute("""
            SELECT event_id, home_team_id, away_team_id, start_time
            FROM core.events
            WHERE event_id BETWEEN 1 AND 6
            ORDER BY event_id;
        """)
        rows = cur.fetchall()
        for r in rows:
            print("EVENT:", r)

if __name__ == "__main__":
    main()