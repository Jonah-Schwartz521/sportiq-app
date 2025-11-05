from datetime import date
import psycopg
from apps.api.app.core.config import POSTGRES_DSN

TEST_ID = 99123

def _seed_event(event_id: int = TEST_ID):
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute("DELETE FROM core.events WHERE event_id = %s", (event_id,))
        cur.execute("""
            INSERT INTO core.events
            (event_id, sport_id, season, "date", home_team_id, away_team_id, venue, status, start_time)
            VALUES (%s, %s, %s, %s, NULL, NULL, %s, %s, NULL)
            ON CONFLICT (event_id) DO NOTHING
        """, (event_id, 1, 2025, date.fromisoformat("2025-01-10"), "Test Arena", "scheduled"))
        conn.commit()

def test_get_event_by_id(client):
    _seed_event(TEST_ID)
    r = client.get(f"/events/{TEST_ID}")
    assert r.status_code == 200
    body = r.json()
    assert body["event_id"] == TEST_ID
    assert body["sport_id"] == 1