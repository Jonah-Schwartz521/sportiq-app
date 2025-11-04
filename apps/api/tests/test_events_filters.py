from typing import List, Tuple
from datetime import date
import psycopg
import pytest

from apps.api.app.core.config import POSTGRES_DSN

TEST_EVENT_IDS = [99001, 99002, 99003, 99004]


@pytest.fixture(scope="module", autouse=True)
def seed_events():
    """
    Insert a small, controlled set of events and clean them up after.
    Uses NULL team IDs to avoid FKs. Assumes sport_id=1 exists.
    """
    rows: List[Tuple] = [
        # event_id, sport_id, season, date, home_team_id, away_team_id, venue, status, start_time
        (TEST_EVENT_IDS[0], 1, 2025, date.fromisoformat("2025-01-01"), None, None, "T Arena", "scheduled", None),
        (TEST_EVENT_IDS[1], 1, 2025, date.fromisoformat("2025-01-02"), None, None, "T Arena", "in_progress", None),
        (TEST_EVENT_IDS[2], 1, 2025, date.fromisoformat("2025-01-03"), None, None, "T Arena", "final", None),
        (TEST_EVENT_IDS[3], 1, 2025, date.fromisoformat("2025-01-04"), None, None, "T Arena", "scheduled", None),
    ]

    ins_sql = """
        INSERT INTO core.events
            (event_id, sport_id, season, "date", home_team_id, away_team_id, venue, status, start_time)
        VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        ON CONFLICT (event_id) DO NOTHING;
    """
    del_sql = "DELETE FROM core.events WHERE event_id = ANY(%s);"

    # setup: delete any leftovers, then insert our rows
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(del_sql, (TEST_EVENT_IDS,))
        cur.executemany(ins_sql, rows)
        conn.commit()

    # run tests
    yield

    # teardown: clean seeds
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(del_sql, (TEST_EVENT_IDS,))
        conn.commit()


def test_events_out_of_range_returns_empty(client):
    resp = client.get("/events", params={"date_from": "2099-01-01", "limit": 5})
    assert resp.status_code == 200
    data = resp.json()
    assert data["items"] == []
    assert data["total_returned"] == 0


def test_events_filter_by_sport(client):
    resp = client.get("/events", params={"sport_id": 1, "limit": 50})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert all(it["sport_id"] == 1 for it in items)


def test_events_date_range(client):
    # Only 2025-01-02 and 2025-01-03 should match
    resp = client.get("/events", params={"date_from": "2025-01-02", "date_to": "2025-01-03", "limit": 50})
    assert resp.status_code == 200
    ids = [it["event_id"] for it in resp.json()["items"]]
    assert TEST_EVENT_IDS[2] in ids and TEST_EVENT_IDS[1] in ids


def test_events_status_filter(client):
    resp = client.get("/events", params={"status": "scheduled", "limit": 50})
    assert resp.status_code == 200
    items = resp.json()["items"]
    assert all(it["status"] == "scheduled" for it in items)


def test_events_pagination_no_dupes(client):
    page1 = client.get("/events", params={"limit": 2, "offset": 0})
    page2 = client.get("/events", params={"limit": 2, "offset": 2})
    assert page1.status_code == 200 and page2.status_code == 200
    ids1 = {it["event_id"] for it in page1.json()["items"]}
    ids2 = {it["event_id"] for it in page2.json()["items"]}
    assert ids1.isdisjoint(ids2)