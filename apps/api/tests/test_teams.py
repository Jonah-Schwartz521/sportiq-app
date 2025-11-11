# apps/api/tests/test_teams.py
import psycopg
import pytest

from apps.api.app.core.config import POSTGRES_DSN


def _ensure_seed_teams():
    rows = [
        (7, 1, "Los Angeles Lakers"),
        (8, 1, "Boston Celtics"),
    ]

    insert_sql = """
        INSERT INTO core.teams (team_id, sport_id, name)
        VALUES (%s, %s, %s)
        ON CONFLICT (team_id) DO NOTHING;
    """

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        for team_id, sport_id, name in rows:
            cur.execute(insert_sql, (team_id, sport_id, name))
        conn.commit()


@pytest.fixture(scope="module", autouse=True)
def seed_teams():
    _ensure_seed_teams()
    yield


def test_team_by_id(client):
    _ensure_seed_teams()

    r = client.get("/teams/7")
    assert r.status_code == 200, f"Unexpected {r.status_code}, body={r.text}"
    body = r.json()
    assert body["team_id"] == 7
    assert body["sport_id"] == 1
    assert body["name"] == "Los Angeles Lakers"


def test_teams_list_and_search(client):
    _ensure_seed_teams()

    # basic list
    r = client.get("/teams", params={"limit": 5})
    assert r.status_code == 200
    data = r.json()
    assert "items" in data
    assert any(t["team_id"] == 7 for t in data["items"])

    # filter by sport_id
    r = client.get("/teams", params={"sport_id": 1, "limit": 50})
    assert r.status_code == 200
    items = r.json()["items"]
    assert all(t["sport_id"] == 1 for t in items)

    # partial name search
    r = client.get("/teams", params={"q": "laker", "limit": 10})
    assert r.status_code == 200
    items = r.json()["items"]
    assert any("Lakers" in t["name"] for t in items)