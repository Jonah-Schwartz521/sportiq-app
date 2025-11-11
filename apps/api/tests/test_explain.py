# apps/api/tests/test_explain.py
import psycopg

from apps.api.app.core.config import POSTGRES_DSN


def test_explain_nba_smoke(client):
    # use some event_id; 1 is fine in test DB, or just rely on insert
    event_id = 1

    r = client.get(f"/explain/nba?event_id={event_id}")
    assert r.status_code == 200

    body = r.json()
    assert body["event_id"] == event_id
    assert body["sport"] == "nba"
    assert body["model_key"].startswith("nba-winprob-")
    assert isinstance(body["top_reasons"], list)
    assert len(body["top_reasons"]) >= 1
    assert "feature_name" in body["top_reasons"][0]


def test_explain_ufc_smoke(client):
    event_id = 2

    r = client.get(f"/explain/ufc?event_id={event_id}")
    assert r.status_code == 200

    body = r.json()
    assert body["event_id"] == event_id
    assert body["sport"] == "ufc"
    assert body["model_key"].startswith("ufc-winprob-")
    assert isinstance(body["top_reasons"], list)
    assert len(body["top_reasons"]) >= 1


def test_explain_unsupported_sport(client):
    r = client.get("/explain/mls?event_id=1")
    assert r.status_code == 422 or r.status_code == 400