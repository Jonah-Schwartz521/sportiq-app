from fastapi.testclient import TestClient
from model_api.main import app

client = TestClient(app)


def test_health():
    resp = client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"
    assert data["num_games"] > 0


def test_events_and_predict():
    # get first event
    resp = client.get("/events?limit=1")
    assert resp.status_code == 200
    events = resp.json()["items"]
    assert len(events) == 1
    game_id = events[0]["event_id"]

    # call predict_by_game_id
    resp = client.get(f"/predict_by_game_id?game_id={game_id}")
    assert resp.status_code == 200
    pred = resp.json()
    assert pred["game_id"] == game_id
    assert 0 <= pred["p_home"] <= 1
    assert 0 <= pred["p_away"] <= 1
    # sanity: probs roughly sum to 1
    assert abs(pred["p_home"] + pred["p_away"] - 1.0) < 1e-6


def test_insights():
    resp = client.get("/events?limit=1")
    game_id = resp.json()["items"][0]["event_id"]

    resp = client.get(f"/insights/{game_id}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["game_id"] == game_id
    assert len(data["insights"]) > 0