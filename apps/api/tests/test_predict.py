# apps/api/tests/test_predict.py
import pytest

def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}

def test_predict_nba_happy_path_mocked(client, monkeypatch):
    # We mock the service so no DB or model is required.
    def fake_predict(event_id: int):
        return {
            "model_key": "nba-winprob-TEST",
            "win_probabilities": {"home": 0.55, "away": 0.45},
            "generated_at": "2025-01-01T00:00:00Z",
        }

    # Patch the symbol exactly as used by the router
    monkeypatch.setattr(
        "apps.api.app.adapters.nba.predict_winprob",
        fake_predict,
        raising=True,
    )

    r = client.post("/predict/nba", json={"event_id": 123})
    assert r.status_code == 200
    body = r.json()
    assert body["model_key"] == "nba-winprob-TEST"
    assert set(body["win_probabilities"]) == {"home", "away"}
    assert 0 <= body["win_probabilities"]["home"] <= 1

def test_predict_ufc_placeholder(client):
    r = client.post("/predict/ufc", json={"fighter_a": "A", "fighter_b": "B"})
    assert r.status_code == 200
    body = r.json()
    assert body["model_key"].startswith("ufc-winprob-")
    assert set(body["win_probabilities"]) == {"fighter_a", "fighter_b"}

def test_predict_missing_event_id_for_team_sport(client):
    r = client.post("/predict/nba", json={})
    assert r.status_code == 400
    body = r.json()

    # Support both default FastAPI and potential custom error envelope
    message = body.get("detail") or body.get("error", {}).get("message", "")
    assert "Missing event_id" in message

def test_predict_unsupported_sport(client):
    r = client.post("/predict/soccer", json={"event_id": 123})

    # Unsupported sport should fail path param validation -> 422 via our global handler
    assert r.status_code == 422

    body = r.json()
    # Matches our custom validation_exception_handler payload
    assert "error" in body
    assert body["error"]["code"] == "VALIDATION_ERROR"
    assert body["error"]["type"] == "validation_error"