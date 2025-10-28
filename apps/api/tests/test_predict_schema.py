from apps.api.app.main import app
from fastapi.testclient import TestClient
client = TestClient(app)

def test_predict_schema():
    r = client.post("/predict/nba", json={"event_id": 1})
    j = r.json()
    assert r.status_code == 200
    assert "model_key" in j and j["model_key"].startswith("nba-winprob-")
    wp = j["win_probabilities"]
    assert set(wp.keys()) == {"home","away"}
    assert 0.0 <= wp["home"] <= 1.0
    assert 0.0 <= wp["away"] <= 1.0