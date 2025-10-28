from fastapi.testclient import TestClient
from apps.api.app.main import app

client = TestClient(app)

def test_health():
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"

def test_predict_nba():
    r = client.post("/predict/nba", json={"event_id": 1})
    assert r.status_code == 200
    assert "win_probabilities" in r.json()

def test_list_predictions():
    r = client.get("/predictions")
    assert r.status_code == 200
    assert isinstance(r.json(), list)