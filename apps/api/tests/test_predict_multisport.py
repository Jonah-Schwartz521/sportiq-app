import pytest
from fastapi.testclient import TestClient
from apps.api.app.main import app

client = TestClient(app)

@pytest.mark.parametrize("sport", ["nba", "ufc", "mlb"])
def test_predict_multisport(sport):
    r = client.post(f"/predict/{sport}", json={"event_id": 1})
    assert r.status_code == 200
    body = r.json()
    assert "model_key" in body
    assert "win_probabilities" in body