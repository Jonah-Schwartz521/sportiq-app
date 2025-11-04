from fastapi.testclient import TestClient
from apps.api.app.main import app

client = TestClient(app)

def test_list_predictions_smoke():
    r = client.get("/predictions")
    assert r.status_code == 200
    assert isinstance(r.json(), list)