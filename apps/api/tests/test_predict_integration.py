# apps/api/tests/test_predict_integration.py
import pytest
from fastapi.testclient import TestClient
from apps.api.app.main import app

pytestmark = pytest.mark.integration

client = TestClient(app)

def test_predict_nba_integration():
    r = client.post("/predict/nba", json={"event_id": 1})
    # TEMP: show body to debug
    print("STATUS:", r.status_code)
    print("BODY:", r.text)
    assert r.status_code == 200