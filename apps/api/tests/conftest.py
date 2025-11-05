import pytest
from starlette.testclient import TestClient
from apps.api.app.main import app

@pytest.fixture(scope="module")
def client():
    """Shared FastAPI test client for this test module"""
    with TestClient(app) as c:
        yield c 
