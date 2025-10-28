# apps/api/tests/conftest.py
import os
import pytest
from fastapi.testclient import TestClient

# Make sure the app imports exactly as you run uvicorn: apps.api.app.main:app
from apps.api.app.main import app

@pytest.fixture(scope="session", autouse=True)
def _set_test_env():
    # Safe defaults so imports don’t explode, even if DB is down
    os.environ.setdefault("POSTGRES_HOST", "localhost")
    os.environ.setdefault("POSTGRES_PORT", "5433")
    os.environ.setdefault("POSTGRES_USER", "sportiq")
    os.environ.setdefault("POSTGRES_PASSWORD", "sportiq")
    os.environ.setdefault("POSTGRES_DB", "sportiq")
    os.environ.setdefault("API_PORT", "8000")
    yield

@pytest.fixture()
def client():
    return TestClient(app)