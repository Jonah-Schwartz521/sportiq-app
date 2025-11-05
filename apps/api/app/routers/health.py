# apps/api/app/routers/health.py
from fastapi import APIRouter

router = APIRouter(prefix="/health", tags=["health"])

@router.get("", summary="Liveness probe")
def health():
    return {"status": "ok"}