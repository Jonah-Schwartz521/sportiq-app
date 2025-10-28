from fastapi import APIRouter
from apps.api.app.adapters.registry import ADAPTERS

router = APIRouter(prefix="", tags=["meta"])

@router.get("/sports")
def list_sports():
    return {"sports": sorted(ADAPTERS.keys())}