from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from apps.api.app.adapters.registry import ADAPTERS

router = APIRouter(prefix="/predict", tags=["predict"])

class PredictRequest(BaseModel):
    event_id: Optional[int] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    fighter_a: Optional[str] = None
    fighter_b: Optional[str] = None

class PredictResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    model_key: str
    win_probabilities: Dict[str, float]
    generated_at: str

@router.post("/{sport}", response_model=PredictResponse)
def predict(sport: Literal["nba", "ufc", "mlb"], payload: PredictRequest):
    adapter = ADAPTERS.get(sport)
    if not adapter:
        raise HTTPException(status_code=404, detail=f"Unsupported sport: {sport}")
    data = payload.model_dump()
    try:
        adapter.validate(data)
        result = adapter.predict(data)
        if payload.event_id is not None:
            adapter.persist(int(payload.event_id), result)
        return result
    except ValueError as ve:
        raise HTTPException(status_code=400, detail=str(ve))
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{sport.upper()} prediction error: {e}")