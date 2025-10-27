from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel
from typing import Optional, Literal
from pydantic import BaseModel, ConfigDict

router = APIRouter()

class PredictRequest(BaseModel):
    event_id: Optional[str] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    fighter_a: Optional[str] = None
    fighter_b: Optional[str] = None

class PredictResponse(BaseModel):
    model_key: str
    win_probabilities: dict
    generated_at: str

@router.post("/{sport}", response_model=PredictResponse)
def predict(sport: Literal["nba","ufc"], payload: PredictRequest):
    # Placeholder logic: return dummy 0.60 / 0.40
    if sport == "nba":
        probs = {"home": 0.60, "away": 0.40}
        mk = "nba-winprob-0.1.0"
    elif sport == "ufc":
        probs = {"fighter_a": 0.55, "fighter_b": 0.45}
        mk = "ufc-winprob-0.1.0"
    else:
        raise HTTPException(400, "Unsupported sport")

    from datetime import datetime, timezone
    return {
        "model_key": mk,
        "win_probabilities": probs,
        "generated_at": datetime.now(timezone.utc).isoformat()
    }
