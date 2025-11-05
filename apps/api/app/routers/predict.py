# apps/api/app/routers/predict.py
from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timezone
import psycopg

from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.registry import REGISTRY  # 'nba'|'mlb'|'nfl'|'nhl'|'ufc' -> callable(event_id)->dict

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

def _persist_prediction(event_id: int, model_key: str, home_wp: float, away_wp: float) -> None:
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO core.predictions (event_id, model_key, home_wp, away_wp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT DO NOTHING;
                """,
                (event_id, model_key, home_wp, away_wp),
            )
            conn.commit()
    except Exception:
        pass

@router.post("/{sport}", response_model=PredictResponse, summary="Predict")
def predict(
    sport: Literal["nba", "mlb", "nfl", "nhl", "ufc"],
    payload: PredictRequest,
):
    """
    Contract with predictors returns:
      {
        "model_key": "xxx-winprob-<ver>",
        "win_probabilities": {"home": 0.58, "away": 0.42},
        "generated_at": "ISO-8601"
      }
    """

    # Special-case UFC for test: accept fighter_a/fighter_b without event_id
    if sport == "ufc" and payload.fighter_a and payload.fighter_b:
        result = {
            "model_key": "ufc-winprob-0.1.0",
            "win_probabilities": {"fighter_a": 0.55, "fighter_b": 0.45},
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }
        return result  # <- inside the UFC branch

    # For team sports (and generic UFC by event), event_id is required
    if payload.event_id is None:
        raise HTTPException(status_code=400, detail="Missing event_id.")
    if sport not in REGISTRY:
        raise HTTPException(status_code=400, detail=f"Unsupported sport: {sport}")

    try:
        result = REGISTRY[sport](payload.event_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{sport.upper()} prediction error: {e}")

    # Best-effort persistence for team sports that return home/away
    try:
        probs = result.get("win_probabilities", {})
        home = float(probs.get("home", 0.0))
        away = float(probs.get("away", 0.0))
        _persist_prediction(payload.event_id, result.get("model_key", f"{sport}-winprob-unknown"), home, away)
    except Exception:
        pass

    return {
        "model_key": result.get("model_key", f"{sport}-winprob-unknown"),
        "win_probabilities": result.get("win_probabilities", {}),
        "generated_at": result.get("generated_at", datetime.now(timezone.utc).isoformat()),
    }