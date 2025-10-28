# apps/api/app/routers/predict.py
from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timezone
import psycopg

from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.nba_model import predict_winprob  # NBA service

router = APIRouter(prefix="/predict", tags=["predict"])

# ----- Schemas -----
class PredictRequest(BaseModel):
    event_id: Optional[int] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    fighter_a: Optional[str] = None
    fighter_b: Optional[str] = None

class PredictResponse(BaseModel):
    # allow "model_key" without pydantic protected namespace warnings
    model_config = ConfigDict(protected_namespaces=())
    model_key: str
    win_probabilities: Dict[str, float]
    generated_at: str

# ----- Helpers -----
def _persist_prediction(event_id: int, model_key: str, home_wp: float, away_wp: float) -> None:
    """Best-effort insert to core.predictions; do nothing if exists."""
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
        # Keep API responsive; add logging if you want to trace failures.
        pass

def _predict_ufc(_: PredictRequest) -> Dict[str, float]:
    # Placeholder: return fighter-oriented keys for now
    return {"fighter_a": 0.55, "fighter_b": 0.45}

# ----- Route -----
@router.post("/{sport}", response_model=PredictResponse)
def predict(sport: Literal["nba", "ufc"], payload: PredictRequest):
    if sport == "nba":
        if payload.event_id is None:
            raise HTTPException(status_code=400, detail="Missing event_id for NBA prediction.")
        try:
            result = predict_winprob(payload.event_id)
            probs = result["win_probabilities"]
            mk = result["model_key"]

            # Persist (home/away) if event_id present
            _persist_prediction(
                payload.event_id,
                mk,
                float(probs.get("home", 0.0)),
                float(probs.get("away", 0.0)),
            )
            return result
        except HTTPException:
            # bubble up nicely-formatted errors from service layer
            raise
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"NBA prediction error: {e}")

    elif sport == "ufc":
        # Demo-only UFC placeholder
        probs = _predict_ufc(payload)
        mk = "ufc-winprob-0.1.0"

        # Try to persist if event_id is supplied; map fighter_a/b -> home/away
        if payload.event_id is not None:
            _persist_prediction(
                payload.event_id, mk,
                float(probs.get("fighter_a", 0.0)),
                float(probs.get("fighter_b", 0.0)),
            )

        return {
            "model_key": mk,
            "win_probabilities": probs,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # Literal guard should prevent this
    raise HTTPException(status_code=400, detail="Unsupported sport")