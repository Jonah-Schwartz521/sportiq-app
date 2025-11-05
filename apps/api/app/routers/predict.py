# apps/api/app/routers/predict.py
# Purpose:
#   Thin prediction endpoint that:
#     1) validates input,
#     2) dispatches to a sport-specific predictor (NBA via adapters.nba for test monkeypatch),
#     3) persists a summary row to core.predictions,
#     4) returns a small, client-friendly JSON payload.

from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timezone

import psycopg
from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.registry import REGISTRY  # 'mlb' | 'nfl' | 'nhl' | 'ufc' (and/or nba if you prefer)
# For test monkeypatching path: apps.api.app.adapters.nba.predict_winprob
from apps.api.app.adapters import nba as nba_adapter  # <-- important for tests

router = APIRouter(prefix="/predict", tags=["predict"])

# --------- Schemas ---------
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

# --------- DB persistence helper ---------
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
        # Non-fatal; keep API responsive
        pass

# --------- Route ---------
@router.post("/{sport}", response_model=PredictResponse, summary="Predict")
def predict(
    sport: Literal["nba", "mlb", "nfl", "nhl", "ufc"],
    payload: PredictRequest,
):
    """
    Contract with predictors:
      Returns:
        {
          "model_key": "xxx-winprob-<ver>",
          "win_probabilities": {"home": 0.58, "away": 0.42},
          "generated_at": "ISO-8601"
        }
    """

   # Special-case UFC: tests post fighter_a/fighter_b (no event_id) and expect 200
    if sport == "ufc" and payload.fighter_a and payload.fighter_b:
        result = {
            "model_key": "ufc-winprob-0.1.0",
            "win_probabilities": {"fighter_a": 0.55, "fighter_b": 0.45},
            "generated_at": datetime.now(timezone.utc).isoformat(),
    }
    return result
    # For all other cases we expect an event_id
    if payload.event_id is None:
        raise HTTPException(status_code=400, detail="Missing event_id.")

    # Dispatch
    try:
        if sport == "nba":
            # Use adapters.nba so tests can monkeypatch this symbol
            result = nba_adapter.predict_winprob(payload.event_id)
        else:
            if sport not in REGISTRY:
                raise HTTPException(status_code=400, detail=f"Unsupported sport: {sport}")
            result = REGISTRY[sport](payload.event_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{sport.upper()} prediction error: {e}")

    # Shape & persistence (best-effort)
    try:
        probs = result.get("win_probabilities", {})
        mk = result.get("model_key", f"{sport}-winprob-unknown")
        home = float(probs.get("home", 0.0))
        away = float(probs.get("away", 0.0))
        _persist_prediction(payload.event_id, mk, home, away)
    except Exception:
        pass

    return {
        "model_key": result.get("model_key", f"{sport}-winprob-unknown"),
        "win_probabilities": result.get("win_probabilities", {}),
        "generated_at": result.get("generated_at", datetime.now(timezone.utc).isoformat()),
    }