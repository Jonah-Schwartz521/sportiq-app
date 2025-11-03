# apps/api/app/routers/predict.py
# Purpose:
#   Thin prediction endpoint that:
#     1) validates input (event_id required),
#     2) dispatches to a sport-specific predictor via REGISTRY,
#     3) persists a summary row to core.predictions,
#     4) returns a small, client-friendly JSON payload.

from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timezone

import psycopg
from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.registry import REGISTRY  # maps 'nba' | 'mlb' | 'nfl' | 'nhl' | 'ufc' -> callable(event_id)->dict

# Router is mounted at /predict
router = APIRouter(prefix="/predict", tags=["predict"])


# --------- Schemas (keep I/O tight and explicit) ---------
class PredictRequest(BaseModel):
    # For now we use event_id as the required input for all sports you support.
    # (You can extend with sport-specific optional fields later.)
    event_id: Optional[int] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    fighter_a: Optional[str] = None
    fighter_b: Optional[str] = None


class PredictResponse(BaseModel):
    # Silence pydantic "model_*" reserved name warning
    model_config = ConfigDict(protected_namespaces=())
    # Returned by service layer (e.g., "nba-winprob-0.3.0")
    model_key: str
    # Probability map — for team sports, typically {"home": p, "away": 1-p}
    win_probabilities: Dict[str, float]
    # ISO-8601 timestamp when this prediction was created
    generated_at: str


# --------- DB persistence helper ---------
def _persist_prediction(event_id: int, model_key: str, home_wp: float, away_wp: float) -> None:
    """
    Insert a summary prediction row for history/UX.
    Intentionally 'best-effort': failures here should not 500 the request.
    """
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
        # Keep API responsive; add logging later if you want.
        pass


# --------- Route ---------
@router.post("/{sport}", response_model=PredictResponse, summary="Predict")
def predict(
    sport: Literal["nba", "mlb", "nfl", "nhl", "ufc"],
    payload: PredictRequest,
):
    """
    Entry point for predictions across sports.

    Contract with REGISTRY[sport]:
      - Callable receives: event_id (int)
      - Returns dict like:
          {
            "model_key": "nba-winprob-0.3.0",
            "win_probabilities": {"home": 0.58, "away": 0.42},
            "generated_at": "2025-11-02T20:15:00.000000+00:00"
          }
    """

    # 1) Validate
    if payload.event_id is None:
        raise HTTPException(status_code=400, detail="Missing event_id.")
    if sport not in REGISTRY:
        raise HTTPException(status_code=400, detail=f"Unsupported sport: {sport}")

    # 2) Dispatch to the sport-specific predictor
    try:
        result = REGISTRY[sport](payload.event_id)
    except HTTPException:
        # If a service already raised an HTTPException, bubble it up as-is
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{sport.upper()} prediction error: {e}")

    # 3) Basic shape check + persistence
    try:
        probs = result["win_probabilities"]
        mk = result["model_key"]
        # Normalize common keys for team sports; if your UFC service returns fighter_a/b,
        # you can map them into home/away here or adapt the persistence for UFC separately.
        home = float(probs.get("home", 0.0))
        away = float(probs.get("away", 0.0))
        _persist_prediction(payload.event_id, mk, home, away)
    except Exception as e:
        # Do not fail the whole request if persistence stumbles; return the prediction.
        # If it's a real shape error, it'll be caught by response_model validation anyway.
        pass

    # 4) Return minimal, client-friendly payload
    return {
        "model_key": result.get("model_key", f"{sport}-winprob-unknown"),
        "win_probabilities": result.get("win_probabilities", {}),
        "generated_at": result.get("generated_at", datetime.now(timezone.utc).isoformat()),
    }