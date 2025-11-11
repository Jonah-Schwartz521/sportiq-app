# apps/api/app/routers/predict.py

from datetime import datetime, timezone
from typing import Optional, Literal, Dict

import psycopg
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.registry import REGISTRY  # sport -> callable(event_id) -> dict
from apps.api.app.schemas.predictions import PredictResponse

router = APIRouter(prefix="/predict", tags=["predict"])


class PredictRequest(BaseModel):
    """
    Generic prediction request body.

    For current contract:
    - Team sports (nba/mlb/nfl/nhl/ufc by event): require event_id.
    - UFC special-case: can use fighter_a/fighter_b without event_id.
    """
    event_id: Optional[int] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    fighter_a: Optional[str] = None
    fighter_b: Optional[str] = None


def _persist_prediction(event_id: int, model_key: str, home_wp: float, away_wp: float) -> None:
    """
    Best-effort persistence into core.predictions.

    Never raises: failures here must not break the API contract.
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
        # TODO: add logging later
        pass


@router.post("/{sport}", response_model=PredictResponse, summary="Predict win probabilities")
def predict(
    sport: Literal["nba", "mlb", "nfl", "nhl", "ufc"],
    payload: PredictRequest,
):
    """
    Team sports contract:

      Request:
        { "event_id": int }

      Response:
        {
          "model_key": "xxx-winprob-<ver>",
          "win_probabilities": { "home": float, "away": float },
          "generated_at": "ISO-8601"
        }

    UFC special-case (temporary):

      If `fighter_a` and `fighter_b` are provided, we reply with a stub prediction
      without requiring `event_id`. This is only for exercising the contract.
    """

    # --- UFC special-case: fighters only, no event_id required ---
    if sport == "ufc" and payload.fighter_a and payload.fighter_b:
        return {
            "model_key": "ufc-winprob-0.1.0",
            "win_probabilities": {
                "fighter_a": 0.55,
                "fighter_b": 0.45,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # --- Common validation for registry-backed predictors ---
    if payload.event_id is None:
        raise HTTPException(status_code=400, detail="Missing event_id.")

    if sport not in REGISTRY:
        raise HTTPException(status_code=400, detail=f"Unsupported sport: {sport}")

    # --- Call underlying predictor ---
    try:
        if sport == "nba":
            # Use adapters.nba directly so tests can monkeypatch cleanly
            from apps.api.app.adapters import nba as nba_adapter

            result = nba_adapter.predict_winprob(payload.event_id)
        else:
            result = REGISTRY[sport](payload.event_id)
    except HTTPException:
        # Allow explicit HTTP errors through
        raise
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"{sport.upper()} prediction error: {e}",
        )

    # --- Best-effort persistence for home/away style predictions ---
    try:
        probs: Dict[str, float] = result.get("win_probabilities") or {}
        home = float(probs.get("home", 0.0))
        away = float(probs.get("away", 0.0))
        _persist_prediction(
            payload.event_id,
            result.get("model_key", f"{sport}-winprob-unknown"),
            home,
            away,
        )
    except Exception:
        # Ignore persistence errors
        pass

    # --- Normalize response shape ---
    return {
        "model_key": result.get("model_key", f"{sport}-winprob-unknown"),
        "win_probabilities": result.get("win_probabilities", {}),
        "generated_at": result.get("generated_at", datetime.now(timezone.utc).isoformat()),
    }