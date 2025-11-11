from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from datetime import datetime, timezone
import psycopg

from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.registry import REGISTRY

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
        # swallow; persistence is best-effort
        pass


@router.post("/{sport}", response_model=PredictResponse, summary="Predict win probabilities")
def predict(
    sport: Literal["nba", "mlb", "nfl", "nhl", "ufc"],
    payload: PredictRequest,
):
    # UFC special path: fighters only, no event_id
    if sport == "ufc" and payload.fighter_a and payload.fighter_b:
        return {
            "model_key": "ufc-winprob-0.1.0",
            "win_probabilities": {
                "fighter_a": 0.55,
                "fighter_b": 0.45,
            },
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    # All other paths require event_id
    if payload.event_id is None:
        raise HTTPException(status_code=400, detail="Missing event_id.")

    if sport not in REGISTRY:
        raise HTTPException(status_code=400, detail=f"Unsupported sport: {sport}")

    # Call underlying predictor
    try:
        if sport == "nba":
            from apps.api.app.adapters import nba as nba_adapter
            result = nba_adapter.predict_winprob(payload.event_id)
        else:
            result = REGISTRY[sport](payload.event_id)
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"{sport.upper()} prediction error: {e}")

    # Best-effort persistence
    try:
        probs = result.get("win_probabilities", {}) or {}
        home = float(probs.get("home", 0.0))
        away = float(probs.get("away", 0.0))
        _persist_prediction(
            payload.event_id,
            result.get("model_key", f"{sport}-winprob-unknown"),
            home,
            away,
        )
    except Exception:
        pass

    # Normalize response
    return {
        "model_key": result.get("model_key", f"{sport}-winprob-unknown"),
        "win_probabilities": result.get("win_probabilities", {}),
        "generated_at": result.get("generated_at", datetime.now(timezone.utc).isoformat()),
    }