# apps/api/app/adapters/mlb.py
from datetime import datetime, timezone
from typing import Dict, Any
import psycopg
from apps.api.app.adapters.base import SportAdapter
from apps.api.app.core.config import POSTGRES_DSN

class MLBAdapter(SportAdapter):
    sport_code = "mlb"

    def validate(self, payload: Dict[str, Any]) -> None:
        if payload.get("event_id") is None:
            raise ValueError("Missing event_id for MLB prediction.")

    def predict(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        # Simple stub
        probs = {"home": 0.52, "away": 0.48}
        return {
            "model_key": "mlb-winprob-0.1.0",
            "win_probabilities": probs,
            "generated_at": datetime.now(timezone.utc).isoformat(),
        }

    def persist(self, event_id: int, result: Dict[str, Any]) -> None:
        probs = result.get("win_probabilities", {})
        try:
            with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.predictions (event_id, model_key, home_wp, away_wp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (event_id, result["model_key"], float(probs.get("home", 0.0)), float(probs.get("away", 0.0))),
                )
                conn.commit()
        except Exception:
            pass