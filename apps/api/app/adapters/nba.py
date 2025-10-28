# apps/api/app/adapters/nba.py
from datetime import datetime, timezone
from typing import Dict, Any
import psycopg
from apps.api.app.adapters.base import SportAdapter
from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.services.nba_model import predict_winprob

class NBAAdapter(SportAdapter):
    sport_code = "nba"

    def validate(self, payload: Dict[str, Any]) -> None:
        if payload.get("event_id") is None:
            raise ValueError("Missing event_id for NBA prediction.")

    def predict(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        event_id = int(payload["event_id"])
        res = predict_winprob(event_id)
        return res  # already has model_key, win_probabilities, generated_at

    def persist(self, event_id: int, result: Dict[str, Any]) -> None:
        probs = result.get("win_probabilities", {})
        mk = result.get("model_key", "nba-winprob-unknown")
        try:
            with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO core.predictions (event_id, model_key, home_wp, away_wp)
                    VALUES (%s, %s, %s, %s)
                    ON CONFLICT DO NOTHING;
                    """,
                    (event_id, mk, float(probs.get("home", 0.0)), float(probs.get("away", 0.0))),
                )
                conn.commit()
        except Exception:
            pass

    # optional: call your existing explain service if you have it
    def explain(self, event_id: int) -> Dict[str, Any]:
        # If you already have /explain wired, you can leave this unimplemented for now.
        raise NotImplementedError