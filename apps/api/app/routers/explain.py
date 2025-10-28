# apps/api/app/routers/explain.py
from fastapi import APIRouter, HTTPException, Query
from typing import Literal, List, Dict, Any
from datetime import datetime, timezone
import logging
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/explain", tags=["explain"])
logger = logging.getLogger(__name__)

def _fake_reasons_for(sport: str, event_id: int) -> List[Dict[str, Any]]:
    if sport == "nba":
        return [
            {"rank": 1, "feature_name": "Rest Advantage", "direction": "pro-home", "abs_impact": 0.07,
             "user_friendly": "Home team had extra rest days vs opponent."},
            {"rank": 2, "feature_name": "Rebound Margin (L5)", "direction": "pro-home", "abs_impact": 0.05,
             "user_friendly": "Recent games show stronger rebounding trend."},
            {"rank": 3, "feature_name": "3PT Attempt Rate", "direction": "pro-away", "abs_impact": 0.03,
             "user_friendly": "Away team’s 3PT volume could narrow the gap."},
        ]
    elif sport == "ufc":
        return [
            {"rank": 1, "feature_name": "Reach Advantage", "direction": "pro-fighter_a", "abs_impact": 0.06,
             "user_friendly": "Fighter A has a longer reach, aiding distance control."},
            {"rank": 2, "feature_name": "Age Delta", "direction": "pro-fighter_a", "abs_impact": 0.04,
             "user_friendly": "Prime-age advantage based on historical outcomes."},
            {"rank": 3, "feature_name": "Recent Form (L3)", "direction": "pro-fighter_b", "abs_impact": 0.03,
             "user_friendly": "Fighter B’s recent performances add uncertainty."},
        ]
    raise HTTPException(status_code=400, detail="Unsupported sport")

@router.get("/{sport}")
def explain(
    sport: Literal["nba", "ufc"],
    event_id: int = Query(..., description="ID from core.events"),
):
    # 1) Get latest pred_id for this event & sport
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT p.pred_id, p.model_key, p.home_wp, p.away_wp, p.created_at
                FROM core.predictions p
                WHERE p.event_id = %s AND p.model_key LIKE %s
                ORDER BY p.pred_id DESC
                LIMIT 1;
                """,
                (event_id, f"{sport}-%",),
            )
            row = cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error reading predictions: {e}")

    if not row:
        raise HTTPException(status_code=404, detail="No prediction found for this event/sport. Call /predict first.")

    pred_id, model_key, home_wp, away_wp, created_at = row

    # 2) Generate reasons (stubbed for now)
    reasons = _fake_reasons_for(sport, event_id)

    # 3) Persist reasons for that pred_id (idempotent for this pred_id)
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            # delete any previous explanations for this prediction
            cur.execute("DELETE FROM core.explanations WHERE pred_id = %s;", (pred_id,))
            # insert the three reasons using VALUES
            cur.execute(
                """
                INSERT INTO core.explanations (pred_id, rank, feature_name, shap_value, contribution_text)
                VALUES
                    (%s, %s, %s, %s, %s),
                    (%s, %s, %s, %s, %s),
                    (%s, %s, %s, %s, %s)
                """,
                (
                    pred_id, reasons[0]["rank"], reasons[0]["feature_name"], reasons[0]["abs_impact"], reasons[0]["user_friendly"],
                    pred_id, reasons[1]["rank"], reasons[1]["feature_name"], reasons[1]["abs_impact"], reasons[1]["user_friendly"],
                    pred_id, reasons[2]["rank"], reasons[2]["feature_name"], reasons[2]["abs_impact"], reasons[2]["user_friendly"],
                ),
            )
            conn.commit()
    except Exception as e:
        logger.warning(f"[Explain] failed to persist reasons for pred_id={pred_id}: {e}")

    return {
        "event_id": event_id,
        "sport": sport,
        "pred_id": pred_id,
        "model_key": model_key,
        "win_probabilities": {"home": float(home_wp), "away": float(away_wp)},
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_reasons": reasons,
    }