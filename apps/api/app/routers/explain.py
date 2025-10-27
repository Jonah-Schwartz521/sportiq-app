# apps/api/app/routers/explain.py
from fastapi import APIRouter, HTTPException, Query
from typing import Literal, List, Dict, Any
from datetime import datetime, timezone
import logging
import psycopg

# NOTE: import path assumes you run uvicorn with "apps.api.app.main:app"
from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/explain", tags=["explain"])
logger = logging.getLogger(__name__)


def _fake_reasons_for(sport: str, event_id: int) -> List[Dict[str, Any]]:
    if sport == "nba":
        return [
            {
                "rank": 1,
                "feature_name": "Rest Advantage",
                "direction": "pro-home",
                "abs_impact": 0.07,
                "user_friendly": "Home team had extra rest days vs opponent.",
            },
            {
                "rank": 2,
                "feature_name": "Rebound Margin (L5)",
                "direction": "pro-home",
                "abs_impact": 0.05,
                "user_friendly": "Recent games show stronger rebounding trend.",
            },
            {
                "rank": 3,
                "feature_name": "3PT Attempt Rate",
                "direction": "pro-away",
                "abs_impact": 0.03,
                "user_friendly": "Away team’s 3PT volume could narrow the gap.",
            },
        ]
    elif sport == "ufc":
        return [
            {
                "rank": 1,
                "feature_name": "Reach Advantage",
                "direction": "pro-fighter_a",
                "abs_impact": 0.06,
                "user_friendly": "Fighter A has a longer reach, aiding distance control.",
            },
            {
                "rank": 2,
                "feature_name": "Age Delta",
                "direction": "pro-fighter_a",
                "abs_impact": 0.04,
                "user_friendly": "Prime-age advantage based on historical outcomes.",
            },
            {
                "rank": 3,
                "feature_name": "Recent Form (L3)",
                "direction": "pro-fighter_b",
                "abs_impact": 0.03,
                "user_friendly": "Fighter B’s recent performances add uncertainty.",
            },
        ]
    raise HTTPException(status_code=400, detail="Unsupported sport")


@router.get("/{sport}")
def explain(
    sport: Literal["nba", "ufc"],
    event_id: int = Query(..., description="ID from core.events"),
):
    reasons = _fake_reasons_for(sport, event_id)

    # Best-effort: persist a mock prediction + the top-3 reasons to the DB
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            # Upsert a demo prediction row (if missing)
            cur.execute(
                """
                INSERT INTO core.predictions (event_id, model_key, home_wp, away_wp)
                VALUES (%s, %s, 0.60, 0.40)
                ON CONFLICT DO NOTHING;
                """,
                (event_id, f"{sport}-winprob-0.1.0"),
            )

            # Remove previous explanations for this prediction
            cur.execute(
                """
                DELETE FROM core.explanations
                WHERE pred_id IN (
                    SELECT pred_id FROM core.predictions
                    WHERE event_id = %s AND model_key = %s
                );
                """,
                (event_id, f"{sport}-winprob-0.1.0"),
            )

            # Insert the Top-3 reasons (portable LATERAL VALUES form)
            cur.execute(
                """
                INSERT INTO core.explanations (pred_id, rank, feature_name, shap_value, contribution_text)
                SELECT p.pred_id, x.rank, x.feature_name, x.shap_value, x.contribution_text
                FROM core.predictions p,
                     LATERAL (
                        VALUES
                            (%s, %s, %s, %s),
                            (%s, %s, %s, %s),
                            (%s, %s, %s, %s)
                     ) AS x(rank, feature_name, shap_value, contribution_text)
                WHERE p.event_id = %s AND p.model_key = %s;
                """,
                (
                    reasons[0]["rank"], reasons[0]["feature_name"], reasons[0]["abs_impact"], reasons[0]["user_friendly"],
                    reasons[1]["rank"], reasons[1]["feature_name"], reasons[1]["abs_impact"], reasons[1]["user_friendly"],
                    reasons[2]["rank"], reasons[2]["feature_name"], reasons[2]["abs_impact"], reasons[2]["user_friendly"],
                    event_id, f"{sport}-winprob-0.1.0",
                ),
            )
            conn.commit()
    except Exception as e:
        # Keep endpoint responsive; log for debugging
        logger.warning(f"[ExplainRouter] Failed to write explanation for {sport}-{event_id}: {e}")

    return {
        "event_id": event_id,
        "sport": sport,
        "model_key": f"{sport}-winprob-0.1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_reasons": reasons,
    }