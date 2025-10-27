# apps/api/app/services/explain_logic.py
from __future__ import annotations

from datetime import datetime, timezone
from typing import List, Dict, Any, Literal
import logging

import psycopg
from apps.api.app.core.config import POSTGRES_DSN

logger = logging.getLogger(__name__)

ModelSport = Literal["nba", "ufc"]


def explain_event(sport: ModelSport, event_id: int) -> Dict[str, Any]:
    """
    Orchestrates generation + (best-effort) persistence of top reasons.
    Keeps routers thin; easy to unit test; swap in SHAP/LLM later.
    """
    if sport == "nba":
        reasons = _compute_reasons_nba(event_id)
        model_key = "nba-winprob-0.2.0"
    elif sport == "ufc":
        reasons = _compute_reasons_ufc(event_id)
        model_key = "ufc-winprob-0.1.0"
    else:
        raise ValueError("Unsupported sport")

    try:
        _persist_explanation(event_id=event_id, model_key=model_key, reasons=reasons)
    except Exception as e:
        logger.warning("[Explain] Persist skipped: %s", e)

    return {
        "event_id": event_id,
        "sport": sport,
        "model_key": model_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "top_reasons": reasons,
    }


def _compute_reasons_nba(event_id: int) -> List[Dict[str, Any]]:
    return [
        {"rank": 1, "feature_name": "Rest Advantage (days)", "direction": "pro-home",
         "abs_impact": 0.06, "user_friendly": "Home team appears to have more rest than the away team."},
        {"rank": 2, "feature_name": "Rebound Margin (season)", "direction": "pro-home",
         "abs_impact": 0.04, "user_friendly": "Home team’s rebound margin trends better this season."},
        {"rank": 3, "feature_name": "3PT Attempt Rate (season)", "direction": "pro-away",
         "abs_impact": 0.03, "user_friendly": "Away team’s three-point volume keeps them in games."},
    ]


def _compute_reasons_ufc(event_id: int) -> List[Dict[str, Any]]:
    return [
        {"rank": 1, "feature_name": "Reach Advantage", "direction": "pro-fighter_a",
         "abs_impact": 0.06, "user_friendly": "Fighter A’s longer reach can control distance exchanges."},
        {"rank": 2, "feature_name": "Age/Prime Window", "direction": "pro-fighter_a",
         "abs_impact": 0.04, "user_friendly": "Fighter A is closer to typical prime years."},
        {"rank": 3, "feature_name": "Recent Form (last 3)", "direction": "pro-fighter_b",
         "abs_impact": 0.03, "user_friendly": "Fighter B’s recent performances add uncertainty."},
    ]


def _persist_explanation(*, event_id: int, model_key: str, reasons: List[Dict[str, Any]]) -> None:
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO core.predictions (event_id, model_key, home_wp, away_wp)
            VALUES (%s, %s, 0.60, 0.40)
            ON CONFLICT DO NOTHING;
            """,
            (event_id, model_key),
        )
        cur.execute(
            """
            DELETE FROM core.explanations
            WHERE pred_id IN (
                SELECT pred_id FROM core.predictions
                WHERE event_id = %s AND model_key = %s
            );
            """,
            (event_id, model_key),
        )
        top3 = (reasons + [{}, {}, {}])[:3]
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
                top3[0].get("rank", 1), top3[0].get("feature_name", "n/a"), top3[0].get("abs_impact", 0.0), top3[0].get("user_friendly", "n/a"),
                top3[1].get("rank", 2), top3[1].get("feature_name", "n/a"), top3[1].get("abs_impact", 0.0), top3[1].get("user_friendly", "n/a"),
                top3[2].get("rank", 3), top3[2].get("feature_name", "n/a"), top3[2].get("abs_impact", 0.0), top3[2].get("user_friendly", "n/a"),
                event_id, model_key,
            ),
        )
        conn.commit()