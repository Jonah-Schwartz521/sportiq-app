# apps/api/app/routers/insights.py
from fastapi import APIRouter, HTTPException
from typing import Literal, List, Dict, Any
from datetime import datetime, timezone
import logging
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/insights", tags=["insights"])
logger = logging.getLogger(__name__)

SportLiteral = Literal["nba", "mlb", "nfl", "nhl", "ufc"]


def _nba_insights(event_id: int) -> List[Dict[str, Any]]:
    # Simple deterministic stub data, shape must be {type,label,detail}
    return [
        {
            "type": "feature_importance",
            "label": "Rest advantage",
            "detail": "Home team has more rest days than the opponent.",
        },
        {
            "type": "feature_importance",
            "label": "Offensive efficiency",
            "detail": "Home team has a higher offensive rating over the last 10 games.",
        },
        {
            "type": "context",
            "label": "Market alignment",
            "detail": "Model is slightly more bullish on the favorite than the market line.",
        },
    ]


def _ufc_insights(event_id: int) -> List[Dict[str, Any]]:
    return [
        {
            "type": "feature_importance",
            "label": "Reach advantage",
            "detail": "Fighter A holds a meaningful reach advantage.",
        },
        {
            "type": "feature_importance",
            "label": "Grappling edge",
            "detail": "Fighter B shows stronger recent grappling metrics.",
        },
        {
            "type": "context",
            "label": "Finishing threat",
            "detail": "Both fighters have high finish rates, increasing volatility.",
        },
    ]


def _generic_insights(sport: SportLiteral, event_id: int) -> List[Dict[str, Any]]:
    """
    Generic stub for MLB / NFL / NHL so /insights works for all sports.
    Contract is the same: list of {type, label, detail}.
    """
    if sport == "mlb":
        prefix = "Matchup context"
    elif sport in ("nfl", "nhl"):
        prefix = "Game context"
    else:
        prefix = "Context"

    return [
        {
            "type": "feature_importance",
            "label": "Recent form",
            "detail": f"{prefix}: recent performance favors the home side.",
        },
        {
            "type": "feature_importance",
            "label": "Schedule and rest",
            "detail": f"{prefix}: rest and travel slightly advantage the home side.",
        },
        {
            "type": "context",
            "label": "Matchup volatility",
            "detail": f"{prefix}: matchup stats add some uncertainty to the projection.",
        },
    ]


def _insights_for(sport: SportLiteral, event_id: int) -> List[Dict[str, Any]]:
    """
    Dispatch to sport-specific or generic insight generators.
    """
    if sport == "nba":
        return _nba_insights(event_id)
    if sport == "ufc":
        return _ufc_insights(event_id)
    # mlb / nfl / nhl
    return _generic_insights(sport, event_id)


def _best_effort_persist(
    sport: str,
    event_id: int,
    model_key: str,
    insights: List[Dict[str, Any]],
) -> None:
    """
    Optional: write a stub prediction + insights to the DB.
    This MUST NOT break the endpoint; all errors are swallowed after logging.
    """
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            # Ensure a prediction row exists
            cur.execute(
                """
                INSERT INTO core.predictions (event_id, model_key, home_wp, away_wp)
                VALUES (%s, %s, 0.55, 0.45)
                ON CONFLICT DO NOTHING;
                """,
                (event_id, model_key),
            )

            # Remove previous explanations for this prediction
            cur.execute(
                """
                DELETE FROM core.explanations
                WHERE pred_id IN (
                    SELECT pred_id
                    FROM core.predictions
                    WHERE event_id = %s AND model_key = %s
                );
                """,
                (event_id, model_key),
            )

            # Insert each insight as an explanation row
            for rank, ins in enumerate(insights, start=1):
                cur.execute(
                    """
                    INSERT INTO core.explanations (
                        pred_id,
                        rank,
                        feature_name,
                        shap_value,
                        contribution_text
                    )
                    SELECT p.pred_id, %s, %s, %s, %s
                    FROM core.predictions p
                    WHERE p.event_id = %s AND p.model_key = %s;
                    """,
                    (
                        rank,
                        ins.get("label", ""),
                        0.0,
                        ins.get("detail", ""),
                        event_id,
                        model_key,
                    ),
                )

            conn.commit()
    except Exception as e:
        logger.warning(
            "[insights] failed to persist insights for %s/%s: %s",
            sport,
            event_id,
            e,
        )


@router.get("/{sport}/{event_id}", summary="Get model insights for an event")
def get_insights(
    sport: SportLiteral,
    event_id: int,
):
    """
    Stable contract (used by tests):

    200 OK:
    {
      "event_id": int,
      "sport": "nba" | "mlb" | "nfl" | "nhl" | "ufc",
      "model_key": "<sport>-winprob-<ver>",
      "generated_at": "<ISO-8601>",
      "insights": [
        { "type": str, "label": str, "detail": str },
        ...
      ]
    }

    400 if event_id is obviously invalid (e.g. <= 0).
    """

    if event_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid event_id")

    insights = _insights_for(sport, event_id)
    model_key = f"{sport}-winprob-0.1.0"

    # Optional persistence; never breaks the response
    _best_effort_persist(sport, event_id, model_key, insights)

    return {
        "event_id": event_id,
        "sport": sport,
        "model_key": model_key,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "insights": insights,
    }