from fastapi import APIRouter, HTTPException
from typing import Literal
from datetime import datetime, timezone

from apps.api.app.schemas.insights import InsightsResponse

router = APIRouter(prefix="/insights", tags=["insights"])

@router.get("/{sport}/{event_id}", response_model=InsightsResponse, summary="High-level game insights")
def get_insights(
    sport: Literal["nba", "mlb", "nfl", "nhl", "ufc"],
    event_id: int,
):
    # For now: deterministic stubbed insights so frontend has a stable contract.
    if event_id <= 0:
        raise HTTPException(status_code=400, detail="Invalid event_id")

    model_key = f"{sport}-winprob-0.1.0"

    # Toy but plausible insights
    base = [
        {
            "type": "angle",
            "label": "Win probability context",
            "detail": f"{sport.upper()} model {model_key} favors the home side, "
                      f"but variance factors keep this matchup live.",
        },
        {
            "type": "trend",
            "label": "Recent form",
            "detail": "Recent performance and schedule context suggest momentum on the favorite's side.",
        },
        {
            "type": "key_stat",
            "label": "Key stat to watch",
            "detail": "Efficiency in late-game possessions is projected to drive most of the edge.",
        },
    ]

    # Make UFC a bit different
    if sport == "ufc":
        base[0]["detail"] = (
            "Stylistic matchup and historical finishes shape the implied edge in this fight."
        )

    return {
        "event_id": event_id,
        "sport": sport,
        "model_key": model_key,
        "insights": base,
    }