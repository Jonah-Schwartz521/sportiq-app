# apps/api/app/routers/odds.py
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List
from pathlib import Path
import pandas as pd

from apps.api.app.schemas.odds import OddsList, OddsForEvent, OddsRecord

router = APIRouter(prefix="/odds", tags=["odds"])

# Path to odds parquet files
ODDS_DIR = Path(__file__).parent.parent.parent.parent.parent / "model" / "data" / "processed" / "odds"


def _load_odds_data(sport: Optional[str] = None) -> pd.DataFrame:
    """Load odds data from parquet files."""
    dfs = []

    # Sports to load
    sports_to_load = [sport] if sport else ["nba", "nfl"]

    for sport_name in sports_to_load:
        file_path = ODDS_DIR / f"{sport_name}_odds.parquet"
        if file_path.exists():
            try:
                df = pd.read_parquet(file_path)
                dfs.append(df)
            except Exception as e:
                print(f"Warning: Could not load {file_path}: {e}")

    if not dfs:
        return pd.DataFrame()

    combined = pd.concat(dfs, ignore_index=True)
    return combined


@router.get("", summary="List all odds", response_model=OddsList)
def list_odds(
    sport: Optional[str] = Query(None, description="Filter by sport (nba, nfl)"),
    home_team: Optional[str] = Query(None, description="Filter by home team"),
    away_team: Optional[str] = Query(None, description="Filter by away team"),
    market: Optional[str] = Query(None, description="Filter by market (h2h, spreads)"),
    bookmaker: Optional[str] = Query(None, description="Filter by bookmaker"),
    limit: int = Query(100, ge=1, le=1000),
):
    """
    Get odds data with optional filters.

    Returns recent odds from The Odds API parquet files.
    """
    try:
        df = _load_odds_data(sport)

        if df.empty:
            return {"items": [], "total_returned": 0}

        # Apply filters
        if home_team:
            df = df[df["home_team"] == home_team]
        if away_team:
            df = df[df["away_team"] == away_team]
        if market:
            df = df[df["market"] == market]
        if bookmaker:
            df = df[df["bookmaker"] == bookmaker]

        # Sort by most recent first
        df = df.sort_values("last_update_utc", ascending=False)

        # Apply limit
        df = df.head(limit)

        # Convert to records
        items = df.to_dict("records")

        return {"items": items, "total_returned": len(items)}

    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading odds: {str(e)}")


@router.get("/event", summary="Get odds for a specific game", response_model=OddsForEvent)
def get_odds_for_event(
    home_team: str = Query(..., description="Home team name"),
    away_team: str = Query(..., description="Away team name"),
    sport: Optional[str] = Query(None, description="Sport (nba, nfl) - improves performance"),
):
    """
    Get all odds (moneyline + spreads) for a specific matchup.

    Returns data grouped by market type.
    """
    try:
        df = _load_odds_data(sport)

        if df.empty:
            raise HTTPException(status_code=404, detail="No odds data available")

        # Filter to specific matchup
        game_df = df[
            (df["home_team"] == home_team) &
            (df["away_team"] == away_team)
        ]

        if game_df.empty:
            raise HTTPException(status_code=404, detail="No odds found for this matchup")

        # Get commence time (should be same for all rows)
        commence_time = game_df["commence_time_utc"].iloc[0]

        # Split by market
        moneyline_df = game_df[game_df["market"] == "h2h"]
        spreads_df = game_df[game_df["market"] == "spreads"]

        # Convert to records
        moneyline_records = moneyline_df.to_dict("records")
        spreads_records = spreads_df.to_dict("records")

        return {
            "home_team": home_team,
            "away_team": away_team,
            "commence_time_utc": commence_time,
            "moneyline": moneyline_records,
            "spreads": spreads_records,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error loading odds: {str(e)}")
