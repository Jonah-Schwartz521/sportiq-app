#!/usr/bin/env python3
"""
Fetch moneyline and spread odds from The Odds API for NBA and NFL.

Optimizations for free tier (500 requests/month):
- Single endpoint per sport (not per-game)
- Only upcoming games in next 24 hours
- Limited bookmaker set (3-5 books)
- Only h2h (moneyline) and spreads markets

Security:
- API key must be in ODDS_API_KEY environment variable

Output:
- model/data/processed/odds/nba_odds.parquet
- model/data/processed/odds/nfl_odds.parquet

Schema:
sport, commence_time_utc, home_team, away_team, bookmaker, market,
outcome_name, outcome_price, point, last_update_utc, source
"""

import os
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import List, Dict, Any, Optional

import pandas as pd
import requests

# Configuration
API_BASE_URL = "https://api.the-odds-api.com/v4"
ODDS_FORMAT = "american"
DATE_FORMAT = "iso"

# Sport configuration (The Odds API sport keys)
SPORTS = {
    "nba": "basketball_nba",
    "nfl": "americanfootball_nfl",
}

# Markets to fetch (minimize API usage)
MARKETS = ["h2h", "spreads"]

# Bookmakers to include (limit to 5 popular US books to reduce payload)
# Using common books with good coverage
BOOKMAKERS = [
    "fanduel",
    "draftkings",
    "betmgm",
    "pointsbetus",
    "bovada",
]

# Time window for upcoming games (24 hours = safer for free tier)
HOURS_AHEAD = 24

# Output paths
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "odds"
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def get_api_key() -> str:
    """Get API key from environment variable."""
    api_key = os.getenv("ODDS_API_KEY")
    if not api_key:
        print("❌ ERROR: ODDS_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)
    return api_key


def fetch_odds_for_sport(
    sport_key: str,
    api_key: str,
) -> List[Dict[str, Any]]:
    """
    Fetch odds for a single sport using the most efficient endpoint.

    Uses /v4/sports/{sport}/odds which returns all games with odds in one call.

    Args:
        sport_key: The Odds API sport key (e.g., 'basketball_nba')
        api_key: The Odds API key

    Returns:
        List of game data with odds
    """
    # Calculate time window
    now = datetime.now(timezone.utc)
    commence_time_from = now.isoformat()
    commence_time_to = (now + timedelta(hours=HOURS_AHEAD)).isoformat()

    url = f"{API_BASE_URL}/sports/{sport_key}/odds"

    params = {
        "apiKey": api_key,
        "regions": "us",  # US bookmakers only
        "markets": ",".join(MARKETS),
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": DATE_FORMAT,
        "bookmakers": ",".join(BOOKMAKERS),
        "commenceTimeFrom": commence_time_from,
        "commenceTimeTo": commence_time_to,
    }

    print(f"📡 Fetching {sport_key} odds...")
    print(f"   Time window: next {HOURS_AHEAD} hours")
    print(f"   Markets: {', '.join(MARKETS)}")
    print(f"   Bookmakers: {', '.join(BOOKMAKERS)}")

    try:
        response = requests.get(url, params=params, timeout=30)
        response.raise_for_status()

        data = response.json()

        # Check remaining API calls (returned in headers)
        remaining = response.headers.get("x-requests-remaining", "unknown")
        used = response.headers.get("x-requests-used", "unknown")

        print(f"✓ Fetched {len(data)} games")
        print(f"   API quota: {used} used, {remaining} remaining")

        return data

    except requests.exceptions.RequestException as e:
        print(f"❌ Error fetching odds for {sport_key}: {e}", file=sys.stderr)
        return []


def normalize_odds_data(
    games: List[Dict[str, Any]],
    sport_name: str,
) -> pd.DataFrame:
    """
    Normalize raw API response into flat dataframe with standard schema.

    Args:
        games: List of game data from API
        sport_name: Short sport name (e.g., 'nba', 'nfl')

    Returns:
        DataFrame with normalized odds data
    """
    rows = []

    for game in games:
        commence_time = game.get("commence_time")
        home_team = game.get("home_team")
        away_team = game.get("away_team")
        last_update = game.get("last_update")

        # Process each bookmaker
        for bookmaker_data in game.get("bookmakers", []):
            bookmaker = bookmaker_data.get("key")

            # Process each market (h2h, spreads)
            for market_data in bookmaker_data.get("markets", []):
                market = market_data.get("key")

                # Process each outcome
                for outcome in market_data.get("outcomes", []):
                    outcome_name = outcome.get("name")
                    outcome_price = outcome.get("price")
                    point = outcome.get("point")  # Will be None for h2h, float for spreads

                    rows.append({
                        "sport": sport_name,
                        "commence_time_utc": commence_time,
                        "home_team": home_team,
                        "away_team": away_team,
                        "bookmaker": bookmaker,
                        "market": market,
                        "outcome_name": outcome_name,
                        "outcome_price": outcome_price,
                        "point": point,
                        "last_update_utc": last_update,
                        "source": "the-odds-api",
                    })

    df = pd.DataFrame(rows)

    # Ensure proper data types
    if not df.empty:
        df["commence_time_utc"] = pd.to_datetime(df["commence_time_utc"])
        df["last_update_utc"] = pd.to_datetime(df["last_update_utc"])
        df["outcome_price"] = df["outcome_price"].astype(float)
        df["point"] = df["point"].astype("Float64")  # Nullable float for spreads

    return df


def upsert_odds_data(
    new_df: pd.DataFrame,
    output_path: Path,
) -> bool:
    """
    Upsert new odds data into existing parquet file.

    Idempotent upsert logic:
    - Composite key: (commence_time_utc, home_team, away_team, bookmaker, market, outcome_name, point)
    - Keep row with most recent last_update_utc

    Args:
        new_df: New odds data
        output_path: Path to parquet file

    Returns:
        True if data changed, False if identical
    """
    if new_df.empty:
        print(f"⚠️  No new odds data to write")
        return False

    # Key columns for upsert
    key_cols = [
        "commence_time_utc",
        "home_team",
        "away_team",
        "bookmaker",
        "market",
        "outcome_name",
        "point",
    ]

    # Load existing data if file exists
    if output_path.exists():
        print(f"📂 Loading existing data from {output_path.name}")
        existing_df = pd.read_parquet(output_path)

        # Combine existing + new data
        combined_df = pd.concat([existing_df, new_df], ignore_index=True)

        # Sort by last_update_utc (descending) and drop duplicates keeping first (most recent)
        combined_df = combined_df.sort_values("last_update_utc", ascending=False)
        combined_df = combined_df.drop_duplicates(subset=key_cols, keep="first")

        # Check if data actually changed
        existing_sorted = existing_df.sort_values(key_cols).reset_index(drop=True)
        combined_sorted = combined_df.sort_values(key_cols).reset_index(drop=True)

        if existing_sorted.equals(combined_sorted):
            print(f"   No changes detected (data identical)")
            return False

        print(f"   Merged: {len(existing_df)} existing + {len(new_df)} new = {len(combined_df)} total rows")
        df_to_write = combined_df

    else:
        print(f"📝 Creating new file: {output_path.name}")
        df_to_write = new_df

    # Write to parquet
    df_to_write.to_parquet(
        output_path,
        index=False,
        compression="snappy",
        engine="pyarrow",
    )

    print(f"✓ Wrote {len(df_to_write)} rows to {output_path.name}")
    return True


def main():
    """Main execution function."""
    print("=" * 60)
    print("The Odds API - Odds Refresh")
    print("=" * 60)
    print()

    # Get API key
    api_key = get_api_key()
    print(f"✓ API key loaded from environment")
    print()

    any_changes = False

    # Process each sport
    for sport_name, sport_key in SPORTS.items():
        print(f"{'=' * 60}")
        print(f"Processing {sport_name.upper()}")
        print(f"{'=' * 60}")

        # Fetch odds
        games = fetch_odds_for_sport(sport_key, api_key)

        if not games:
            print(f"⚠️  No upcoming games found for {sport_name} in next {HOURS_AHEAD} hours")
            print()
            continue

        # Normalize data
        df = normalize_odds_data(games, sport_name)
        print(f"📊 Normalized {len(df)} odds records")

        # Upsert to parquet
        output_path = OUTPUT_DIR / f"{sport_name}_odds.parquet"
        changed = upsert_odds_data(df, output_path)

        if changed:
            any_changes = True

        print()

    # Summary
    print("=" * 60)
    if any_changes:
        print("✓ Odds refresh complete - changes detected")
    else:
        print("ℹ️  Odds refresh complete - no changes")
    print("=" * 60)

    return 0 if any_changes else 1


if __name__ == "__main__":
    sys.exit(main())
