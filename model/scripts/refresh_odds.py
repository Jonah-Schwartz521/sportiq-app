#!/usr/bin/env python3
"""
Fetch moneyline and spread odds from The Odds API for NBA, NFL, NHL, and UFC.

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
- model/data/processed/odds/nhl_odds.parquet
- model/data/processed/odds/ufc_odds.parquet

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
    "nhl": "icehockey_nhl",
    "ufc": "mma_mixed_martial_arts",
}

# Markets to fetch (minimize API usage); UFC will override to h2h-only
MARKETS = ["h2h", "spreads"]

# Preferred bookmakers (safe keys known to work with US major sports)
# If these cause 422, we'll retry without the filter and post-filter later
PREFERRED_BOOKMAKERS = [
    "fanduel",
    "draftkings",
    "betmgm",
    "caesars",
    "bet365",
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


def format_datetime_for_api(dt: datetime) -> str:
    """
    Format datetime to conservative ISO-8601 format accepted by The Odds API.

    Uses YYYY-MM-DDTHH:MM:SSZ format (no microseconds, UTC with Z suffix).

    Args:
        dt: datetime object in UTC

    Returns:
        ISO-8601 formatted string
    """
    # Remove microseconds and format with Z suffix
    return dt.replace(microsecond=0).strftime("%Y-%m-%dT%H:%M:%SZ")


def log_http_error(response: requests.Response) -> None:
    """Print status code and response body (JSON or text) for debugging."""
    status = response.status_code
    print(f"❌ HTTP {status} error: {response.url}", file=sys.stderr)

    try:
        body = response.json()
        print(f"   Error details: {body}", file=sys.stderr)
    except Exception:
        print(f"   Error body: {response.text}", file=sys.stderr)


def fetch_odds_for_sport(
    sport_key: str,
    api_key: str,
    use_bookmaker_filter: bool = True,
    markets: Optional[List[str]] = None,
) -> List[Dict[str, Any]]:
    """
    Fetch odds for a single sport using the most efficient endpoint.

    Uses /v4/sports/{sport}/odds which returns all games with odds in one call.

    Args:
        sport_key: The Odds API sport key (e.g., 'basketball_nba')
        api_key: The Odds API key
        use_bookmaker_filter: Whether to filter by preferred bookmakers

    Returns:
        List of game data with odds
    """
    # Calculate time window with conservative formatting
    now = datetime.now(timezone.utc)
    commence_time_from = format_datetime_for_api(now)
    commence_time_to = format_datetime_for_api(now + timedelta(hours=HOURS_AHEAD))

    url = f"{API_BASE_URL}/sports/{sport_key}/odds"

    target_markets = markets or MARKETS

    params = {
        "apiKey": api_key,
        "regions": "us",  # US bookmakers only
        "markets": ",".join(target_markets),
        "oddsFormat": ODDS_FORMAT,
        "dateFormat": DATE_FORMAT,
        "commenceTimeFrom": commence_time_from,
        "commenceTimeTo": commence_time_to,
    }

    # Optionally add bookmaker filter
    if use_bookmaker_filter:
        params["bookmakers"] = ",".join(PREFERRED_BOOKMAKERS)
        print(f"📡 Fetching {sport_key} odds (filtered to {len(PREFERRED_BOOKMAKERS)} bookmakers)...")
        print(f"   Bookmakers: {', '.join(PREFERRED_BOOKMAKERS)}")
    else:
        print(f"📡 Fetching {sport_key} odds (all bookmakers)...")

    print(f"   Time window: {commence_time_from} to {commence_time_to}")
    print(f"   Markets: {', '.join(target_markets)}")

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

    except requests.exceptions.HTTPError as e:
        if e.response is not None:
            log_http_error(e.response)
        else:
            print(f"❌ HTTP error for {sport_key}: {e}", file=sys.stderr)

        # If 422 and we used bookmaker filter, signal for retry
        if e.response is not None and e.response.status_code == 422 and use_bookmaker_filter:
            print(f"   → Will retry without bookmaker filter", file=sys.stderr)
            raise  # Re-raise to trigger retry logic

        return []

    except requests.exceptions.RequestException as e:
        print(f"❌ Request error for {sport_key}: {e}", file=sys.stderr)
        return []


def normalize_odds_data(
    games: List[Dict[str, Any]],
    sport_name: str,
    preferred_bookmakers: Optional[List[str]] = None,
    force_market: Optional[str] = None,
) -> pd.DataFrame:
    """
    Normalize raw API response into flat dataframe with standard schema.

    Args:
        games: List of game data from API
        sport_name: Short sport name (e.g., 'nba', 'nfl')
        preferred_bookmakers: Optional list to filter bookmakers in post-processing

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

            # Optional post-filter by preferred bookmakers
            if preferred_bookmakers and bookmaker not in preferred_bookmakers:
                continue

            # Process each market (h2h, spreads)
            for market_data in bookmaker_data.get("markets", []):
                market = force_market or market_data.get("key")

                # Process each outcome
                for outcome in market_data.get("outcomes", []):
                    outcome_name = outcome.get("name")
                    outcome_price = outcome.get("price")
                    point = (
                        None
                        if force_market == "h2h"
                        else outcome.get("point")
                    )  # Will be None for h2h, float for spreads

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

        games = []

        # UFC uses only h2h market
        target_markets = ["h2h"] if sport_name == "ufc" else MARKETS

        # Try with bookmaker filter first
        try:
            games = fetch_odds_for_sport(
                sport_key,
                api_key,
                use_bookmaker_filter=True,
                markets=target_markets,
            )
        except requests.exceptions.HTTPError as e:
            # If 422 error, retry without bookmaker filter
            if e.response.status_code == 422:
                print(f"⚠️  Retrying without bookmaker filter...")
                games = fetch_odds_for_sport(
                  sport_key,
                  api_key,
                  use_bookmaker_filter=False,
                  markets=target_markets,
                )
            else:
                # Other HTTP errors, don't retry
                games = []

        if not games:
            print(f"⚠️  No upcoming games found for {sport_name} in next {HOURS_AHEAD} hours")
            print()
            continue

        # Normalize data (with post-filter to preferred bookmakers if we got all)
        df = normalize_odds_data(
            games,
            sport_name,
            preferred_bookmakers=PREFERRED_BOOKMAKERS,
            force_market="h2h" if sport_name == "ufc" else None,
        )

        if df.empty:
            print(f"⚠️  No odds from preferred bookmakers")
            print()
            continue

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
