#!/usr/bin/env python3
"""
Nightly NHL scores refresh using NHL Web API.

Fetches recent games (yesterday + today) from NHL public API and upserts
into local parquet storage. Designed to run via GitHub Actions nightly.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo
import time

import pandas as pd
import requests

# Configuration
API_BASE = "https://api-web.nhle.com/v1"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "nhl"
OUTPUT_FILE = OUTPUT_DIR / "nhl_games_with_scores.parquet"
DENVER_TZ = ZoneInfo("America/Denver")
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_date_window() -> tuple[str, str]:
    """
    Calculate yesterday and today in America/Denver timezone.

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    now_denver = datetime.now(DENVER_TZ)
    today = now_denver.date()
    yesterday = today - timedelta(days=1)

    return yesterday.isoformat(), today.isoformat()


def infer_season(game_date: str) -> int:
    """
    Infer NHL season year from game date.

    NHL season runs October (year N) through June (year N+1).
    Season year is the year the season started.

    Args:
        game_date: Game date in YYYY-MM-DD format

    Returns:
        Season year (e.g., 2024 for 2024-25 season)
    """
    date = datetime.strptime(game_date, "%Y-%m-%d")
    year = date.year
    month = date.month

    # Oct-Dec: current year is season year
    # Jan-Jun: previous year is season year
    # Jul-Sep: off-season, use previous year (shouldn't have games)
    if month >= 10:
        return year
    elif month <= 6:
        return year - 1
    else:
        return year - 1


def fetch_scores_for_date(date: str) -> dict:
    """
    Fetch NHL scores for a specific date.

    Args:
        date: Date in YYYY-MM-DD format

    Returns:
        API response JSON dict
    """
    url = f"{API_BASE}/score/{date}"

    print(f"Fetching NHL scores for {date}...")

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
            data = response.json()

            games_count = len(data.get("games", []))
            print(f"  Retrieved {games_count} games")

            return data
        except requests.exceptions.RequestException as e:
            print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
            else:
                print(f"ERROR: Failed to fetch scores for {date} after {MAX_RETRIES} attempts", file=sys.stderr)
                raise


def normalize_game(game: dict, game_date: str) -> dict:
    """
    Normalize a single NHL game into our schema.

    Args:
        game: Raw game dict from API
        game_date: Game date in YYYY-MM-DD format

    Returns:
        Normalized game dict
    """
    # Extract game ID (should be present in API response)
    game_id = str(game.get("id", ""))

    # Extract team info
    home_team = game.get("homeTeam", {})
    away_team = game.get("awayTeam", {})

    home_abbrev = home_team.get("abbrev", home_team.get("placeName", {}).get("default", ""))
    away_abbrev = away_team.get("abbrev", away_team.get("placeName", {}).get("default", ""))

    # Extract scores
    home_score = home_team.get("score")
    away_score = away_team.get("score")

    # Map game state to status
    # NHL API uses: "FUT" (future), "LIVE", "FINAL", "OFF" (official)
    game_state = game.get("gameState", "")
    if game_state in ("FINAL", "OFF"):
        status = "final"
    elif game_state == "LIVE":
        status = "in_progress"
    else:
        status = "scheduled"

    # Game datetime (UTC)
    start_time_utc = game.get("startTimeUTC")

    # Infer season
    season = infer_season(game_date)

    # Postseason flag (infer from game type if available)
    # NHL API may have gameType: 2 = regular season, 3 = playoffs
    game_type = game.get("gameType", 2)
    postseason = game_type == 3

    # Neutral site (NHL API may not provide this reliably)
    neutral_site = None

    return {
        "game_id": game_id,
        "sport": "NHL",
        "season": season,
        "game_date": game_date,
        "game_datetime_utc": start_time_utc,
        "home_team": home_abbrev,
        "away_team": away_abbrev,
        "home_score": home_score,
        "away_score": away_score,
        "status": status,
        "neutral_site": neutral_site,
        "postseason": postseason,
        "source": "nhl_api_web",
    }


def fetch_and_normalize(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Fetch and normalize NHL games for a date range.

    Args:
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)

    Returns:
        DataFrame of normalized games
    """
    all_games = []

    # Generate date range
    start = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    current = start
    while current <= end:
        date_str = current.isoformat()

        try:
            data = fetch_scores_for_date(date_str)
            games = data.get("games", [])

            for game in games:
                normalized = normalize_game(game, date_str)
                all_games.append(normalized)

        except Exception as e:
            print(f"ERROR: Failed to process date {date_str}: {e}", file=sys.stderr)
            raise

        current += timedelta(days=1)

    print(f"\nTotal games fetched and normalized: {len(all_games)}")

    if not all_games:
        return pd.DataFrame()

    df = pd.DataFrame(all_games)

    # Convert types
    df["game_id"] = df["game_id"].astype(str)
    df["season"] = pd.to_numeric(df["season"], errors="coerce").astype("Int64")
    df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").astype("Int64")
    df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").astype("Int64")
    df["postseason"] = df["postseason"].astype(bool)

    return df


def upsert_to_parquet(new_data: pd.DataFrame, output_path: Path) -> tuple[int, int]:
    """
    Upsert new game data into existing parquet file.

    Merges on game_id, preferring new data for conflicts.

    Args:
        new_data: New game data to upsert
        output_path: Path to parquet file

    Returns:
        Tuple of (inserted_count, updated_count)
    """
    if new_data.empty:
        print("No new data to upsert")
        return 0, 0

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load existing data if file exists
    if output_path.exists():
        print(f"Loading existing data from {output_path}...")
        existing_data = pd.read_parquet(output_path)
        print(f"  Existing records: {len(existing_data)}")

        # Find new vs updated records
        existing_ids = set(existing_data["game_id"])
        new_ids = set(new_data["game_id"])

        inserted_ids = new_ids - existing_ids
        updated_ids = new_ids & existing_ids

        # Merge: remove old versions of updated games, then concat
        merged = pd.concat([
            existing_data[~existing_data["game_id"].isin(updated_ids)],
            new_data
        ], ignore_index=True)

        # Sort by season, game_date, game_id for consistency
        merged = merged.sort_values(
            ["season", "game_date", "game_id"]
        ).reset_index(drop=True)

        inserted = len(inserted_ids)
        updated = len(updated_ids)
    else:
        print(f"No existing data found, creating new file at {output_path}")
        merged = new_data.sort_values(
            ["season", "game_date", "game_id"]
        ).reset_index(drop=True)
        inserted = len(merged)
        updated = 0

    # Write to parquet
    print(f"Writing {len(merged)} total records to {output_path}...")
    merged.to_parquet(output_path, index=False, engine="pyarrow")
    print("  Write complete")

    return inserted, updated


def main():
    """Main execution flow."""
    print("=" * 60)
    print("NHL Scores Refresh - NHL Web API")
    print("=" * 60)
    print(f"Run time: {datetime.utcnow().isoformat()} UTC")
    print()

    # Calculate date window in Denver timezone
    start_date, end_date = get_date_window()
    print(f"Date window (America/Denver): {start_date} to {end_date}")
    print()

    # Fetch and normalize
    df = fetch_and_normalize(start_date, end_date)
    print()

    # Upsert to parquet
    inserted, updated = upsert_to_parquet(df, OUTPUT_FILE)
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Games in window: {len(df)}")
    print(f"New games:       {inserted}")
    print(f"Updated games:   {updated}")
    print(f"Output file:     {OUTPUT_FILE}")
    print()
    print("✓ Refresh complete")


if __name__ == "__main__":
    main()
