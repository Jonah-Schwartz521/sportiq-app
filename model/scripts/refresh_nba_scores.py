#!/usr/bin/env python3
"""
Nightly NBA scores refresh using BallDontLie API.

Fetches recent games (yesterday + today) and upserts into local parquet storage.
Designed to run via GitHub Actions on a nightly schedule.
"""

import os
import sys
from datetime import datetime, timedelta
from pathlib import Path
import time

import pandas as pd
import requests

# Configuration
API_BASE = "https://api.balldontlie.io/v1"
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "nba"
OUTPUT_FILE = OUTPUT_DIR / "nba_games_with_scores.parquet"
MAX_RETRIES = 3
RETRY_DELAY = 2  # seconds


def get_api_key() -> str:
    """Retrieve BallDontLie API key from environment."""
    api_key = os.getenv("BALLDONTLIE_API_KEY")
    if not api_key:
        print("ERROR: BALLDONTLIE_API_KEY environment variable not set", file=sys.stderr)
        sys.exit(1)
    return api_key


def fetch_games(api_key: str, start_date: str, end_date: str) -> list[dict]:
    """
    Fetch games from BallDontLie API for a date range.

    Args:
        api_key: BallDontLie API key
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format

    Returns:
        List of game dictionaries
    """
    headers = {"Authorization": api_key}

    # BallDontLie uses start_date and end_date query params
    params = {
        "start_date": start_date,
        "end_date": end_date,
        "per_page": 100  # Max per page to minimize requests
    }

    all_games = []
    page = 1

    while True:
        params["page"] = page
        url = f"{API_BASE}/games"

        print(f"Fetching games page {page} for {start_date} to {end_date}...")

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=headers, params=params, timeout=30)
                response.raise_for_status()
                data = response.json()
                break
            except requests.exceptions.RequestException as e:
                print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY * (attempt + 1))  # Exponential backoff
                else:
                    print(f"ERROR: Failed to fetch games after {MAX_RETRIES} attempts", file=sys.stderr)
                    sys.exit(1)

        games = data.get("data", [])
        if not games:
            break

        all_games.extend(games)
        print(f"  Retrieved {len(games)} games")

        # Check if there are more pages
        meta = data.get("meta", {})
        if page >= meta.get("total_pages", 1):
            break

        page += 1
        time.sleep(0.5)  # Be nice to the API

    print(f"Total games fetched: {len(all_games)}")
    return all_games


def normalize_games(games: list[dict]) -> pd.DataFrame:
    """
    Normalize BallDontLie game data into consistent schema.

    Expected output columns:
    - game_id: Unique game identifier
    - date_utc: Game date in YYYY-MM-DD format
    - home_team_id: Home team ID
    - home_team_name: Home team full name
    - away_team_id: Away team ID (visitor)
    - away_team_name: Away team full name
    - home_score: Final home team score (null if not finished)
    - away_score: Final away team score (null if not finished)
    - status: Game status
    - season: Season year
    - postseason: Boolean flag
    """
    if not games:
        return pd.DataFrame()

    records = []
    for game in games:
        # Parse date from ISO format
        date_str = game.get("date", "")
        if date_str:
            date_utc = date_str.split("T")[0]  # Extract YYYY-MM-DD
        else:
            date_utc = None

        # Extract team info
        home_team = game.get("home_team", {})
        visitor_team = game.get("visitor_team", {})

        record = {
            "game_id": game.get("id"),
            "date_utc": date_utc,
            "home_team_id": home_team.get("id"),
            "home_team_name": home_team.get("full_name"),
            "away_team_id": visitor_team.get("id"),
            "away_team_name": visitor_team.get("full_name"),
            "home_score": game.get("home_team_score"),
            "away_score": game.get("visitor_team_score"),
            "status": game.get("status"),
            "season": game.get("season"),
            "postseason": game.get("postseason", False),
        }
        records.append(record)

    df = pd.DataFrame(records)

    # Convert types
    if not df.empty:
        df["game_id"] = df["game_id"].astype("Int64")
        df["home_team_id"] = df["home_team_id"].astype("Int64")
        df["away_team_id"] = df["away_team_id"].astype("Int64")
        df["home_score"] = pd.to_numeric(df["home_score"], errors="coerce").astype("Int64")
        df["away_score"] = pd.to_numeric(df["away_score"], errors="coerce").astype("Int64")
        df["season"] = df["season"].astype("Int64")
        df["postseason"] = df["postseason"].astype(bool)

    return df


def upsert_to_parquet(new_data: pd.DataFrame, output_path: Path) -> tuple[int, int]:
    """
    Upsert new game data into existing parquet file.

    Merges on game_id, preferring new data for conflicts.

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
        existing_ids = set(existing_data["game_id"].dropna())
        new_ids = set(new_data["game_id"].dropna())

        inserted_ids = new_ids - existing_ids
        updated_ids = new_ids & existing_ids

        # Merge: remove old versions of updated games, then concat
        merged = pd.concat([
            existing_data[~existing_data["game_id"].isin(updated_ids)],
            new_data
        ], ignore_index=True)

        # Sort by date and game_id for consistency
        merged = merged.sort_values(["date_utc", "game_id"]).reset_index(drop=True)

        inserted = len(inserted_ids)
        updated = len(updated_ids)
    else:
        print(f"No existing data found, creating new file at {output_path}")
        merged = new_data.sort_values(["date_utc", "game_id"]).reset_index(drop=True)
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
    print("NBA Scores Refresh - BallDontLie API")
    print("=" * 60)
    print(f"Run time: {datetime.utcnow().isoformat()} UTC")
    print()

    # Get API key
    api_key = get_api_key()
    print("✓ API key loaded from environment")
    print()

    # Calculate date window: yesterday and today (UTC)
    today = datetime.utcnow().date()
    yesterday = today - timedelta(days=1)

    start_date = yesterday.isoformat()
    end_date = today.isoformat()

    print(f"Date window: {start_date} to {end_date}")
    print()

    # Fetch games
    games = fetch_games(api_key, start_date, end_date)
    print()

    # Normalize data
    print("Normalizing game data...")
    df = normalize_games(games)
    print(f"  Normalized {len(df)} games")
    print()

    # Upsert to parquet
    inserted, updated = upsert_to_parquet(df, OUTPUT_FILE)
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Games fetched:  {len(games)}")
    print(f"New games:      {inserted}")
    print(f"Updated games:  {updated}")
    print(f"Output file:    {OUTPUT_FILE}")
    print()
    print("✓ Refresh complete")


if __name__ == "__main__":
    main()
