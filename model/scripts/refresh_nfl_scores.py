#!/usr/bin/env python3
"""
Nightly NFL scores refresh using nfl_data_py.

Fetches recent games (yesterday + today) from NFL schedule data and upserts
into local parquet storage. Designed to run via GitHub Actions nightly.
"""

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "nfl"
OUTPUT_FILE = OUTPUT_DIR / "nfl_games_with_scores.parquet"
DENVER_TZ = ZoneInfo("America/Denver")
MAX_RETRIES = 3


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


def get_seasons_to_fetch(start_date: str, end_date: str) -> list[int]:
    """
    Determine which NFL seasons to fetch based on date window.

    NFL season typically runs Aug-Feb, so we may need to fetch 2 seasons
    if the date window spans a year boundary.

    Args:
        start_date: Start date in YYYY-MM-DD
        end_date: End date in YYYY-MM-DD

    Returns:
        List of season years to fetch
    """
    start_year = int(start_date[:4])
    end_year = int(end_date[:4])

    # Get current NFL season (season year is the year it starts, not ends)
    now_denver = datetime.now(DENVER_TZ)
    current_year = now_denver.year

    # If we're in Jan/Feb, the current NFL season actually started last year
    if now_denver.month <= 2:
        current_season = current_year - 1
    else:
        current_season = current_year

    # Fetch current season and previous season to be safe
    seasons = list(set([current_season, current_season - 1]))
    seasons.sort()

    return seasons


def fetch_nfl_schedules(seasons: list[int]) -> pd.DataFrame:
    """
    Fetch NFL schedule data for given seasons using nfl_data_py.

    Args:
        seasons: List of season years to fetch

    Returns:
        DataFrame of schedule data
    """
    # Import here to avoid loading at module level
    try:
        import nfl_data_py as nfl
    except ImportError:
        print("ERROR: nfl_data_py is not installed", file=sys.stderr)
        print("Install with: pip install nfl-data-py", file=sys.stderr)
        sys.exit(1)

    print(f"Fetching NFL schedules for seasons: {seasons}")

    for attempt in range(MAX_RETRIES):
        try:
            schedules = nfl.import_schedules(seasons)
            print(f"  Successfully fetched {len(schedules)} total games")
            return schedules
        except Exception as e:
            print(f"  Attempt {attempt + 1}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES - 1:
                print("  Retrying...")
            else:
                print(f"ERROR: Failed to fetch schedules after {MAX_RETRIES} attempts", file=sys.stderr)
                sys.exit(1)

    return pd.DataFrame()  # Should never reach here


def normalize_nfl_data(schedules: pd.DataFrame, start_date: str, end_date: str) -> pd.DataFrame:
    """
    Normalize NFL schedule data into consistent schema and filter to date window.

    Expected output columns:
    - game_id: Unique game identifier from nfl_data_py
    - sport: Always "NFL"
    - season: Season year
    - week: Week number
    - game_date: Game date in YYYY-MM-DD format
    - game_datetime_utc: ISO datetime string (if available)
    - home_team: Home team abbreviation
    - away_team: Away team abbreviation
    - home_score: Final home team score (null if not finished)
    - away_score: Final away team score (null if not finished)
    - status: Game status (derived from scores)
    - neutral_site: Boolean flag for neutral site games
    - postseason: Boolean flag (True if game_type != 'REG')
    - source: Always "nfl_data_py"

    Args:
        schedules: Raw schedule DataFrame from nfl_data_py
        start_date: Filter start date (YYYY-MM-DD)
        end_date: Filter end date (YYYY-MM-DD)

    Returns:
        Normalized and filtered DataFrame
    """
    if schedules.empty:
        return pd.DataFrame()

    print(f"Normalizing {len(schedules)} games...")

    # Filter to date window first
    # nfl_data_py uses 'gameday' column for dates
    if 'gameday' in schedules.columns:
        # Ensure gameday is string for comparison
        schedules['gameday'] = schedules['gameday'].astype(str)
        in_window = (schedules['gameday'] >= start_date) & (schedules['gameday'] <= end_date)
        filtered = schedules[in_window].copy()
        print(f"  Filtered to {len(filtered)} games in date window ({start_date} to {end_date})")
    else:
        print("  WARNING: 'gameday' column not found, using all games")
        filtered = schedules.copy()

    if filtered.empty:
        print("  No games found in date window")
        return pd.DataFrame()

    # Build normalized schema
    normalized = pd.DataFrame({
        'game_id': filtered.get('game_id', ''),
        'sport': 'NFL',
        'season': filtered.get('season', None),
        'week': filtered.get('week', None),
        'game_date': filtered.get('gameday', None),
        'home_team': filtered.get('home_team', None),
        'away_team': filtered.get('away_team', None),
        'home_score': filtered.get('home_score', None),
        'away_score': filtered.get('away_score', None),
        'neutral_site': filtered.get('location', '').str.lower() == 'neutral',
        'source': 'nfl_data_py',
    })

    # Construct game_datetime_utc if we have gameday and gametime
    if 'gameday' in filtered.columns and 'gametime' in filtered.columns:
        # gametime is typically in ET format like "13:00"
        # For simplicity, we'll just store the date; full datetime conversion
        # would require knowing timezone and converting to UTC
        normalized['game_datetime_utc'] = None  # Can enhance later if needed
    else:
        normalized['game_datetime_utc'] = None

    # Determine postseason flag from game_type
    # game_type values: 'REG' (regular season), 'WC', 'DIV', 'CON', 'SB' (playoffs)
    if 'game_type' in filtered.columns:
        normalized['postseason'] = filtered['game_type'] != 'REG'
    else:
        # If game_type not available, try to infer from week (weeks 18+ are usually playoffs)
        if 'week' in filtered.columns:
            normalized['postseason'] = filtered['week'] > 18
        else:
            normalized['postseason'] = False

    # Derive status from scores
    # If both scores are null, game hasn't started
    # If scores exist, game is final (schedules don't track "in progress" well)
    has_home = normalized['home_score'].notna()
    has_away = normalized['away_score'].notna()
    normalized['status'] = 'scheduled'
    normalized.loc[has_home & has_away, 'status'] = 'final'

    # Convert types
    normalized['season'] = pd.to_numeric(normalized['season'], errors='coerce').astype('Int64')
    normalized['week'] = pd.to_numeric(normalized['week'], errors='coerce').astype('Int64')
    normalized['home_score'] = pd.to_numeric(normalized['home_score'], errors='coerce').astype('Int64')
    normalized['away_score'] = pd.to_numeric(normalized['away_score'], errors='coerce').astype('Int64')
    normalized['neutral_site'] = normalized['neutral_site'].astype(bool)
    normalized['postseason'] = normalized['postseason'].astype(bool)

    # Ensure game_id is string and not empty
    normalized['game_id'] = normalized['game_id'].astype(str)
    if normalized['game_id'].str.strip().eq('').any():
        print("  WARNING: Some game_ids are empty, creating fallback IDs")
        # Create fallback game_id for any empty ones
        empty_ids = normalized['game_id'].str.strip().eq('')
        normalized.loc[empty_ids, 'game_id'] = (
            normalized.loc[empty_ids, 'season'].astype(str) + '_' +
            normalized.loc[empty_ids, 'week'].astype(str) + '_' +
            normalized.loc[empty_ids, 'away_team'].astype(str) + '_at_' +
            normalized.loc[empty_ids, 'home_team'].astype(str) + '_' +
            normalized.loc[empty_ids, 'game_date'].astype(str)
        )

    print(f"  Normalization complete: {len(normalized)} games ready for upsert")
    return normalized


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

        # Sort by season, week, game_date for consistency
        merged = merged.sort_values(
            ["season", "week", "game_date", "game_id"]
        ).reset_index(drop=True)

        inserted = len(inserted_ids)
        updated = len(updated_ids)
    else:
        print(f"No existing data found, creating new file at {output_path}")
        merged = new_data.sort_values(
            ["season", "week", "game_date", "game_id"]
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
    print("NFL Scores Refresh - nfl_data_py")
    print("=" * 60)
    print(f"Run time: {datetime.utcnow().isoformat()} UTC")
    print()

    # Calculate date window in Denver timezone
    start_date, end_date = get_date_window()
    print(f"Date window (America/Denver): {start_date} to {end_date}")
    print()

    # Determine seasons to fetch
    seasons = get_seasons_to_fetch(start_date, end_date)
    print(f"Seasons to fetch: {seasons}")
    print()

    # Fetch schedules
    schedules = fetch_nfl_schedules(seasons)
    print()

    # Normalize and filter
    normalized = normalize_nfl_data(schedules, start_date, end_date)
    print()

    # Upsert to parquet
    inserted, updated = upsert_to_parquet(normalized, OUTPUT_FILE)
    print()

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Seasons fetched: {seasons}")
    print(f"Games in window: {len(normalized)}")
    print(f"New games:       {inserted}")
    print(f"Updated games:   {updated}")
    print(f"Output file:     {OUTPUT_FILE}")
    print()
    print("✓ Refresh complete")


if __name__ == "__main__":
    main()
