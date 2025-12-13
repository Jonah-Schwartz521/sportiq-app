#!/usr/bin/env python3
"""
Nightly NFL scores refresh using nfl_data_py.

Fetches recent games (yesterday + today by default) from NFL schedule data and upserts
into local parquet storage without losing historical data. Designed to run via
GitHub Actions nightly.
"""

import sys
import argparse
import shutil
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# Configuration
OUTPUT_DIR = Path(__file__).parent.parent / "data" / "processed" / "nfl"
OUTPUT_FILE = OUTPUT_DIR / "nfl_games_with_scores.parquet"
BASE_FILE = OUTPUT_DIR / "nfl_games.parquet"
DENVER_TZ = ZoneInfo("America/Denver")
MAX_RETRIES = 3


def get_date_window(days: int = 2) -> tuple[str, str]:
    """
    Calculate (today - days + 1) through today in America/Denver timezone.

    Returns:
        Tuple of (start_date, end_date) in YYYY-MM-DD format
    """
    now_denver = datetime.now(DENVER_TZ)
    today = now_denver.date()
    start = today - timedelta(days=days - 1)

    return start.isoformat(), today.isoformat()


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


def normalize_nfl_data(
    schedules: pd.DataFrame,
    start_date: str | None,
    end_date: str | None,
) -> pd.DataFrame:
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

    # Filter to date window first (if provided)
    # nfl_data_py uses 'gameday' column for dates
    if start_date and end_date and 'gameday' in schedules.columns:
        # Ensure gameday is string for comparison
        schedules['gameday'] = schedules['gameday'].astype(str)
        in_window = (schedules['gameday'] >= start_date) & (schedules['gameday'] <= end_date)
        filtered = schedules[in_window].copy()
        print(f"  Filtered to {len(filtered)} games in date window ({start_date} to {end_date})")
        if not filtered.empty:
            print(f"  Window gameday min/max: {filtered['gameday'].min()} → {filtered['gameday'].max()}")
    else:
        if 'gameday' in schedules.columns:
            schedules['gameday'] = schedules['gameday'].astype(str)
        print("  No date window filtering applied (using all games)")
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
    parser = argparse.ArgumentParser(description="Refresh NFL scores parquet.")
    parser.add_argument(
        "--days",
        type=int,
        default=2,
        help="Number of days to include ending today (default: 2, i.e., yesterday + today) in America/Denver",
    )
    parser.add_argument(
        "--full-rebuild",
        action="store_true",
        help="Rebuild with_scores from base history plus updates; ignore existing with_scores file.",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("NFL Scores Refresh - nfl_data_py")
    print("=" * 60)
    print(f"Run time: {datetime.utcnow().isoformat()} UTC")
    print()

    # Calculate date window in Denver timezone
    start_date, end_date = get_date_window(days=args.days)
    print(f"Date window (America/Denver): {start_date} to {end_date}")
    print()

    # Determine seasons to fetch
    seasons = get_seasons_to_fetch(start_date, end_date)
    print(f"Seasons to fetch: {seasons}")
    print()

    # Fetch schedules
    schedules = fetch_nfl_schedules(seasons)
    print()

    # Build updates for the date window only (to keep network usage light)
    updates = normalize_nfl_data(schedules, start_date, end_date)
    update_rows = len(updates)
    print()

    # Load base dataset with robust selection
    base_df = pd.DataFrame()
    base_rows = -1
    with_rows = -1
    base_exists = BASE_FILE.exists()
    with_exists = OUTPUT_FILE.exists()

    if base_exists:
        base_rows = len(pd.read_parquet(BASE_FILE, columns=None))
        print(f"Detected base history file {BASE_FILE} with {base_rows} rows.")
    else:
        print(f"Base history file {BASE_FILE} not found.")

    if with_exists:
        with_rows = len(pd.read_parquet(OUTPUT_FILE, columns=None))
        print(f"Detected with_scores file {OUTPUT_FILE} with {with_rows} rows.")
    else:
        print(f"with_scores file {OUTPUT_FILE} not found.")

    chosen_path: Path | None = None
    reason = ""

    if args.full_rebuild:
        if base_exists:
            chosen_path = BASE_FILE
            reason = "--full-rebuild specified; using full history base file"
        elif with_exists:
            chosen_path = OUTPUT_FILE
            reason = "--full-rebuild specified but base missing; using with_scores"
    else:
        if base_exists and with_exists:
            if base_rows > with_rows:
                chosen_path = BASE_FILE
                reason = "base has more rows than with_scores"
            elif with_rows < 0.3 * base_rows:
                chosen_path = BASE_FILE
                reason = "with_scores is suspiciously small (<30% of base); recovering from base"
            else:
                chosen_path = OUTPUT_FILE
                reason = "with_scores chosen (>= base rows or acceptable size)"
        elif base_exists:
            chosen_path = BASE_FILE
            reason = "only base exists"
        elif with_exists:
            chosen_path = OUTPUT_FILE
            reason = "only with_scores exists"

    if chosen_path:
        print(f"Base dataset chosen: {chosen_path} ({reason})")
        base_df = pd.read_parquet(chosen_path)
    else:
        print("No existing base found; initializing from full schedules")
        base_df = normalize_nfl_data(schedules, None, None)
        reason = "initialized from schedules"

    if base_df.empty:
        print("ERROR: Base NFL dataset is empty; aborting to avoid data loss.", file=sys.stderr)
        sys.exit(1)

    # Ensure critical columns exist and have consistent types
    for col in ["game_id", "season", "week", "game_date", "gameday", "home_team", "away_team", "home_score", "away_score"]:
        if col not in base_df.columns:
            base_df[col] = None
    base_df["season"] = pd.to_numeric(base_df["season"], errors="coerce").astype("Int64")
    base_df["week"] = pd.to_numeric(base_df["week"], errors="coerce").astype("Int64")
    if "gameday" in base_df.columns and "game_date" not in base_df.columns:
        base_df["game_date"] = base_df["gameday"]
    if "game_date" in base_df.columns and base_df["game_date"].dtype != "string":
        base_df["game_date"] = base_df["game_date"].astype("string")
    if "gameday" in base_df.columns and base_df["gameday"].dtype != "string":
        base_df["gameday"] = base_df["gameday"].astype("string")

    # Suspicious small with_scores recovery
    if (
        not args.full_rebuild
        and base_exists
        and with_exists
        and with_rows >= 0
        and base_rows > 0
        and with_rows < 0.3 * base_rows
    ):
        print(
            f"with_scores file is <30% of base rows ({with_rows} vs {base_rows}); recovering using base history."
        )
        base_df = pd.read_parquet(BASE_FILE)
        base_df["season"] = pd.to_numeric(base_df["season"], errors="coerce").astype("Int64")
        if "gameday" in base_df.columns and "game_date" not in base_df.columns:
            base_df["game_date"] = base_df["gameday"]

    # Prepare composite key when game_id is missing
    def composite_key(df: pd.DataFrame) -> pd.Series:
        if "gameday" in df.columns:
            date_col = df["gameday"]
        elif "game_date" in df.columns:
            date_col = df["game_date"]
        else:
            date_col = pd.Series([""] * len(df), index=df.index)

        return (
            df["season"].astype(str).fillna("")
            + "|"
            + df["week"].astype(str).fillna("")
            + "|"
            + df["home_team"].astype(str).fillna("")
            + "|"
            + df["away_team"].astype(str).fillna("")
            + "|"
            + date_col.astype(str).fillna("")
        )

    if updates.empty:
        print("No updates found in the date window; keeping base dataset unchanged.")
        merged = base_df.copy()
        updated_games = 0
    else:
        updates = updates.copy()
        updates["season"] = pd.to_numeric(updates["season"], errors="coerce").astype("Int64")
        updates["week"] = pd.to_numeric(updates["week"], errors="coerce").astype("Int64")
        if "gameday" in updates.columns and "game_date" not in updates.columns:
            updates["game_date"] = updates["gameday"]
        if "game_date" in updates.columns and updates["game_date"].dtype != "string":
            updates["game_date"] = updates["game_date"].astype("string")
        updates["composite_key"] = composite_key(updates)
        updates["primary_key"] = updates["game_id"].where(
            updates["game_id"].notna() & (updates["game_id"].astype(str) != ""),
            updates["composite_key"],
        )
        updates["primary_key"] = updates["primary_key"].astype(str)

        base = base_df.copy()
        base["season"] = pd.to_numeric(base["season"], errors="coerce").astype("Int64")
        base["week"] = pd.to_numeric(base["week"], errors="coerce").astype("Int64")
        base["composite_key"] = composite_key(base)
        base["primary_key"] = base["game_id"].where(
            base["game_id"].notna() & (base["game_id"].astype(str) != ""),
            base["composite_key"],
        )
        base["primary_key"] = base["primary_key"].astype(str)

        # Union columns
        union_cols = sorted(set(base.columns) | set(updates.columns))
        base = base.reindex(columns=union_cols)
        updates = updates.reindex(columns=union_cols)

        # Update only score/status/time-related fields when a match exists
        update_targets = {
            "home_score": ["home_score"],
            "away_score": ["away_score"],
            "status": ["status", "game_status"],
            "game_datetime_utc": ["game_datetime_utc"],
            "game_date": ["game_date", "gameday"],
            "gameday": ["gameday", "game_date"],
            "week": ["week"],
            "season": ["season"],
            "neutral_site": ["neutral_site"],
            "postseason": ["postseason"],
        }

        base_index = {pk: idx for idx, pk in base["primary_key"].items()}
        updated_games = 0

        for _, row in updates.iterrows():
            pk = row["primary_key"]
            if pk in base_index:
                updated_games += 1
                idx = base_index[pk]
                for src_col, dest_cols in update_targets.items():
                    if src_col not in updates.columns:
                        continue
                    val = row[src_col]
                    if pd.isna(val):
                        continue
                    for dest_col in dest_cols:
                        if dest_col in base.columns:
                            base.at[idx, dest_col] = val
            else:
                # New game not in base; append
                base = pd.concat([base, row.to_frame().T], ignore_index=True)
                base_index[pk] = len(base_index)

        merged = base

    # Safety guard: prevent accidental wipe
    if len(merged) < len(base_df):
        print(
            f"ERROR: Merged rows ({len(merged)}) < base rows ({len(base_df)}). Aborting write.",
            file=sys.stderr,
        )
        sys.exit(1)
    if len(base_df) > 0 and len(merged) < 0.95 * len(base_df):
        print(
            f"ERROR: Merged rows ({len(merged)}) < 95% of base rows ({len(base_df)}). Aborting write.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Sort for consistency
    sort_cols = [c for c in ["season", "week", "game_date", "game_id"] if c in merged.columns]
    if sort_cols:
        merged = merged.sort_values(sort_cols).reset_index(drop=True)

    # Write atomically: temp then replace; keep backup
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    tmp_path = OUTPUT_FILE.with_suffix(".parquet.tmp")
    bak_path = OUTPUT_FILE.with_suffix(".parquet.bak")

    print(f"Writing merged dataset to temp file {tmp_path} ...")
    merged.to_parquet(tmp_path, index=False, engine="pyarrow")

    if OUTPUT_FILE.exists():
        print(f"Creating backup at {bak_path}")
        shutil.copy2(OUTPUT_FILE, bak_path)

    print(f"Replacing {OUTPUT_FILE} with merged dataset")
    tmp_path.replace(OUTPUT_FILE)

    # Summary
    print("=" * 60)
    print("SUMMARY")
    print("=" * 60)
    print(f"Base rows:    {len(base_df)}")
    print(f"Update rows:  {update_rows}")
    print(f"Merged rows:  {len(merged)}")
    print(f"Games updated (by key overlap): {updated_games}")
    if not updates.empty and 'game_date' in updates.columns:
        print(f"Update window game_date min/max: {updates['game_date'].min()} → {updates['game_date'].max()}")
    print(f"Output file:  {OUTPUT_FILE}")
    print()
    print("✓ Refresh complete")


if __name__ == "__main__":
    main()
