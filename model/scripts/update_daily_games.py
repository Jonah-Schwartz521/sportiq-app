# model/scripts/update_daily_games.py

from __future__ import annotations

import os
import sys
from pathlib import Path
from datetime import date, datetime, timedelta, UTC
from typing import List, Dict

import time  # 👈 for retry backoff
import pandas as pd
import requests

# --- Project paths --------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import PROCESSED_DIR  # type: ignore  # noqa

COMBINED_PATH = PROCESSED_DIR / "games_with_scores_and_future.parquet"

# --- BallDontLie config ---------------------------------------------

BALLDONTLIE_BASE_URL = "https://api.balldontlie.io/v1"

API_KEY = os.environ.get("BALLDONTLIE_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "BALLDONTLIE_API_KEY is not set. "
        "Export it in your shell or ~/.zshrc before running this script."
    )

HEADERS = {"Authorization": API_KEY}


TEAM_NAME_FIXES: Dict[str, str] = {
    "LA Clippers": "Los Angeles Clippers",
    "Los Angeles Clippers": "Los Angeles Clippers",  # normalize just in case
    # You can add more if you ever hit similar issues:
    # "LA Lakers": "Los Angeles Lakers",
}


def fetch_results_for_date(day: date) -> List[Dict]:
    """
    Pull NBA games for a specific date from BallDontLie,
    with simple retry handling for rate limits (429).
    """
    date_str = day.isoformat()
    max_attempts = 5

    for attempt in range(1, max_attempts + 1):
        print(f"Fetching NBA results for {date_str} (attempt {attempt}/{max_attempts})...")

        params = {
            "dates[]": date_str,
            "per_page": 100,
        }

        url = f"{BALLDONTLIE_BASE_URL}/games"
        resp = requests.get(url, params=params, headers=HEADERS, timeout=30)

        # 429: Too Many Requests – respect rate limit and retry
        if resp.status_code == 429:
            retry_after = resp.headers.get("Retry-After")
            try:
                sleep_seconds = int(retry_after) if retry_after is not None else 2**attempt
            except ValueError:
                sleep_seconds = 2**attempt

            print(
                f"  Got 429 Too Many Requests for {date_str}. "
                f"Sleeping {sleep_seconds} seconds before retry..."
            )
            time.sleep(sleep_seconds)
            continue

        # Other error codes: raise and stop
        resp.raise_for_status()

        data = resp.json()
        games = data.get("data", [])
        print(f"  Retrieved {len(games)} games from BallDontLie.")
        return games

    # If we get here, all attempts failed
    print(f"  ERROR: Failed to fetch results for {date_str} after {max_attempts} attempts.")
    return []


def update_df_for_date(df: pd.DataFrame, day: date) -> int:
    """
    For a given calendar day, fetch BallDontLie results and update
    home_pts, away_pts, home_score, away_score, home_win in df.

    Returns the number of rows updated.
    """
    games = fetch_results_for_date(day)
    if not games:
        print(f"  No games returned for {day}; nothing to update.")
        return 0

    updates = 0

    # Use a helper date_only column for matching
    if "date_only" not in df.columns:
        df["date"] = pd.to_datetime(df["date"])
        df["date_only"] = df["date"].dt.date

    for g in games:
        # BallDontLie fields
        game_date = datetime.fromisoformat(g["date"]).date()
        if game_date != day:
            # Shouldn't happen, but be safe
            continue

        # Raw names from BallDontLie
        home_name_raw = g["home_team"]["full_name"]
        away_name_raw = g["visitor_team"]["full_name"]

        # Normalize using TEAM_NAME_FIXES so short names like "LA Clippers"
        # match our schedule names like "Los Angeles Clippers".
        home_name = TEAM_NAME_FIXES.get(home_name_raw, home_name_raw)
        away_name = TEAM_NAME_FIXES.get(away_name_raw, away_name_raw)

        home_pts = g["home_team_score"]
        away_pts = g["visitor_team_score"]

        mask = (
            (df["date_only"] == game_date)
            & (df["home_team"] == home_name)
            & (df["away_team"] == away_name)
        )

        if not mask.any():
            print(
                f"  WARNING: No match for {away_name} @ {home_name} "
                f"on {game_date} in combined parquet."
            )
            continue

        # Update core point columns
        df.loc[mask, "home_pts"] = home_pts
        df.loc[mask, "away_pts"] = away_pts

        # Also mirror into home_score / away_score used by the UI
        df.loc[mask, "home_score"] = home_pts
        df.loc[mask, "away_score"] = away_pts

        # Compute home_win (1 or 0)
        df.loc[mask, "home_win"] = 1 if home_pts > away_pts else 0

        updates += int(mask.sum())

    if updates == 0:
        print(f"  No rows were updated for {day}.")
    else:
        print(f"  Updated {updates} row(s) for {day} with final scores.")

    return updates


def backfill_2025(df: pd.DataFrame) -> pd.DataFrame:
    """
    One-time backfill: iterate through all 2025 dates present in the schedule
    (and up to yesterday) and update scores from BallDontLie.
    """
    print("=== Starting 2025 backfill from BallDontLie ===")

    # Ensure date & helper column exist
    df["date"] = pd.to_datetime(df["date"])
    df["date_only"] = df["date"].dt.date

    # All 2025 schedule days that appear in the parquet
    schedule_days_2025 = sorted({d for d in df["date_only"] if d.year == 2025})

    if not schedule_days_2025:
        print("No 2025 dates found in the parquet; nothing to backfill.")
        return df

    today_utc = datetime.now(UTC).date()
    yesterday = today_utc - timedelta(days=1)

    total_updates = 0
    for day in schedule_days_2025:
        if day > yesterday:
            # Don't try to fetch future games
            continue

        print(f"\n--- Updating results for {day} ---")
        updated_for_day = update_df_for_date(df, day)
        total_updates += updated_for_day

        # 🔹 Incremental save WITHOUT the helper column 'date_only'
        df_to_write = df.drop(columns=["date_only"], errors="ignore")
        df_to_write.to_parquet(COMBINED_PATH, index=False)
        print(f"  Wrote incremental parquet after {day} updates.")

    print(f"\n=== Backfill complete. Total rows updated for 2025: {total_updates} ===")

    # Drop helper column in the in-memory df before returning
    df = df.drop(columns=["date_only"], errors="ignore")
    return df


def main() -> None:
    # 1) Load existing combined parquet (historical + future schedule)
    print("Loading existing combined games parquet...")
    path = COMBINED_PATH
    print("Path:", path)
    df = pd.read_parquet(path)

    # 🔹 Clean up any accidentally persisted helper column from past runs
    if "date_only" in df.columns:
        print("Dropping stale 'date_only' helper column from parquet on load.")
        df = df.drop(columns=["date_only"])

    # Decide mode based on CLI args
    args = sys.argv[1:]
    backfill_mode = "--backfill-2025" in args

    if backfill_mode:
        # One-time backfill for all available 2025 games
        df = backfill_2025(df)
    else:
        # Normal daily mode: update yesterday only (for cron)
        today_utc = datetime.now(UTC).date()
        yesterday = today_utc - timedelta(days=1)
        print(f"Running daily update for yesterday: {yesterday}")
        df["date"] = pd.to_datetime(df["date"])
        df["date_only"] = df["date"].dt.date
        update_df_for_date(df, yesterday)
        # drop helper before final write
        df = df.drop(columns=["date_only"], errors="ignore")

    # 4) Final write – ensure no 'date_only' helper column is saved
    df.to_parquet(path, index=False)
    print("Wrote updated parquet to:", path)


if __name__ == "__main__":
    main()