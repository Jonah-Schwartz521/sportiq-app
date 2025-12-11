#!/usr/bin/env python
"""
Backfill final scores for 2025 games into games_with_scores_and_future.parquet.

- Uses balldontlie to fetch game results by date.
- Matches rows by (date, home_team, away_team).
- Fills home_pts, away_pts, and home_win.
"""

from __future__ import annotations

import os
from datetime import datetime, date, timedelta
from typing import Dict, Tuple
import time

import pandas as pd
import requests

import sys
from pathlib import Path

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import PROCESSED_DIR  # type: ignore  # noqa

# ====== CONFIG ======

BALLDONTLIE_API_BASE = "https://api.balldontlie.io/v1"
BALLDONTLIE_API_KEY = os.environ.get("BALLDONTLIE_API_KEY")  # set this in your env

# First date of the season you want to backfill.
# END_DATE is just a hard safety upper bound; in practice we will only
# hit dates that actually appear in the schedule and that are <= today.
START_DATE = date(2024, 10, 1)   # 2024-25 season started Oct 2024
END_DATE   = date(2026, 6, 30)   # safety upper bound

TEAM_NAME_FIXES: Dict[str, str] = {
    "LA Clippers": "Los Angeles Clippers",
    "Los Angeles Clippers": "Los Angeles Clippers",
    # Add more mappings here if you encounter other mismatches
}

def get_headers() -> Dict[str, str]:
    if not BALLDONTLIE_API_KEY:
        raise RuntimeError(
            "BALLDONTLIE_API_KEY not set – export it before running this script."
        )
    # balldontlie v2 uses Authorization header; if yours is different, change here
    return {"Authorization": BALLDONTLIE_API_KEY}


def fetch_games_for_date(d: date) -> pd.DataFrame:
    """
    Fetch all NBA games for a specific calendar date from balldontlie.
    Returns a small DataFrame with:
        date, home_team_name, away_team_name, home_pts, away_pts
    """
    iso = d.isoformat()
    games = []
    page = 1
    max_attempts = 5

    while True:
        params = {
            "dates[]": iso,
            "per_page": 100,
            "page": page,
        }

        # Simple retry loop to handle 429 rate limits
        resp = None
        for attempt in range(1, max_attempts + 1):
            try:
                resp = requests.get(
                    f"{BALLDONTLIE_API_BASE}/games",
                    params=params,
                    headers=get_headers(),
                    timeout=15,
                )
            except Exception as e:
                # For network errors, give up immediately; you can add retry here if desired
                raise

            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                try:
                    sleep_seconds = int(retry_after) if retry_after is not None else 2**attempt
                except ValueError:
                    sleep_seconds = 2**attempt

                print(
                    f"  Got 429 Too Many Requests for {iso} (page {page}, attempt {attempt}/{max_attempts}). "
                    f"Sleeping {sleep_seconds} seconds before retry..."
                )
                time.sleep(sleep_seconds)
                continue

            # Non-429: either OK or some other error
            resp.raise_for_status()
            break
        else:
            # All attempts exhausted
            print(f"  ! Giving up on {iso} page {page} after {max_attempts} attempts.")
            break

        data = resp.json()

        for g in data.get("data", []):
            # Raw names from API
            home_name_raw = g["home_team"]["full_name"]
            away_name_raw = g["visitor_team"]["full_name"]

            # Normalize to the same style used in your parquet
            home_name = TEAM_NAME_FIXES.get(home_name_raw, home_name_raw)
            away_name = TEAM_NAME_FIXES.get(away_name_raw, away_name_raw)

            home_pts = g["home_team_score"]
            away_pts = g["visitor_team_score"]

            games.append(
                {
                    "date": iso,
                    "home_team_name": home_name,
                    "away_team_name": away_name,
                    "home_pts": home_pts,
                    "away_pts": away_pts,
                }
            )

        meta = data.get("meta", {})
        if page >= meta.get("total_pages", 1):
            break
        page += 1

    return pd.DataFrame(games)


def build_api_results_lookup(dates: list[str]) -> Dict[Tuple[str, str, str], Dict]:
    """
    Build a lookup of final scores keyed by
      (date_str, home_team_name, away_team_name)
    but ONLY for the distinct dates that actually appear in our schedule.

    This prevents us from hammering the API for days where we have no games
    in our parquet (and avoids endlessly walking into far-future dates).
    """
    lookup: Dict[Tuple[str, str, str], Dict] = {}
    for date_str in dates:
        print(f"Fetching scores for {date_str} ...")
        try:
            d = datetime.strptime(date_str, "%Y-%m-%d").date()
            df = fetch_games_for_date(d)
        except Exception as e:
            print(f"  ! Failed to fetch {date_str}: {e}")
            continue

        for _, row in df.iterrows():
            key = (
                row["date"],            # already an ISO string from fetch_games_for_date
                row["home_team_name"],
                row["away_team_name"],
            )
            lookup[key] = {
                "home_pts": int(row["home_pts"]),
                "away_pts": int(row["away_pts"]),
            }

    print(f"Built lookup with {len(lookup)} completed games.")
    return lookup


def main() -> None:
    path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
    print(f"Loading games table from {path} ...")
    games = pd.read_parquet(path)

    # Ensure date is string "YYYY-MM-DD"
    if not isinstance(games["date"].iloc[0], str):
        games = games.copy()
        games["date"] = pd.to_datetime(games["date"]).dt.date.astype(str)

    # Restrict to 2025+ rows where scores are missing
    mask_candidate = (
        (games["date"] >= START_DATE.isoformat())
        & (games["date"] <= END_DATE.isoformat())
        & (games["home_pts"].isna() | games["away_pts"].isna())
    )
    num_candidates = mask_candidate.sum()
    print(f"Found {num_candidates} candidate rows with missing scores in 2025+.")

    # Only query the API for dates that actually appear in our schedule
    # and that are not in the future relative to *today*.
    if num_candidates == 0:
        print("No candidate rows to backfill; exiting.")
        return

    candidate_dates = sorted({d for d in games.loc[mask_candidate, "date"]})
    today_iso = date.today().isoformat()
    candidate_dates = [d for d in candidate_dates if d <= today_iso]

    if not candidate_dates:
        print("All candidate dates are in the future; nothing to backfill yet.")
        return

    print(
        f"Will query API for {len(candidate_dates)} distinct dates "
        f"from {candidate_dates[0]} to {candidate_dates[-1]}."
    )

    # Build lookup from API only for the dates we actually need
    lookup = build_api_results_lookup(candidate_dates)

    updated = 0
    missing = 0

    for idx in games[mask_candidate].index:
        row = games.loc[idx]

        # Normalize home/away team names to match the API-derived keys
        home_name = TEAM_NAME_FIXES.get(row["home_team"], row["home_team"])
        away_name = TEAM_NAME_FIXES.get(row["away_team"], row["away_team"])

        key = (row["date"], home_name, away_name)

        # NOTE: if your names don't match exactly (e.g. "LA Clippers" vs "Los Angeles Clippers"),
        # you’ll need a small mapping dict here to translate.
        scores = lookup.get(key)
        if not scores:
            missing += 1
            continue

        games.at[idx, "home_pts"] = scores["home_pts"]
        games.at[idx, "away_pts"] = scores["away_pts"]

        # Mirror into UI score columns if present
        if "home_score" in games.columns:
            games.at[idx, "home_score"] = scores["home_pts"]
        if "away_score" in games.columns:
            games.at[idx, "away_score"] = scores["away_pts"]

        # home_win = 1 if home_pts > away_pts else 0
        games.at[idx, "home_win"] = (
            1 if scores["home_pts"] > scores["away_pts"] else 0
        )

        updated += 1

    print(f"Updated {updated} rows with final scores. {missing} rows had no match.")

    out_path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
    games.to_parquet(out_path, index=False)
    print(f"Wrote backfilled table to {out_path}")


if __name__ == "__main__":
    main()