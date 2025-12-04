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

import pandas as pd
import requests

from src.paths import PROCESSED_DIR

# ====== CONFIG ======

BALLDONTLIE_API_BASE = "https://api.balldontlie.io/v1"
BALLDONTLIE_API_KEY = os.environ.get("BALLDONTLIE_API_KEY")  # set this in your env

# First date of the season you want to backfill
START_DATE = date(2025, 10, 1)   # adjust if needed
END_DATE   = date(2026, 6, 30)   # safety upper bound


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

    while True:
        params = {
            "dates[]": iso,
            "per_page": 100,
            "page": page,
        }
        resp = requests.get(
            f"{BALLDONTLIE_API_BASE}/games",
            params=params,
            headers=get_headers(),
            timeout=15,
        )
        resp.raise_for_status()
        data = resp.json()

        for g in data.get("data", []):
            # Adjust these fields if your naming differs
            home_name = g["home_team"]["full_name"]
            away_name = g["visitor_team"]["full_name"]
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


def build_api_results_lookup(start: date, end: date) -> Dict[Tuple[str, str, str], Dict]:
    """
    For all dates in [start, end], build a dict keyed by
      (date_str, home_team_name, away_team_name)
    where values contain final scores.
    """
    lookup: Dict[Tuple[str, str, str], Dict] = {}
    cur = start
    while cur <= end:
        print(f"Fetching scores for {cur.isoformat()} ...")
        try:
            df = fetch_games_for_date(cur)
        except Exception as e:
            print(f"  ! Failed to fetch {cur}: {e}")
            cur += timedelta(days=1)
            continue

        for _, row in df.iterrows():
            key = (
                row["date"],
                row["home_team_name"],
                row["away_team_name"],
            )
            lookup[key] = {
                "home_pts": int(row["home_pts"]),
                "away_pts": int(row["away_pts"]),
            }

        cur += timedelta(days=1)

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

    # Build lookup from API
    lookup = build_api_results_lookup(START_DATE, END_DATE)

    updated = 0
    missing = 0

    for idx in games[mask_candidate].index:
        row = games.loc[idx]
        key = (row["date"], row["home_team"], row["away_team"])

        # NOTE: if your names don't match exactly (e.g. "LA Clippers" vs "Los Angeles Clippers"),
        # you’ll need a small mapping dict here to translate.
        scores = lookup.get(key)
        if not scores:
            missing += 1
            continue

        games.at[idx, "home_pts"] = scores["home_pts"]
        games.at[idx, "away_pts"] = scores["away_pts"]

        # home_win = 1 if home_pts > away_pts else 0
        games.at[idx, "home_win"] = (
            1 if scores["home_pts"] > scores["away_pts"] else 0
        )

        updated += 1

    print(f"Updated {updated} rows with final scores. {missing} rows had no match.")

    out_path = PROCESSED_DIR / "games_with_scores_and_future_backfilled.parquet"
    games.to_parquet(out_path, index=False)
    print(f"Wrote backfilled table to {out_path}")


if __name__ == "__main__":
    main()