# model/scripts/update_daily_games.py

from __future__ import annotations

import os
from pathlib import Path
from datetime import datetime, timedelta

import sys
import pandas as pd
import requests

# --- Project paths --------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import PROCESSED_DIR  # type: ignore  # noqa

COMBINED_PATH = PROCESSED_DIR / "games_with_scores_and_future.parquet"

# --- BallDontLie config ---------------------------------------------

# NOTE: base URL for the *new* BallDontLie platform.
# If their docs show a different base, change it here.
BALLDONTLIE_BASE_URL = "https://api.balldontlie.io/v1"

API_KEY = os.environ.get("BALLDONTLIE_API_KEY")
if not API_KEY:
    raise RuntimeError(
        "BALLDONTLIE_API_KEY is not set. "
        "Export it in your shell or ~/.zshrc before running this script."
    )

HEADERS = {"Authorization": API_KEY}


def fetch_yesterday_results() -> list[dict]:
    """
    Pull yesterday's NBA games from BallDontLie.
    We only care about:
      - date
      - home_team.full_name
      - visitor_team.full_name
      - home_team_score
      - visitor_team_score
    """
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()
    date_str = yesterday.isoformat()
    print(f"Fetching NBA results for {date_str}...")

    params = {
        "dates[]": date_str,
        "per_page": 100,
    }

    url = f"{BALLDONTLIE_BASE_URL}/games"
    resp = requests.get(url, params=params, headers=HEADERS, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    games = data.get("data", [])
    print(f"  Retrieved {len(games)} games from BallDontLie.")
    return games


def main() -> None:
    # 1) Load existing combined parquet (historical + future schedule)
    print("Loading existing combined games parquet...")
    df = pd.read_parquet(COMBINED_PATH)
    df["date"] = pd.to_datetime(df["date"])

    # 2) Fetch yesterday's games
    games = fetch_yesterday_results()
    if not games:
        print("No games returned for yesterday; nothing to update.")
        return

    # For matching, work with a date-only column
    df["date_only"] = df["date"].dt.date

    updates = 0
    yesterday = (datetime.utcnow() - timedelta(days=1)).date()

    for g in games:
        # BallDontLie fields (double-check against their docs if needed)
        game_date = datetime.fromisoformat(g["date"]).date()
        if game_date != yesterday:
            # Shouldn't happen, but be safe
            continue

        home_name = g["home_team"]["full_name"]
        away_name = g["visitor_team"]["full_name"]
        home_pts = g["home_team_score"]
        away_pts = g["visitor_team_score"]

        # 3) Find the corresponding row in our combined table
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

        # We expect exactly one row, but .loc is safe
        df.loc[mask, "home_pts"] = int(home_pts)
        df.loc[mask, "away_pts"] = int(away_pts)
        df.loc[mask, "home_win"] = 1 if home_pts > away_pts else 0

        updates += int(mask.sum())

    if updates == 0:
        print("No rows were updated. Check name matching / dates.")
    else:
        print(f"Updated {updates} rows with final scores.")

    # 4) Clean up helper column and write back
    df = df.drop(columns=["date_only"])
    df.to_parquet(COMBINED_PATH, index=False)
    print("Wrote updated parquet to:", COMBINED_PATH)


if __name__ == "__main__":
    main()