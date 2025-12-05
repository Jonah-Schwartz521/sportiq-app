# model/scripts/build_mlb_model_input.py

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import MLB_PROCESSED_DIR

GAMES_PATH = MLB_PROCESSED_DIR / "mlb_games_with_scores.parquet"
TEAMS_PATH = MLB_PROCESSED_DIR / "mlb_teams.parquet"
OUTPUT_PATH = MLB_PROCESSED_DIR / "mlb_model_input.parquet"

def add_rest_features(df):
    df = df.sort_values(["home_team_id", "date"]).copy()
    df["home_last_date"] = df.groupby("home_team_id")["date"].shift(1)
    df["home_days_rest"] = (df["date"] - df["home_last_date"]).dt.days

    df = df.sort_values(["away_team_id", "date"]).copy()
    df["away_last_date"] = df.groupby("away_team_id")["date"].shift(1)
    df["away_days_rest"] = (df["date"] - df["away_last_date"]).dt.days

    df["home_is_b2b"] = (df["home_days_rest"] == 1).astype("Int64")
    df["away_is_b2b"] = (df["away_days_rest"] == 1).astype("Int64")
    return df

def build_mlb_model_input():
    print("=== Building MLB model_input from Lahman teams ===")

    games = pd.read_parquet(GAMES_PATH)

    # Ensure we have a stable numeric game_id and sport column
    if "game_id" not in games.columns:
        # Sort for stability, then assign sequential IDs
        sort_cols = [c for c in ["season", "date", "home_team_id", "away_team_id"] if c in games.columns]
        if sort_cols:
            games = games.sort_values(sort_cols).reset_index(drop=True)
        else:
            games = games.reset_index(drop=True)
        games["game_id"] = range(1, len(games) + 1)

    if "sport" not in games.columns:
        games["sport"] = "MLB"

    teams = pd.read_parquet(TEAMS_PATH)

    games["date"] = pd.to_datetime(games["date"])

    # Merge home team metadata
    home_meta = teams.rename(columns={
        "team_id": "home_team_id",
        "team_name": "home_team_name",
        "league": "home_league"
    })
    df = games.merge(home_meta, on=["season", "home_team_id"], how="left")

    # Merge away team metadata
    away_meta = teams.rename(columns={
        "team_id": "away_team_id",
        "team_name": "away_team_name",
        "league": "away_league"
    })
    df = df.merge(away_meta, on=["season", "away_team_id"], how="left")

    df = add_rest_features(df)

    cols = [
        "sport","season","game_id","date",
        "home_team_id","away_team_id",
        "home_team_name","away_team_name",
        "home_score","away_score","home_win",
        "home_league","away_league",
        "home_days_rest","away_days_rest",
        "home_is_b2b","away_is_b2b"
    ]

    df = df[cols].sort_values(["season","date","game_id"]).reset_index(drop=True)

    print(df.head())
    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"Wrote model_input to {OUTPUT_PATH}")

if __name__ == "__main__":
    build_mlb_model_input()