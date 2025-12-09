import pandas as pd
from pathlib import Path


# Base project dir: /Volumes/easystore/Projects/sportiq-app/model
BASE_DIR = Path(__file__).resolve().parents[1]

# Input: what you just fetched (all regular-season NHL games)
NHL_SOURCE_PATH = BASE_DIR / "data" / "processed" / "nhl_games_all_regular.parquet"

# Output: cleaner file that’s ready to join into your master games table
NHL_OUT_PATH = BASE_DIR / "data" / "processed" / "nhl_games_with_scores.parquet"


def build_nhl_games_for_app() -> None:
    print(f"Loading NHL source from: {NHL_SOURCE_PATH}")
    df = pd.read_parquet(NHL_SOURCE_PATH)
    print("Raw shape:", df.shape)
    print("Raw columns:", list(df.columns))

    # ------------------------------------------------------------------
    # 1) Limit to modern seasons so you are not dragging 1917 data around
    #    Adjust the cutoff if you want more history.
    # ------------------------------------------------------------------
    df = df[df["season"] >= 20152016].copy()
    print("After season filter (>= 2015-16):", df.shape)

    # ------------------------------------------------------------------
    # 2) Standardize date/time columns
    # ------------------------------------------------------------------
    # game_date came from gameDate; start_time_eastern from easternStartTime
    df["game_date"] = pd.to_datetime(df["game_date"])
    df["start_time_eastern"] = pd.to_datetime(df["start_time_eastern"])

    # ------------------------------------------------------------------
    # 3) Add helper flags for your app logic
    # ------------------------------------------------------------------
    # From the API docs / behavior:
    # gameStateId:
    #   1 = Scheduled
    #   2 = Pre-game
    #   3/4/5 = In-progress states
    #   6/7 = Final / Over / Completed
    df["has_final_score"] = df["gameStateId"].isin([6, 7])
    df["is_future_game"] = ~df["has_final_score"]

    # ------------------------------------------------------------------
    # 4) Build a clean column set in a sensible order
    #    This is your NHL-specific "games with scores" table.
    #    We keep some raw IDs for debugging.
    # ------------------------------------------------------------------
    cols_order = [
        "league",
        "season_label",
        "season",
        "game_id",
        "game_date",
        "start_time_eastern",
        "home_team_id",
        "away_team_id",
        "home_score",
        "away_score",
        "gameType",
        "gameNumber",
        "gameStateId",
        "gameScheduleStateId",
        "has_final_score",
        "is_future_game",
    ]

    # Some safety in case anything is missing
    existing_cols = [c for c in cols_order if c in df.columns]
    missing_cols = [c for c in cols_order if c not in df.columns]
    if missing_cols:
        print("Warning: these expected columns are missing and will be skipped:", missing_cols)

    df = df[existing_cols]

    # ------------------------------------------------------------------
    # 5) Save it
    # ------------------------------------------------------------------
    NHL_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(NHL_OUT_PATH, index=False)
    print(f"Saved cleaned NHL games to: {NHL_OUT_PATH}")
    print("Final columns:", list(df.columns))
    print("Done ✅")


if __name__ == "__main__":
    build_nhl_games_for_app()