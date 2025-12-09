import json
import pathlib

import requests
import pandas as pd

BASE_DIR = pathlib.Path(__file__).resolve().parents[1]  # /model
RAW_PATH = BASE_DIR / "data" / "raw" / "nhl_games_all_regular.json"
PARQUET_PATH = BASE_DIR / "data" / "processed" / "nhl_games_all_regular.parquet"


def fetch_all_nhl_regular_season_games() -> None:
    """Fetch all NHL regular-season games from the public stats API
    and save both the raw JSON and a trimmed parquet with tidy columns.
    """

    url = (
        "https://api.nhle.com/stats/rest/en/game"
        "?cayenneExp=gameType=2"  # 2 = regular season
        "&start=0&limit=-1"
    )

    print(f"Requesting: {url}")
    resp = requests.get(url)
    resp.raise_for_status()

    payload = resp.json()
    games = payload.get("data", [])
    print(f"Fetched {len(games)} games")

    # Save raw JSON (for debugging / reproducibility)
    RAW_PATH.parent.mkdir(parents=True, exist_ok=True)
    with RAW_PATH.open("w") as f:
        json.dump(payload, f)

    # Convert to DataFrame
    df = pd.DataFrame(games)
    if df.empty:
        print("No games returned from NHL API; nothing to write.")
        return

    # Optional: keep only columns you care about (nice for later joins)
    keep_cols = [
        "id",
        "season",
        "gameDate",
        "easternStartTime",
        "gameType",
        "gameNumber",
        "homeTeamId",
        "visitingTeamId",
        "homeScore",
        "visitingScore",
        "gameStateId",
        "gameScheduleStateId",
    ]
    df = df[keep_cols]

    # Rename to match your unified games schema
    df = df.rename(
        columns={
            "id": "game_id",
            "gameDate": "game_date",
            "easternStartTime": "start_time_eastern",
            "homeTeamId": "home_team_id",
            "visitingTeamId": "away_team_id",
            "homeScore": "home_score",
            "visitingScore": "away_score",
        }
    )

    # Parse dates and sort for sanity
    df["game_date"] = pd.to_datetime(df["game_date"], errors="coerce")
    df = df.sort_values(["game_date", "game_id"]).reset_index(drop=True)

    # Add league + human-readable season label
    df["league"] = "NHL"

    # season comes as 19171918, 20242025, etc → make "1917-18_NHL"
    df["season_label"] = (
        df["season"].astype(str).str.slice(0, 4)
        + "-"
        + df["season"].astype(str).str.slice(4, 8).str[-2:]
        + "_NHL"
    )

    # Convenience flags used by later scripts: do we have final scores yet?
    df["has_final_score"] = df["home_score"].notna() & df["away_score"].notna()
    df["is_future_game"] = ~df["has_final_score"]

    PARQUET_PATH.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(PARQUET_PATH, index=False)
    print(f"Saved parquet to {PARQUET_PATH}")
    print("Sample:")
    print(df.head())


if __name__ == "__main__":
    fetch_all_nhl_regular_season_games()