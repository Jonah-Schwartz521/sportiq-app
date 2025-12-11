import pandas as pd
from pathlib import Path

# ---------- CONFIG ----------
SPORT_ID_NHL = 4  # keep consistent with your other sport IDs

# Paths relative to this script: model/scripts/...
ROOT_DIR = Path(__file__).resolve().parents[1]  # .../model
RAW_NHL_DIR = ROOT_DIR / "data" / "raw" / "nhl"
PROC_NHL_DIR = ROOT_DIR / "data" / "processed" / "nhl"

RAW_FILE = RAW_NHL_DIR / "moneypuck_nhl_games.csv"


def build_nhl_from_moneypuck() -> None:
    """
    Read MoneyPuck team-level game data and produce:
      - nhl_team_lookup.csv
      - nhl_games_with_scores.parquet
      - nhl_games_for_app.parquet

    This script ONLY reads/writes inside model/data/{raw,processed}/nhl
    so it cannot affect NBA/MLB/NFL.
    """
    PROC_NHL_DIR.mkdir(parents=True, exist_ok=True)

    if not RAW_FILE.exists():
        raise FileNotFoundError(f"Expected NHL raw file at: {RAW_FILE}")

    df = pd.read_csv(RAW_FILE)
    print(f"Loaded {len(df):,} rows from MoneyPuck NHL data")

    # ------------------------------------------------------------------
    # 1) Filter to one row per team per game: Team Level + situation 'all'
    # ------------------------------------------------------------------
    df = df[(df["position"] == "Team Level") & (df["situation"] == "all")].copy()

    # ------------------------------------------------------------------
    # 2) Build team lookup (simple numeric IDs for NHL teams)
    # ------------------------------------------------------------------
    teams = (
        df[["playerTeam"]]
        .drop_duplicates()
        .sort_values("playerTeam")
        .reset_index(drop=True)
    )
    teams["team_id"] = teams.index + 1  # 1,2,3,... for NHL only
    teams["team_code"] = teams["playerTeam"]
    # For now we just use the code as the display name; you can swap later
    teams["team_name"] = teams["playerTeam"]

    team_lookup_path = PROC_NHL_DIR / "nhl_team_lookup.csv"
    teams[["team_id", "team_code", "team_name"]].to_csv(
        team_lookup_path, index=False
    )

    code_to_id = dict(zip(teams["team_code"], teams["team_id"]))

    # ------------------------------------------------------------------
    # 3) Collapse to one row per GAME: use only the HOME team rows
    #    These rows already contain opponent + full score.
    # ------------------------------------------------------------------
    home = df[df["home_or_away"] == "HOME"].copy()

    # Normalize datetime - MoneyPuck uses YYYYMMDD format (e.g., 20081004)
    home["game_datetime"] = pd.to_datetime(
        home["gameDate"], format="%Y%m%d", errors="coerce"
    )

    # Basic identifiers
    home["game_id"] = home["gameId"]
    home["season_int"] = home["season"].astype(int)

    # Create a readable season label, e.g. "2008-2009_NHL"
    home["season_label"] = home["season_int"].apply(
        lambda s: f"{s}-{s+1}_NHL"
    )

    # Team codes
    home["home_team_code"] = home["playerTeam"]
    home["away_team_code"] = home["opposingTeam"]

    # Map to numeric team IDs
    home["home_team_id"] = home["home_team_code"].map(code_to_id)
    home["away_team_id"] = home["away_team_code"].map(code_to_id)

    # Scores
    home["home_score"] = home["goalsFor"].astype("Int64")
    home["away_score"] = home["goalsAgainst"].astype("Int64")

    # Flags
    home["sport_id"] = SPORT_ID_NHL
    home["sport_name"] = "NHL"
    home["is_playoff"] = home["playoffGame"].fillna(0).astype(bool)

    # ------------------------------------------------------------------
    # 4) Build the core NHL games table (with scores)
    # ------------------------------------------------------------------
    games_cols = [
        "game_id",
        "sport_id",
        "sport_name",
        "season_int",
        "season_label",
        "game_datetime",
        "home_team_id",
        "home_team_code",
        "home_score",
        "away_team_id",
        "away_team_code",
        "away_score",
        "is_playoff",
    ]

    games = home[games_cols].rename(
        columns={
            "season_int": "season",
            "home_team_code": "home_team_name",  # these are abbreviations
            "away_team_code": "away_team_name",
        }
    )

    # Save intermediate table (useful for debugging / future analysis)
    games_with_scores_path = PROC_NHL_DIR / "nhl_games_with_scores.parquet"
    games.to_parquet(games_with_scores_path, index=False)

    # ------------------------------------------------------------------
    # 5) Build the app-ready file (games_for_app)
    #    We add an is_future_game flag; otherwise schema is the same.
    # ------------------------------------------------------------------
    games_for_app = games.copy()
    now_utc = pd.Timestamp.utcnow().tz_localize(None)
    games_for_app["is_future_game"] = games_for_app["game_datetime"] > now_utc

    games_for_app_path = PROC_NHL_DIR / "nhl_games_for_app.parquet"
    games_for_app.to_parquet(games_for_app_path, index=False)

    print("Wrote:")
    print(f"  - {team_lookup_path}")
    print(f"  - {games_with_scores_path}")
    print(f"  - {games_for_app_path}")


if __name__ == "__main__":
    build_nhl_from_moneypuck()
