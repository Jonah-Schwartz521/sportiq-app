# model/scripts/build_mlb_games.py

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pandas as pd
import zipfile

# -------------------------------------------------------------------
# Project paths
# -------------------------------------------------------------------

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

# Uses the paths you already added in model/src/paths.py
from model.src.paths import (  # type: ignore  # noqa
    MLB_RAW_DIR,
    MLB_PROCESSED_DIR,
)

RETRO_DIR = MLB_RAW_DIR / "retrosheet"
OUTPUT_PATH = MLB_PROCESSED_DIR / "mlb_games_with_scores.parquet"


# -------------------------------------------------------------------
# Helpers
# -------------------------------------------------------------------

def _find_inner_log_name(zf: zipfile.ZipFile) -> str:
    """
    Retrosheet ZIPs usually contain a single game log file like 'GL2023.TXT' or 'gl2023.txt'.
    This finds that inner file name.
    """
    candidates: List[str] = [
        name
        for name in zf.namelist()
        if name.lower().endswith(".txt") or name.lower().endswith(".csv")
    ]
    if not candidates:
        raise RuntimeError(f"No .txt/.csv game log found in {zf.filename}")
    # Usually there is exactly one; take the first
    return candidates[0]


def load_retro_year(zip_path: Path) -> pd.DataFrame:
    """
    Load a single Retrosheet game log ZIP (e.g. gl2015.zip) into a DataFrame
    with a small, clean set of columns.
    """
    print(f"Loading {zip_path.name} ...")

    with zipfile.ZipFile(zip_path, "r") as zf:
        inner_name = _find_inner_log_name(zf)
        with zf.open(inner_name) as f:
            # Retrosheet game logs are comma-separated with no header.
            df_raw = pd.read_csv(f, header=None)

    if df_raw.empty:
        print(f"  WARNING: {zip_path.name} is empty; skipping.")
        return pd.DataFrame()

    # Retrosheet game log core fields (0-based indices):
    #  0: date (YYYYMMDD)
    #  1: game number (0, 1, 2 for doubleheaders)
    #  3: visitor team ID (Retrosheet code)
    #  6: home team ID (Retrosheet code)
    #  9: visitor score
    # 10: home score
    # This subset is enough for a first-pass games table.
    keep_cols = {
        0: "raw_date",
        1: "game_num",
        3: "away_team_id",
        6: "home_team_id",
        9: "away_score",
        10: "home_score",
    }

    df = df_raw[list(keep_cols.keys())].rename(columns=keep_cols)

    # Convert date
    df["date"] = pd.to_datetime(df["raw_date"].astype(str), format="%Y%m%d", errors="coerce")
    df["season"] = df["date"].dt.year

    # Core derived fields
    df["sport"] = "MLB"
    df["game_status"] = "FINAL"
    df["home_win"] = (df["home_score"] > df["away_score"]).astype("Int64")

    # Build an event_id that is stable and readable.
    # Example: 2023_MLB_NYY@BOS_2023-07-15_G1
    df["event_id"] = (
        df["season"].astype(str)
        + "_MLB_"
        + df["away_team_id"].astype(str)
        + "@"
        + df["home_team_id"].astype(str)
        + "_"
        + df["date"].dt.strftime("%Y-%m-%d")
        + "_G"
        + df["game_num"].astype(str)
    )

    # Reorder columns to something tidy
    df = df[
        [
            "event_id",
            "sport",
            "season",
            "date",
            "game_status",
            "home_team_id",
            "away_team_id",
            "home_score",
            "away_score",
            "home_win",
            "game_num",
        ]
    ]

    print(
        f"  Loaded {len(df):,} games from {zip_path.name} "
        f"({df['season'].min()}–{df['season'].max()})"
    )
    return df


def build_mlb_games() -> pd.DataFrame:
    """
    Load all Retrosheet game log ZIPs under model/data/raw/mlb/retrosheet,
    combine them, and write a clean games parquet.
    """
    if not RETRO_DIR.exists():
        raise RuntimeError(
            f"Retrosheet directory not found: {RETRO_DIR}\n"
            "Make sure you downloaded the glYYYY.zip files into this folder."
        )

    zip_files = sorted(RETRO_DIR.glob("gl*.zip"))
    if not zip_files:
        raise RuntimeError(
            f"No 'glYYYY.zip' files found in {RETRO_DIR}. "
            "Download Retrosheet game log ZIPs (e.g., gl2015.zip) into this folder."
        )

    print("=== Building MLB games from Retrosheet logs ===")
    print(f"Found {len(zip_files)} game-log ZIPs.\n")

    dfs: List[pd.DataFrame] = []
    for zp in zip_files:
        df_year = load_retro_year(zp)
        if not df_year.empty:
            dfs.append(df_year)

    if not dfs:
        raise RuntimeError("No data loaded from any ZIPs — nothing to write.")

    games = pd.concat(dfs, ignore_index=True)

    # Basic sanity / sorting
    games = games.dropna(subset=["date"]).sort_values(["date", "event_id"]).reset_index(drop=True)

    print("\n=== Summary ===")
    print(f"Total games: {len(games):,}")
    print("Seasons:", games["season"].min(), "→", games["season"].max())
    print("Sample:")
    print(games.head())

    # Ensure output directory exists
    MLB_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    games.to_parquet(OUTPUT_PATH, index=False)

    print(f"\nWrote MLB games parquet to:\n  {OUTPUT_PATH}")
    return games


def main() -> None:
    build_mlb_games()


if __name__ == "__main__":
    main()