#!/usr/bin/env python3
"""
Restore NBA historical seasons (2015–present) by merging a historical source
parquet with the current nba_games_with_scores.parquet.

Inputs:
- Current: model/data/processed/nba/nba_games_with_scores.parquet (keeps 2025)
- Historical source: model/data/processed/games_with_scores_and_future.parquet (NBA rows)

Outputs:
- Overwrites model/data/processed/nba/nba_games_with_scores.parquet with merged data
  (seasons 2015–2024 from historical source, 2025 from current).

Safety guards:
- Abort if required seasons 2015..2025 are not present after merge.
- Abort if seasons drop vs current or rows drop >5% vs current.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Optional

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
CURRENT_PATH = ROOT / "model" / "data" / "processed" / "nba" / "nba_games_with_scores.parquet"
HIST_SOURCE_PATH = ROOT / "model" / "data" / "processed" / "games_with_scores_and_future.parquet"
TARGET_YEAR = 2025
REQUIRED_SEASONS = set(range(2015, TARGET_YEAR + 1))


def parse_season(val: object) -> Optional[int]:
    """
    Convert season strings like '2015-16_NBA' or numeric to start year int.
    """
    if val is None:
        return None
    s = str(val)
    # If it's purely numeric, take it directly
    if s.isdigit():
        return int(s)
    # Expect formats like '2015-16_NBA' or '2015-16'
    try:
        return int(s[:4])
    except Exception:
        return None


def quality(row: pd.Series) -> int:
    status = str(row.get("status", "")).upper()
    has_scores = pd.notna(row.get("home_pts")) and pd.notna(row.get("away_pts"))
    if status == "FINAL" and has_scores:
        return 3
    if has_scores:
        return 2
    if status in ("IN_PROGRESS", "LIVE"):
        return 1
    return 0


def dedupe(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["__quality"] = df.apply(quality, axis=1)
    df = df.sort_values(
        ["season", "date", "home_team", "away_team", "__quality"],
        ascending=[True, True, True, True, False],
    )
    df = df.drop_duplicates(subset=["season", "date", "home_team", "away_team"], keep="first")
    return df.drop(columns="__quality")


def load_current() -> pd.DataFrame:
    if not CURRENT_PATH.exists():
        raise SystemExit(f"Current parquet missing: {CURRENT_PATH}")
    df = pd.read_parquet(CURRENT_PATH)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["season"] = df["season"].apply(parse_season).astype("Int64")
    return df


def load_historical_source() -> pd.DataFrame:
    if not HIST_SOURCE_PATH.exists():
        raise SystemExit(f"Historical source missing: {HIST_SOURCE_PATH}")
    df = pd.read_parquet(HIST_SOURCE_PATH)
    # Filter NBA rows
    if "league" in df.columns:
        df = df[df["league"].astype(str).str.upper() == "NBA"]
    df = df.rename(
        columns={
            "home_team_name": "home_team",
            "away_team_name": "away_team",
            "home_score": "home_pts",
            "away_score": "away_pts",
        }
    )
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df["season"] = df["season"].apply(parse_season).astype("Int64")
    return df


def main() -> None:
    current = load_current()
    hist = load_historical_source()

    print("Current seasons:", current["season"].value_counts(dropna=False).head(20))
    print("Historical source seasons:", hist["season"].value_counts(dropna=False).head(20))

    hist_past = hist[hist["season"].notna() & (hist["season"] < TARGET_YEAR)]
    current_target = current[current["season"] == TARGET_YEAR]
    current_other = current[current["season"] != TARGET_YEAR]

    # Drop duplicate column labels before aligning
    hist_past = hist_past.loc[:, ~hist_past.columns.duplicated()]
    current_target = current_target.loc[:, ~current_target.columns.duplicated()]

    # Align columns between frames to avoid duplicate/extra columns
    current_other = current_other.loc[:, ~current_other.columns.duplicated()]

    all_cols = sorted(set(hist_past.columns) | set(current_target.columns) | set(current_other.columns))
    hist_past = hist_past.reindex(columns=all_cols)
    current_target = current_target.reindex(columns=all_cols)
    current_other = current_other.reindex(columns=all_cols)

    merged = pd.concat([hist_past, current_other, current_target], ignore_index=True)
    merged = dedupe(merged)

    seasons_after = set(merged["season"].dropna().unique().tolist())
    if not REQUIRED_SEASONS.issubset(seasons_after):
        raise SystemExit(f"Refusing to write: missing seasons {REQUIRED_SEASONS - seasons_after}")

    # Safety vs current
    seasons_before = current["season"].nunique(dropna=True)
    seasons_after_count = merged["season"].nunique(dropna=True)
    if seasons_after_count < seasons_before:
        raise SystemExit(f"Refusing to write: seasons dropped from {seasons_before} to {seasons_after_count}")
    if len(merged) < len(current) * 0.95:
        raise SystemExit(f"Refusing to write: rows fell from {len(current)} to {len(merged)}")

    merged = merged.sort_values("date")
    merged.to_parquet(CURRENT_PATH, index=False)

    final_seasons = merged["season"].value_counts(dropna=False)
    final_final = (merged["status"] == "FINAL").sum() if "status" in merged.columns else 0
    print("✅ Restored NBA parquet")
    print(f"Rows: {len(merged)}  FINAL rows: {final_final}")
    print("Seasons:", final_seasons.head(20))
    print("Max date:", merged["date"].max())


if __name__ == "__main__":
    main()
