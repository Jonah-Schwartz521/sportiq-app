#!/usr/bin/env python3
"""
Quick verification of NBA scheduled rows to ensure no fake 0-0 scores.

Usage:
  python model/scripts/verify_nba_future_scores.py 2025-12-14
If no date arg provided, defaults to today.
"""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[2]
PARQUET = ROOT / "model" / "data" / "processed" / "nba" / "nba_games_with_scores.parquet"


def main() -> None:
    if not PARQUET.exists():
        raise SystemExit(f"Missing parquet at {PARQUET}")

    target = sys.argv[1] if len(sys.argv) > 1 else datetime.utcnow().date().isoformat()
    df = pd.read_parquet(PARQUET)
    df["date"] = pd.to_datetime(df["date"], errors="coerce").dt.date
    status = df.get("status", "").astype(str).str.upper()

    # Scheduled rows for target date
    mask = (df["date"].astype(str) == target) & status.isin(["SCHEDULED", "PRE"])
    subset = df[mask].copy()
    subset["home_pts"] = pd.to_numeric(subset.get("home_pts"), errors="coerce")
    subset["away_pts"] = pd.to_numeric(subset.get("away_pts"), errors="coerce")

    null_scores = subset["home_pts"].isna() & subset["away_pts"].isna()
    zero_scores = (subset["home_pts"] == 0) & (subset["away_pts"] == 0)

    print(f"Target date: {target}")
    print(f"Scheduled rows: {len(subset)}")
    print(f"  null scores: {int(null_scores.sum())}")
    print(f"  zero scores: {int(zero_scores.sum())}")
    print("Sample (up to 5 rows):")
    print(subset[["date", "home_team", "away_team", "home_pts", "away_pts", "status"]].head(5))


if __name__ == "__main__":
    main()
