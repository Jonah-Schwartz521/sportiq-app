#!/usr/bin/env python
"""
Build a UFC fights parquet for the app by processing the ufc-master.csv dataset.

Input:
  - model/data/processed/ufc/ufc-master.csv (historical UFC fights through 2024)

Output:
  - model/data/processed/ufc/ufc_fights_for_app.parquet

This script normalizes UFC fight data into the same schema as other sports
so it can be loaded into the unified events API.
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, time

import pandas as pd

# Set up imports from model/src
MODEL_ROOT = Path(__file__).resolve().parents[1]
if str(MODEL_ROOT) not in sys.path:
    sys.path.append(str(MODEL_ROOT))

# Define paths
UFC_DIR = MODEL_ROOT / "data" / "processed" / "ufc"
INPUT_CSV = UFC_DIR / "ufc-master.csv"
OUTPUT_PARQUET = UFC_DIR / "ufc_fights_for_app.parquet"


def normalize_ufc_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize UFC fight data into the app's unified schema.

    Maps:
    - RedFighter -> home_team_name
    - BlueFighter -> away_team_name
    - Winner ('Red'/'Blue') -> home_score/away_score (1/0)
    - Date -> fight_datetime (with default 19:00 ET time)
    """
    out = df.copy()

    # 1. Parse date and add default time (7:00 PM ET for UFC main cards)
    out["Date"] = pd.to_datetime(out["Date"], errors="coerce")
    out = out.dropna(subset=["Date"])

    # Add default time (19:00 = 7 PM ET, typical UFC main card start)
    out["fight_datetime"] = out["Date"].apply(
        lambda d: datetime.combine(d.date(), time(19, 0)) if pd.notna(d) else None
    )
    out["fight_datetime"] = pd.to_datetime(out["fight_datetime"], errors="coerce")

    # 2. Map fighters to home/away (Red=home, Blue=away by convention)
    out["home_team_name"] = out["RedFighter"].astype(str).str.strip()
    out["away_team_name"] = out["BlueFighter"].astype(str).str.strip()

    # Drop rows missing either fighter name
    out = out[
        (out["home_team_name"] != "") &
        (out["home_team_name"] != "nan") &
        (out["away_team_name"] != "") &
        (out["away_team_name"] != "nan")
    ]

    # 3. Map winner to scores (1 for winner, 0 for loser)
    # Winner column contains "Red" or "Blue"
    def compute_scores(row):
        winner = str(row["Winner"]).strip()
        if winner == "Red":
            return 1, 0  # home wins
        elif winner == "Blue":
            return 0, 1  # away wins
        else:
            # Draw or no contest (shouldn't happen in this dataset, but defensive)
            return None, None

    out[["home_score", "away_score"]] = out.apply(
        lambda row: pd.Series(compute_scores(row)), axis=1
    )

    # Create home_win indicator (1 if home won, 0 if away won, NaN if unknown)
    out["home_win"] = out["home_score"].astype("Int64")  # Nullable int

    # 4. Add sport/league labels
    out["sport"] = "UFC"
    out["league"] = "UFC"

    # 5. Keep useful metadata columns for UI
    metadata_cols = []
    if "Location" in out.columns:
        out["location"] = out["Location"]
        metadata_cols.append("location")
    if "WeightClass" in out.columns:
        out["weight_class"] = out["WeightClass"]
        metadata_cols.append("weight_class")
    if "TitleBout" in out.columns:
        out["title_bout"] = out["TitleBout"]
        metadata_cols.append("title_bout")
    if "Gender" in out.columns:
        out["gender"] = out["Gender"]
        metadata_cols.append("gender")
    if "Finish" in out.columns:
        out["method"] = out["Finish"]  # e.g., "KO", "SUB", "U-DEC"
        metadata_cols.append("method")
    if "FinishDetails" in out.columns:
        out["finish_details"] = out["FinishDetails"]
        metadata_cols.append("finish_details")
    if "FinishRound" in out.columns:
        out["finish_round"] = out["FinishRound"]
        metadata_cols.append("finish_round")
    if "FinishRoundTime" in out.columns:
        out["finish_time"] = out["FinishRoundTime"]
        metadata_cols.append("finish_time")
    if "NumberOfRounds" in out.columns:
        out["scheduled_rounds"] = out["NumberOfRounds"]
        metadata_cols.append("scheduled_rounds")

    # 6. Select final columns for output
    keep_cols = [
        "fight_datetime",
        "home_team_name",
        "away_team_name",
        "home_score",
        "away_score",
        "home_win",
        "sport",
        "league",
    ] + metadata_cols

    out = out[keep_cols]

    # 7. Drop duplicates
    # De-dupe by fight_datetime + fighters (some events may have rematches)
    out = out.drop_duplicates(
        subset=["fight_datetime", "home_team_name", "away_team_name"],
        keep="first"
    )

    # 8. Sort by date
    out = out.sort_values("fight_datetime").reset_index(drop=True)

    return out


def main() -> None:
    """Load, normalize, and save UFC fight data."""

    if not INPUT_CSV.exists():
        raise SystemExit(f"UFC input CSV not found at {INPUT_CSV}")

    print(f"Reading UFC data from: {INPUT_CSV}")
    raw_df = pd.read_csv(INPUT_CSV)
    print(f"Loaded {len(raw_df):,} raw fights")

    # Normalize to app schema
    normalized_df = normalize_ufc_data(raw_df)

    # Ensure output directory exists
    UFC_DIR.mkdir(parents=True, exist_ok=True)

    # Write to parquet
    normalized_df.to_parquet(OUTPUT_PARQUET, index=False)

    # Print summary
    print(f"\n✅ Wrote UFC fights parquet to {OUTPUT_PARQUET}")
    print(f"Total rows: {len(normalized_df):,}")
    print(
        f"Date range: {normalized_df['fight_datetime'].min().date()} → "
        f"{normalized_df['fight_datetime'].max().date()}"
    )

    # Count fights with known outcomes
    known_outcomes = normalized_df["home_win"].notna().sum()
    missing_outcomes = len(normalized_df) - known_outcomes
    print(f"Fights with known winner: {known_outcomes:,}")
    print(f"Fights with missing outcome: {missing_outcomes:,}")

    # Show sample of recent fights
    print("\n=== Sample of recent UFC fights ===")
    recent = normalized_df.tail(5)
    for _, row in recent.iterrows():
        winner = "Red" if row["home_win"] == 1 else "Blue" if row["home_win"] == 0 else "Unknown"
        print(
            f"  {row['fight_datetime'].date()}: "
            f"{row['home_team_name']} vs {row['away_team_name']} "
            f"[{row.get('weight_class', 'N/A')}] → {winner} wins"
        )


if __name__ == "__main__":
    main()
