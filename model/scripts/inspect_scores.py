import pandas as pd
from pathlib import Path
import sys

# Make sure the `model` package root is on sys.path so we can import src.*
ROOT = Path(__file__).resolve().parents[1]  # /Volumes/.../sportiq-app/model
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

# Reuse your existing paths helper
from src.paths import PROCESSED_DIR

# This is where your season folders live:
# /model/data/raw/NBA_schedule_results/2016-17_NBA, 2019-20_NBA, etc.
RAW_ROOT = PROCESSED_DIR.parent / "raw" / "NBA_schedule_results"


def main() -> None:
    print("RAW_ROOT:", RAW_ROOT)
    print("Exists?", RAW_ROOT.exists())

    all_rows = []

    # Walk each season folder: 2015-16_NBA, 2016-17_NBA, ...
    for season_dir in RAW_ROOT.iterdir():
        if not season_dir.is_dir():
            # skip .DS_Store or any weird files
            continue

        season_label = season_dir.name  # e.g. "2015-16_NBA"
        print(f"\n=== Season folder: {season_label} ===")

        # Grab both .xls and .xlsx just in case
        excel_files = list(season_dir.glob("*.xls")) + list(
            season_dir.glob("*.xlsx")
        )
        if not excel_files:
            print("  (no Excel files found here)")
            continue

        for xls_path in excel_files:
            print(f"  Loading {xls_path.name} ...")
            # Many Basketball-Reference ".xls" downloads are actually HTML tables.
            # First try read_excel; if the format/engine cannot be determined,
            # fall back to read_html.
            try:
                df_raw = pd.read_excel(xls_path)
            except Exception:
                df_list = pd.read_html(xls_path)
                if not df_list:
                    print("   !! read_html found no tables, skipping this file")
                    continue
                df_raw = df_list[0]

            # Show the first few column names so we can spot scores
            print(
                "   → columns:",
                df_raw.columns.tolist()[:12],
            )

            # Keep all rows but tag which season they came from
            all_rows.append(df_raw.assign(_season=season_label))

    print("\nNumber of monthly files loaded:", len(all_rows))
    if not all_rows:
        print("No data loaded – check that the folders actually contain .xls/.xlsx files.")
        return

    # Combine everything into one big frame
    raw = pd.concat(all_rows, ignore_index=True)
    print("Raw combined shape:", raw.shape)

    # Show all columns so we can identify home/away scores
    print("\nAll columns:")
    print(raw.columns.tolist())

    # Show a couple of sample rows:
    print("\nSample rows:")
    print(raw.head(5).to_dict(orient="records"))

    # ---- Build a clean scores table from raw ----
    scores = raw.rename(
        columns={
            "Date": "date",
            "Visitor/Neutral": "away_team",
            "Home/Neutral": "home_team",
            "PTS": "away_score",
            "PTS.1": "home_score",
        }
    )[["date", "away_team", "home_team", "away_score", "home_score"]].copy()

    # Normalize date to just the calendar date (not weekday string)
    scores["date"] = pd.to_datetime(scores["date"]).dt.date

    # Sometimes Basketball-Reference repeats rows (e.g., rescheduled games)
    scores = scores.drop_duplicates(
        subset=["date", "home_team", "away_team"], keep="first"
    )

    print("\nScores frame shape:", scores.shape)
    print("Scores sample:", scores.head(5).to_dict(orient="records"))

    # ---- Load your processed modeling table ----
    processed_path = PROCESSED_DIR / "processed_games_b2b_model.parquet"
    print("\nLoading processed parquet from:", processed_path)
    processed = pd.read_parquet(processed_path)
    print("Processed shape BEFORE merge:", processed.shape)

    # Create a join key on the processed side
    processed["date_key"] = pd.to_datetime(processed["date"]).dt.date

    # Also create a join key in scores
    scores["date_key"] = scores["date"]

    # Merge scores into the processed table
    merged = processed.merge(
        scores[["date_key", "home_team", "away_team", "home_score", "away_score"]],
        on=["date_key", "home_team", "away_team"],
        how="left",
    )

    # Drop helper key column
    merged = merged.drop(columns=["date_key"])

    print("\nProcessed shape AFTER merge:", merged.shape)
    print("Non-null home_score rows:", merged["home_score"].notna().sum())
    print("Non-null away_score rows:", merged["away_score"].notna().sum())

    # Save to a new parquet so we don't destroy the original yet
    out_path = PROCESSED_DIR / "processed_games_with_scores.parquet"
    merged.to_parquet(out_path, index=False)
    print("\nWrote merged file with scores to:", out_path)


if __name__ == "__main__":
    main()
