# model/scripts/build_future_games.py

from pathlib import Path
import sys
import pandas as pd

# Ensure project root is on sys.path so `import model...` works when running this file directly
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import PROCESSED_DIR

RAW_ROOT = PROCESSED_DIR.parent / "raw" / "NBA_schedule_results"

# Only treat these as "future" schedule seasons
FUTURE_SEASONS = {"2025-26_NBA"}

def load_future_schedule() -> pd.DataFrame:
    all_rows = []

    for season_dir in RAW_ROOT.iterdir():
        if not season_dir.is_dir():
            continue

        if season_dir.name not in FUTURE_SEASONS:
            continue

        for xls_path in season_dir.glob("*.xls"):
            print(f"  Loading {season_dir.name}/{xls_path.name} ...")
            # Some of these schedule files are HTML tables saved with a .xls extension.
            # First try regular Excel parsing; if that fails, fall back to read_html.
            try:
                df_raw = pd.read_excel(xls_path)
            except Exception:
                tables = pd.read_html(xls_path)
                if not tables:
                    raise RuntimeError(f"No tables found in {xls_path}")
                df_raw = tables[0]

            # attach the season label, e.g. "2024-25_NBA"
            df_raw["_season_label"] = season_dir.name
            all_rows.append(df_raw)

    if not all_rows:
        raise RuntimeError("No future schedule rows loaded. Check FUTURE_SEASONS / folders.")

    raw = pd.concat(all_rows, ignore_index=True)
    print("Raw future schedule shape:", raw.shape)

    # Normalize date (these strings look like "Thu, Dec 1, 2016")
    date_str = raw["Date"].astype(str).str.split(",", n=1).str[1].str.strip()
    dates = pd.to_datetime(date_str, errors="coerce")

    schedule = pd.DataFrame(
        {
            "date": dates.dt.date,
            "start_et": raw["Start (ET)"],
            "away_team": raw["Visitor/Neutral"],
            "home_team": raw["Home/Neutral"],
            "arena": raw["Arena"],
            "season": raw["_season_label"].str.replace("_NBA", "", regex=False),
        }
    )

    # Drop header / invalid rows
    schedule = schedule.dropna(subset=["date", "away_team", "home_team"])
    print("Cleaned future schedule shape:", schedule.shape)

    return schedule


def main():
    # 1) Load existing processed games (with scores)
    base_path = PROCESSED_DIR / "processed_games_with_scores.parquet"
    print("Loading base games from:", base_path)
    base = pd.read_parquet(base_path)
    print("Base shape:", base.shape)
    print("Base seasons:", sorted(base["season"].unique()))
    print("Base date range:", base["date"].min(), "→", base["date"].max())

    # Ensure date is datetime.date for consistent comparisons
    base["date"] = pd.to_datetime(base["date"]).dt.date

    # 2) Load future schedule
    future = load_future_schedule()

    # Only keep games after the last historical date to avoid duplicates
    last_hist_date = base["date"].max()
    future = future[future["date"] > last_hist_date].copy()
    print("Future rows AFTER filtering > last_hist_date:", future.shape)

    # 3) Align columns to base schema

    # existing columns in base
    base_cols = list(base.columns)

    # We'll fill missing modeling features with NaN/None for now.
    for col in base_cols:
        if col in {"game_id", "date", "start_et", "away_team", "home_team", "arena", "season"}:
            continue
        if col not in future.columns:
            future[col] = None

    # 4) Assign new game_ids
    max_game_id = int(base["game_id"].max())
    future["game_id"] = range(max_game_id + 1, max_game_id + 1 + len(future))

    # Reorder columns to match base
    future = future[base_cols]

    print("Future sample:")
    print(future.head().to_dict(orient="records"))

    # 5) Concatenate and write out
    combined = pd.concat([base, future], ignore_index=True)
    combined["date"] = pd.to_datetime(combined["date"])
    out_path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
    combined.to_parquet(out_path, index=False)

    print("Combined shape:", combined.shape)
    print("Combined seasons:", sorted(combined["season"].unique()))
    print("Combined date range:", combined["date"].min(), "→", combined["date"].max())
    print("Wrote:", out_path)


if __name__ == "__main__":
    main()