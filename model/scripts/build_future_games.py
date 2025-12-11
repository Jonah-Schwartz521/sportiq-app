# model/scripts/build_future_games.py

from __future__ import annotations

from pathlib import Path
import sys

import pandas as pd

# Ensure project root is on sys.path so `import model...` works when running this file directly
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import PROCESSED_DIR  # type: ignore  # noqa

RAW_ROOT = PROCESSED_DIR.parent / "raw" / "NBA_schedule_results"


def parse_nba_start_time(raw_time_str) -> str:
    """
    Parse NBA start time from raw schedule.

    Input examples:
      - "7:30p" → "19:30"
      - "8:00" → "20:00"  (assume PM for evening games)
      - "12:00" → "12:00" (noon)
      - "1:00" → "13:00"  (assume PM)
      - "" or NaN → "19:00" (default 7:00 PM)

    Returns: 24-hour time string "HH:MM"
    """
    import re

    # Handle missing/blank times
    if pd.isna(raw_time_str) or str(raw_time_str).strip() == "":
        return "19:00"  # Default to 7:00 PM ET

    time_str = str(raw_time_str).strip().lower()

    # Already has AM/PM indicator
    if 'p' in time_str or 'a' in time_str:
        # Extract just the time part
        match = re.match(r'(\d{1,2}):?(\d{2})?\s*([ap])', time_str)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2)) if match.group(2) else 0
            period = match.group(3)

            if period == 'p' and hour != 12:
                hour += 12
            elif period == 'a' and hour == 12:
                hour = 0

            return f"{hour:02d}:{minute:02d}"

    # No AM/PM - parse and infer
    match = re.match(r'(\d{1,2}):?(\d{2})?', time_str)
    if match:
        hour = int(match.group(1))
        minute = int(match.group(2)) if match.group(2) else 0

        # NBA game time inference rules:
        # - 12:00, 1:00, 2:00, 3:00 → keep as-is (afternoon games)
        # - 6:00 and later → assume PM
        # - 4:00, 5:00 → assume PM (rare but possible)
        # - Everything else 7+ → assume PM

        if hour >= 6 and hour <= 11:
            # Evening games: 6, 7, 8, 9, 10, 11 → PM
            hour += 12
        elif hour == 12:
            # Noon
            pass
        elif 1 <= hour <= 5:
            # Afternoon: 1 PM - 5 PM
            hour += 12

        return f"{hour:02d}:{minute:02d}"

    # Fallback
    return "19:00"

# Only treat these as "future" schedule seasons
FUTURE_SEASONS = {"2024-25_NBA", "2025-26_NBA"}


def load_future_schedule() -> pd.DataFrame:
    """Load the raw 2025-26 schedule from the HTML/.xls files."""
    all_rows: list[pd.DataFrame] = []

    for season_dir in RAW_ROOT.iterdir():
        if not season_dir.is_dir():
            continue

        if season_dir.name not in FUTURE_SEASONS:
            continue

        # Read both .xls and .csv files (for manual patches)
        schedule_files = list(season_dir.glob("*.xls")) + list(season_dir.glob("*.csv"))

        for schedule_path in schedule_files:
            print(f"  Loading {season_dir.name}/{schedule_path.name} ...")

            # Handle CSV files (manual patches)
            if schedule_path.suffix == '.csv':
                # Skip comment lines starting with #
                # index_col=False prevents pandas from using first column as index
                df_raw = pd.read_csv(schedule_path, comment='#', index_col=False)
            else:
                # Handle .xls files (some are HTML tables saved with .xls extension)
                try:
                    df_raw = pd.read_excel(schedule_path)
                except Exception:
                    tables = pd.read_html(schedule_path)
                    if not tables:
                        raise RuntimeError(f"No tables found in {schedule_path}")
                    df_raw = tables[0]

            df_raw["_season_label"] = season_dir.name
            all_rows.append(df_raw)

    if not all_rows:
        raise RuntimeError("No future schedule rows loaded. Check FUTURE_SEASONS / folders.")

    raw = pd.concat(all_rows, ignore_index=True)
    print("Raw future schedule shape:", raw.shape)

    # Normalize date (these strings look like "Thu, Dec 1, 2016")
    date_str = raw["Date"].astype(str).str.split(",", n=1).str[1].str.strip()
    dates = pd.to_datetime(date_str, errors="coerce")

    # Parse and normalize start times
    parsed_start_times = raw["Start (ET)"].apply(parse_nba_start_time)

    schedule = pd.DataFrame(
        {
            "date": dates.dt.date,
            "start_et": parsed_start_times,  # Now in 24-hour "HH:MM" format
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


def build_team_baselines(base: pd.DataFrame) -> pd.DataFrame:
    """
    Build a simple baseline win% per team using all historical games in `base`.

    We compute:
      - total games
      - total wins (home + away)
      - baseline_win_pct  (wins / games)

    This will be used as a proxy for:
      - home_season_win_pct / away_season_win_pct
      - home_recent_win_pct_20g / away_recent_win_pct_20g
    for purely future schedule rows.
    """
    hist = base.copy()

    if "home_win" not in hist.columns:
        raise RuntimeError("Expected 'home_win' column in base parquet.")

    # Home side stats
    home_stats = (
        hist.groupby("home_team")["home_win"]
        .agg(games_home="count", wins_home="sum")
    )

    # Away side stats: define away_win = 1 - home_win, grouped by away_team
    hist["away_win"] = 1 - hist["home_win"]
    away_stats = (
        hist.groupby("away_team")["away_win"]
        .agg(games_away="count", wins_away="sum")
    )

    # Combine home + away stats per franchise
    team_stats = home_stats.join(away_stats, how="outer").fillna(0)

    team_stats["games_total"] = team_stats["games_home"] + team_stats["games_away"]
    team_stats["wins_total"] = team_stats["wins_home"] + team_stats["wins_away"]

    # Avoid division by zero
    team_stats["baseline_wp"] = 0.5  # default neutral
    mask = team_stats["games_total"] > 0
    team_stats.loc[mask, "baseline_wp"] = (
        team_stats.loc[mask, "wins_total"] / team_stats.loc[mask, "games_total"]
    )

    print("Built team baselines for", len(team_stats), "teams.")
    return team_stats[["baseline_wp"]]


def add_rest_features(future: pd.DataFrame) -> pd.DataFrame:
    """
    For future schedule games, compute:
      - home_days_rest / away_days_rest
      - home_b2b / away_b2b
    based purely on the schedule spacing.

    For the first game of a season for a team, we just give them 5 'rest' days.
    """
    future = future.copy()
    future["date"] = pd.to_datetime(future["date"])

    future["home_days_rest"] = pd.NA
    future["away_days_rest"] = pd.NA
    future["home_b2b"] = 0
    future["away_b2b"] = 0

    teams = pd.unique(pd.concat([future["home_team"], future["away_team"]]))

    for team in teams:
        mask_team = (future["home_team"] == team) | (future["away_team"] == team)
        sub = future.loc[mask_team].sort_values("date")

        prev_date = None
        for idx, row in sub.iterrows():
            if prev_date is None:
                days_rest = 5  # neutral default for first game
            else:
                days_rest = (row["date"] - prev_date).days

            # Assign to home or away side for this game
            if row["home_team"] == team:
                future.at[idx, "home_days_rest"] = days_rest
                future.at[idx, "home_b2b"] = 1 if days_rest <= 1 else 0
            if row["away_team"] == team:
                future.at[idx, "away_days_rest"] = days_rest
                future.at[idx, "away_b2b"] = 1 if days_rest <= 1 else 0

            prev_date = row["date"]

    # Fill any remaining NA with a neutral value
    future["home_days_rest"] = future["home_days_rest"].fillna(5)
    future["away_days_rest"] = future["away_days_rest"].fillna(5)

    return future


def main() -> None:
    # 1) Load existing processed games (with scores and full features)
    base_path = PROCESSED_DIR / "processed_games_with_scores.parquet"
    print("Loading base games from:", base_path)
    base = pd.read_parquet(base_path)
    print("Base shape:", base.shape)
    print("Base seasons:", sorted(base["season"].unique()))
    print("Base date range:", base["date"].min(), "→", base["date"].max())

    base["date"] = pd.to_datetime(base["date"])

    # 2) Load future schedule
    future = load_future_schedule()

    # Only keep games strictly after the last historical date
    last_hist_date = base["date"].max().date()
    future = future[future["date"] > last_hist_date].copy()
    print("Future rows AFTER filtering > last_hist_date:", future.shape)

    if future.empty:
        print("No future rows after last_hist_date; nothing to do.")
        return

    # 3) Build team baselines from historical data
    team_baselines = build_team_baselines(base)

    # 4) Start filling modeling features for future rows

    # Represent date as datetime for consistency with base
    future["date"] = pd.to_datetime(future["date"])

    # Outcome columns: unknown for future games
    future["home_pts"] = pd.NA
    future["away_pts"] = pd.NA
    future["home_win"] = pd.NA

    # Season strength and recent form (use same baseline)
    future = future.merge(
        team_baselines.rename(columns={"baseline_wp": "home_season_win_pct"}),
        left_on="home_team",
        right_index=True,
        how="left",
    )
    future = future.merge(
        team_baselines.rename(columns={"baseline_wp": "away_season_win_pct"}),
        left_on="away_team",
        right_index=True,
        how="left",
    )

    # Use same baseline as a proxy for "recent" 20-game form
    future["home_recent_win_pct_20g"] = future["home_season_win_pct"]
    future["away_recent_win_pct_20g"] = future["away_season_win_pct"]

    # Last-game point differential is unknown → neutral 0
    future["home_last_pd"] = 0.0
    future["away_last_pd"] = 0.0

    # 5) Rest and back-to-back flags from schedule spacing
    future = add_rest_features(future)

    # 6) Align columns to base schema

    base_cols = list(base.columns)

    # Make sure all base columns exist in `future`
    for col in base_cols:
        if col not in future.columns:
            future[col] = pd.NA

    # 7) Assign new game_ids
    if "game_id" not in base.columns:
        # If base has no game_id yet, create one deterministically
        base = base.sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)
        base["game_id"] = base.index.astype(int)
        print("Assigned game_id to base rows.")
    max_game_id = int(base["game_id"].max())
    future["game_id"] = range(max_game_id + 1, max_game_id + 1 + len(future))

    # Ensure `game_id` is in columns list (in case it wasn't originally)
    if "game_id" not in base_cols:
        base_cols.append("game_id")

    # Reorder to match base
    future = future[base_cols]

    print("Future sample:")
    print(future.head().to_dict(orient="records"))

    # 8) Concatenate and write out
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