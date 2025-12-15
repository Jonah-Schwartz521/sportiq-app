"""
Build simple NHL win-probability predictions for future games.

Inputs:
  - Historical games with scores: model/data/processed/nhl/nhl_games_for_app.parquet
  - Future schedule: model/data/processed/nhl/nhl_future_schedule_for_app.parquet

Output:
  - model/data/processed/nhl/nhl_predictions_future.parquet

Columns:
  nhl_game_id_str, event_id, date, home_team, away_team,
  p_home_win, p_away_win, source, generated_at
"""

from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

# Paths
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from src.paths import NHL_PROCESSED_DIR  # type: ignore  # noqa: E402
except Exception:
    NHL_PROCESSED_DIR = ROOT_DIR / "data" / "processed" / "nhl"

HIST_PATH = NHL_PROCESSED_DIR / "nhl_games_for_app.parquet"
FUTURE_PATH = NHL_PROCESSED_DIR / "nhl_future_schedule_for_app.parquet"
OUTPUT_PATH = NHL_PROCESSED_DIR / "nhl_predictions_future.parquet"

K_FACTOR = 20
START_RATING = 1500.0


def expected_score(home_rating: float, away_rating: float) -> float:
    return 1.0 / (1.0 + 10 ** ((away_rating - home_rating) / 400.0))


def build_ratings(history: pd.DataFrame) -> dict[str, float]:
    ratings: dict[str, float] = {}
    history = history.copy()
    history = history.sort_values("date")

    for _, row in history.iterrows():
        home = str(row["home_team"])
        away = str(row["away_team"])
        if pd.isna(row["home_pts"]) or pd.isna(row["away_pts"]):
            continue

        home_r = ratings.get(home, START_RATING)
        away_r = ratings.get(away, START_RATING)

        exp_home = expected_score(home_r, away_r)
        result_home = 1.0 if float(row["home_pts"]) > float(row["away_pts"]) else 0.0
        delta = K_FACTOR * (result_home - exp_home)

        ratings[home] = home_r + delta
        ratings[away] = away_r - delta

    return ratings


def build_predictions() -> None:
    if not HIST_PATH.exists():
        raise SystemExit(f"Historical NHL parquet not found: {HIST_PATH}")
    if not FUTURE_PATH.exists():
        raise SystemExit(f"Future NHL schedule parquet not found: {FUTURE_PATH}")

    hist = pd.read_parquet(HIST_PATH)
    future = pd.read_parquet(FUTURE_PATH)

    # Ensure dates are datetime and sorted
    hist["date"] = pd.to_datetime(hist["date"], utc=True, errors="coerce").dt.tz_localize(None)
    future["date"] = pd.to_datetime(future["date"], utc=True, errors="coerce").dt.tz_localize(None)

    hist = hist.dropna(subset=["home_team", "away_team", "home_pts", "away_pts"])
    ratings = build_ratings(hist)

    preds: list[dict[str, object]] = []
    now_ts = datetime.now(timezone.utc).replace(tzinfo=None)
    future_sorted = future.sort_values("date")

    for _, row in future_sorted.iterrows():
        home = str(row["home_team"])
        away = str(row["away_team"])
        home_r = ratings.get(home, START_RATING)
        away_r = ratings.get(away, START_RATING)
        p_home = expected_score(home_r, away_r)
        p_away = 1.0 - p_home

        preds.append(
            {
                "nhl_game_id_str": row.get("nhl_game_id_str"),
                "event_id": row.get("event_id"),
                "date": row["date"],
                "home_team": home,
                "away_team": away,
                "p_home_win": p_home,
                "p_away_win": p_away,
                "source": "elo_v1",
                "generated_at": now_ts,
            }
        )

    preds_df = pd.DataFrame(preds)
    preds_df = preds_df.dropna(subset=["nhl_game_id_str"])

    if preds_df.empty:
        raise SystemExit("No NHL predictions generated (rowcount=0).")

    preds_df = preds_df.sort_values("date").reset_index(drop=True)
    preds_df.to_parquet(OUTPUT_PATH, index=False)

    print(f"Generated {len(preds_df)} NHL predictions")
    print("Sample predictions (first 5 by date):")
    print(
        preds_df[["date", "home_team", "away_team", "p_home_win", "p_away_win", "nhl_game_id_str"]]
        .head(5)
        .to_string(index=False)
    )


if __name__ == "__main__":
    build_predictions()
