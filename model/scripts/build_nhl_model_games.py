#!/usr/bin/env python3
"""
Build an NHL model dataset with pre-game features (no leakage).

Inputs
------
- model/data/processed/nhl/nhl_games_with_scores.parquet (preferred)
- model/data/processed/nhl/nhl_games_for_app.parquet (fallback)
- model/data/processed/nhl/nhl_team_lookup.csv

Outputs
-------
- model/data/processed/nhl/nhl_model_games.parquet
  Contains:
    - date (UTC-naive)
    - home_team / away_team (strings)
    - home_pts / away_pts (scores if final, else NaN)
    - status (final / scheduled)
    - nhl_game_id_str (YYYY_MM_DD_HOME_AWAY)
    - engineered, shifted rolling features for home_* and away_* prefixes

Rolling features are shifted by one game to avoid leakage.
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT_DIR / "model" / "data" / "processed"
NHL_DIR = PROCESSED_DIR / "nhl"
OUTPUT_PATH = NHL_DIR / "nhl_model_games.parquet"
FEATURE_BASES: List[str] = [
    "win_pct_10",
    "win_pct_20",
    "gf_avg_10",
    "ga_avg_10",
    "gd_avg_10",
    "gf_avg_20",
    "ga_avg_20",
    "gd_avg_20",
    "season_win_pct",
    "season_games_played",
]


def load_source_games() -> pd.DataFrame:
    """Load NHL games from both scored + app files and prefer rows with scores."""
    scored_path = NHL_DIR / "nhl_games_with_scores.parquet"
    app_path = NHL_DIR / "nhl_games_for_app.parquet"

    frames: list[pd.DataFrame] = []
    if scored_path.exists():
        scored = pd.read_parquet(scored_path).copy()
        scored["__source_flag"] = "scored"
        frames.append(scored)
        print(f"Loaded {len(scored)} rows from {scored_path}")
    if app_path.exists():
        app = pd.read_parquet(app_path).copy()
        app["__source_flag"] = "app"
        frames.append(app)
        print(f"Loaded {len(app)} rows from {app_path}")

    if not frames:
        raise FileNotFoundError("No NHL games parquet found in processed/nhl.")

    df = pd.concat(frames, ignore_index=True)

    # Normalize minimal columns for dedupe
    df = df.rename(
        columns={
            "game_datetime": "date",
            "home_team_name": "home_team",
            "away_team_name": "away_team",
            "home_score": "home_pts",
            "away_score": "away_pts",
        }
    )
    for col in ["date", "home_team", "away_team", "home_pts", "away_pts"]:
        if col not in df.columns:
            df[col] = np.nan
    df["date"] = pd.to_datetime(df["date"], errors="coerce")

    # For schedule/app rows, scores are not final → force to NaN
    app_mask = df["__source_flag"] == "app"
    if app_mask.any():
        df.loc[app_mask, ["home_pts", "away_pts"]] = np.nan

    has_scores = df["home_pts"].notna() & df["away_pts"].notna()
    df["_quality"] = has_scores.astype(int)

    dedupe_key = df["date"].dt.strftime("%Y-%m-%d") + "|" + df["home_team"].astype(str) + "|" + df["away_team"].astype(str)
    df["_dedupe_key"] = dedupe_key
    df = df.sort_values(["_dedupe_key", "_quality"], ascending=[True, False])
    df = df.drop_duplicates(subset=["_dedupe_key"], keep="first")
    df = df.drop(columns=["_quality", "_dedupe_key"])
    return df.reset_index(drop=True)


def canonical_team_code(raw: str | None) -> str | None:
    """Return uppercase team code with spaces/punctuation stripped."""
    if raw is None or pd.isna(raw):
        return None
    return (
        str(raw)
        .upper()
        .replace(" ", "")
        .replace(".", "")
        .replace("-", "")
        .replace("_", "")
    )


def build_game_id(row: pd.Series) -> str | None:
    """Construct YYYY_MM_DD_HOME_AWAY id."""
    dt = pd.to_datetime(row.get("date"), errors="coerce")
    if pd.isna(dt):
        return None

    home_code = canonical_team_code(
        row.get("raw_home_team_abbrev")
        or row.get("home_team_abbrev")
        or row.get("home_team_name")
        or row.get("home_team")
    )
    away_code = canonical_team_code(
        row.get("raw_away_team_abbrev")
        or row.get("away_team_abbrev")
        or row.get("away_team_name")
        or row.get("away_team")
    )

    if home_code is None or away_code is None:
        return None

    return f"{dt.strftime('%Y_%m_%d')}_{home_code}_{away_code}"


def normalize_games(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names and add status + nhl_game_id_str."""
    rename_map = {
        "game_datetime": "date",
        "home_team_name": "home_team",
        "away_team_name": "away_team",
        "home_score": "home_pts",
        "away_score": "away_pts",
    }
    df = df.rename(columns={k: v for k, v in rename_map.items() if k in df.columns})

    # Ensure required columns exist
    for col in ["home_team", "away_team", "home_pts", "away_pts", "date"]:
        if col not in df.columns:
            df[col] = np.nan

    df["date"] = pd.to_datetime(df["date"], utc=True, errors="coerce").dt.tz_localize(None)

    # If game date is in the future, ensure scores are NaN so they are treated as upcoming
    future_mask = df["date"] > pd.Timestamp.utcnow().tz_localize(None)
    if future_mask.any():
        df.loc[future_mask, ["home_pts", "away_pts"]] = np.nan

    # Derive status
    scored = df["home_pts"].notna() & df["away_pts"].notna()
    df["status"] = np.where(scored, "final", "scheduled")

    # Build canonical id
    df["nhl_game_id_str"] = df.apply(build_game_id, axis=1)

    return df


def build_long_df(games: pd.DataFrame) -> pd.DataFrame:
    """Create long form per-team rows for rolling feature computation."""
    rows: List[Dict[str, object]] = []
    for idx, row in games.iterrows():
        date_val = row["date"]
        season = row.get("season")
        home = str(row.get("home_team"))
        away = str(row.get("away_team"))
        home_pts = row.get("home_pts")
        away_pts = row.get("away_pts")

        # If scores missing, still include row to allow history length tracking
        goal_diff_home = (
            None if pd.isna(home_pts) or pd.isna(away_pts) else float(home_pts - away_pts)
        )

        rows.append(
            {
                "orig_idx": idx,
                "team": home,
                "opponent": away,
                "is_home": True,
                "date": date_val,
                "season": season,
                "goals_for": home_pts,
                "goals_against": away_pts,
                "goal_diff": goal_diff_home,
                "win": (
                    np.nan
                    if pd.isna(home_pts) or pd.isna(away_pts)
                    else float(home_pts > away_pts)
                ),
            }
        )

        goal_diff_away = (
            None if pd.isna(home_pts) or pd.isna(away_pts) else float(away_pts - home_pts)
        )
        rows.append(
            {
                "orig_idx": idx,
                "team": away,
                "opponent": home,
                "is_home": False,
                "date": date_val,
                "season": season,
                "goals_for": away_pts,
                "goals_against": home_pts,
                "goal_diff": goal_diff_away,
                "win": (
                    np.nan
                    if pd.isna(home_pts) or pd.isna(away_pts)
                    else float(away_pts > home_pts)
                ),
            }
        )

    long_df = pd.DataFrame(rows)
    long_df["date"] = pd.to_datetime(long_df["date"], errors="coerce")
    for col in ["goals_for", "goals_against", "goal_diff", "win"]:
        long_df[col] = pd.to_numeric(long_df[col], errors="coerce")
    return long_df


def add_rolling_features(long_df: pd.DataFrame) -> pd.DataFrame:
    """Compute shifted rolling stats per team."""
    feature_cols = FEATURE_BASES.copy()

    def compute(group: pd.DataFrame) -> pd.DataFrame:
        g = group.sort_values("date").copy()
        # season-aware counters
        g["season_games_played"] = (
            g.groupby(g["season"]).cumcount().astype(float)
        )  # before current game

        # rolling helpers (shifted)
        g["win_pct_10"] = (
            g["win"].rolling(window=10, min_periods=1).mean().shift(1)
        )
        g["win_pct_20"] = (
            g["win"].rolling(window=20, min_periods=1).mean().shift(1)
        )

        for window, suffix in [(10, "10"), (20, "20")]:
            g[f"gf_avg_{suffix}"] = (
                g["goals_for"].rolling(window=window, min_periods=1).mean().shift(1)
            )
            g[f"ga_avg_{suffix}"] = (
                g["goals_against"].rolling(window=window, min_periods=1).mean().shift(1)
            )
            g[f"gd_avg_{suffix}"] = (
                g["goal_diff"].rolling(window=window, min_periods=1).mean().shift(1)
            )

        # season-to-date win pct (shifted)
        g["season_wins_before"] = g.groupby(g["season"])["win"].cumsum().shift(1)
        g["season_win_pct"] = g["season_wins_before"] / g["season_games_played"].replace(
            0, np.nan
        )

        return g

    enriched = long_df.groupby("team", group_keys=False).apply(compute)

    # Fill safe defaults
    pct_cols = [c for c in enriched.columns if "win_pct" in c]
    enriched[pct_cols] = enriched[pct_cols].fillna(0.5)
    other_cols = [c for c in feature_cols if c not in pct_cols and c in enriched.columns]
    enriched[other_cols] = enriched[other_cols].fillna(0.0)

    return enriched


def merge_features_wide(
    games: pd.DataFrame, features_long: pd.DataFrame
) -> pd.DataFrame:
    """Attach home/away features back to wide games."""
    feature_cols = FEATURE_BASES.copy()

    home_feats = (
        features_long[features_long["is_home"]]
        .set_index("orig_idx")[feature_cols]
        .add_prefix("home_")
    )
    away_feats = (
        features_long[~features_long["is_home"]]
        .set_index("orig_idx")[feature_cols]
        .add_prefix("away_")
    )

    out = games.copy()
    out = out.join(home_feats, how="left").join(away_feats, how="left")

    # Defaults
    pct_cols = [f"home_{b}" for b in FEATURE_BASES if "win_pct" in b] + [
        f"away_{b}" for b in FEATURE_BASES if "win_pct" in b
    ]
    pct_cols = [c for c in pct_cols if c in out.columns]
    out[pct_cols] = out[pct_cols].fillna(0.5)

    other_feature_cols = [
        f"home_{b}" for b in FEATURE_BASES if "win_pct" not in b
    ] + [
        f"away_{b}" for b in FEATURE_BASES if "win_pct" not in b
    ]
    other_feature_cols = [c for c in other_feature_cols if c in out.columns]
    out[other_feature_cols] = out[other_feature_cols].fillna(0.0)

    return out


def main() -> None:
    games_raw = load_source_games()
    games = normalize_games(games_raw)
    games = games.sort_values("date").reset_index(drop=True)

    long_df = build_long_df(games)
    features_long = add_rolling_features(long_df)
    model_games = merge_features_wide(games, features_long)

    model_games.to_parquet(OUTPUT_PATH, index=False)
    print(f"✅ Wrote NHL model dataset to {OUTPUT_PATH} (rows={len(model_games)})")


if __name__ == "__main__":
    main()
