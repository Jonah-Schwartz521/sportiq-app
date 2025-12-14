"""
Build a canonical NFL modeling dataset that NEVER replaces history.

Inputs:
- model/data/processed/nfl/nfl_games.parquet               (historical backbone)
- model/data/processed/nfl/nfl_games_with_scores.parquet   (recent score refresh)

Output:
- model/data/processed/nfl/nfl_model_games.parquet
"""

from pathlib import Path
import pandas as pd

NFL_DIR = Path("model/data/processed/nfl")
BASE_PATH = NFL_DIR / "nfl_games.parquet"
SCORES_PATH = NFL_DIR / "nfl_games_with_scores.parquet"
OUT_PATH = NFL_DIR / "nfl_model_games.parquet"

# Key choice:
# Prefer a stable game_id if available, else fall back to (game_date, home_team, away_team).
PREFERRED_KEYS = ["game_id"]
FALLBACK_KEYS = ["game_date", "home_team", "away_team"]

def pick_join_keys(df: pd.DataFrame) -> list[str]:
    if all(k in df.columns for k in PREFERRED_KEYS):
        return PREFERRED_KEYS
    if all(k in df.columns for k in FALLBACK_KEYS):
        return FALLBACK_KEYS
    raise ValueError(f"Could not find join keys. Need {PREFERRED_KEYS} or {FALLBACK_KEYS}.")

def main():
    if not BASE_PATH.exists():
        raise FileNotFoundError(f"Missing base: {BASE_PATH}")

    base = pd.read_parquet(BASE_PATH)
    print(f"Loaded base: {len(base)} rows from {BASE_PATH}")

    if SCORES_PATH.exists():
        scores = pd.read_parquet(SCORES_PATH)
        print(f"Loaded scores: {len(scores)} rows from {SCORES_PATH}")
    else:
        scores = None
        print("No scores file found — building model table from base only.")

    # Normalize date column name
    if "gameday" in base.columns and "game_date" not in base.columns:
        base = base.rename(columns={"gameday": "game_date"})
    if scores is not None and "gameday" in scores.columns and "game_date" not in scores.columns:
        scores = scores.rename(columns={"gameday": "game_date"})

    # Choose join keys
    keys = pick_join_keys(base)
    print(f"Join keys: {keys}")

    merged = base.copy()

    # Merge scores in WITHOUT dropping history
    if scores is not None:
        # Only bring in score-related columns (avoid duplicating everything)
        score_cols = [c for c in scores.columns if c in [
            "game_id", "game_date", "home_team", "away_team",
            "home_score", "away_score", "status", "week", "season",
            "home_team_score", "away_team_score",  # just in case naming differs
            "score_home", "score_away"
        ]]
        scores_small = scores[score_cols].copy()

        # Unify score column names if needed
        rename_map = {}
        if "home_team_score" in scores_small.columns and "home_score" not in scores_small.columns:
            rename_map["home_team_score"] = "home_score"
        if "away_team_score" in scores_small.columns and "away_score" not in scores_small.columns:
            rename_map["away_team_score"] = "away_score"
        if "score_home" in scores_small.columns and "home_score" not in scores_small.columns:
            rename_map["score_home"] = "home_score"
        if "score_away" in scores_small.columns and "away_score" not in scores_small.columns:
            rename_map["score_away"] = "away_score"
        if rename_map:
            scores_small = scores_small.rename(columns=rename_map)

        # Drop dupes on the join key (keep latest row)
        scores_small = scores_small.sort_values(keys).drop_duplicates(subset=keys, keep="last")

        merged = merged.merge(
            scores_small,
            on=keys,
            how="left",
            suffixes=("", "_score")
        )

        # If base already had scores but refresh has newer, prefer refresh values when present
        for col in ["home_score", "away_score", "status"]:
            score_col = f"{col}_score"
            if score_col in merged.columns:
                merged[col] = merged[score_col].combine_first(merged[col] if col in merged.columns else None)
                merged = merged.drop(columns=[score_col])

    # Create label
    if "home_score" in merged.columns and "away_score" in merged.columns:
        merged["home_win"] = (merged["home_score"] > merged["away_score"]).astype("float")
        # Future games (or missing scores) should be NaN label
        merged.loc[merged["home_score"].isna() | merged["away_score"].isna(), "home_win"] = pd.NA
    else:
        merged["home_win"] = pd.NA

    merged["is_future"] = merged["home_win"].isna()

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(OUT_PATH, index=False)
    print(f"Wrote: {OUT_PATH} ({len(merged)} rows)")
    print(f"Future games (no label): {int(merged['is_future'].sum())}")

if __name__ == "__main__":
    main()