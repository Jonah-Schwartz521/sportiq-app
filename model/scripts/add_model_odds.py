from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

# -----------------------------
# Project paths
# -----------------------------
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))  # <-- fix: sys.path.append, not sys.append

from model.src.paths import PROCESSED_DIR  # type: ignore  # noqa: E402
from model.src.nba_inference import load_nba_model  # type: ignore  # noqa: E402

COMBINED_PATH = PROCESSED_DIR / "games_with_scores_and_future.parquet"


# -----------------------------
# Helpers
# -----------------------------
def prob_to_american(p: float) -> float:
    """
    Convert a win probability (0-1) to American odds.

    p >= 0.5 → favorite (negative)
    p <  0.5 → underdog (positive)
    """
    if p <= 0 or p >= 1:
        return float("nan")

    if p >= 0.5:
        # favorite
        return -round(100 * p / (1 - p))
    else:
        # underdog
        return round(100 * (1 - p) / p)


def main() -> None:
    print("Loading combined games parquet...")
    df = pd.read_parquet(COMBINED_PATH)

    # Make sure date is datetime
    if not pd.api.types.is_datetime64_any_dtype(df["date"]):
        df["date"] = pd.to_datetime(df["date"])

    # Identify scheduled games: no final scores yet
    is_scheduled = df["home_score"].isna() & df["away_score"].isna()
    scheduled = df[is_scheduled].copy()

    print(f"Found {len(scheduled)} scheduled games to score with the model.")

    if scheduled.empty:
        print("No scheduled games found. Nothing to do.")
        return

    # -----------------------------
    # Load model
    # -----------------------------
    print("Loading NBA model...")
    bundle = load_nba_model()

    # load_nba_model() may return either a bare estimator or a dict bundle.
    if isinstance(bundle, dict):
        model = bundle.get("model") or bundle.get("clf") or bundle.get("estimator")
        feature_cols = (
            bundle.get("feature_names")
            or bundle.get("feature_columns")
            or bundle.get("feature_cols")
        )

        if model is None:
            raise RuntimeError(
                "load_nba_model() returned a dict but no 'model'/'clf'/'estimator' key was found."
            )

        if feature_cols is None:
            # Fallback: try to read from the estimator itself
            if hasattr(model, "feature_names_in_"):
                feature_cols = list(model.feature_names_in_)
            else:
                raise RuntimeError(
                    "Could not determine feature columns from load_nba_model() bundle. "
                    "Add 'feature_names' (or similar) to the returned dict."
                )
        else:
            feature_cols = list(feature_cols)
    else:
        # Assume this is a scikit-style estimator
        model = bundle
        if hasattr(model, "feature_names_in_"):
            feature_cols = list(model.feature_names_in_)
        else:
            raise RuntimeError(
                "Model returned by load_nba_model() has no 'feature_names_in_' attribute. "
                "Update add_model_odds.py to know which feature columns to use."
            )

    print(f"Using {len(feature_cols)} feature columns for scoring")

    missing_cols = [c for c in feature_cols if c not in scheduled.columns]
    if missing_cols:
        print("ERROR: The following model feature columns are missing in the parquet:")
        for c in missing_cols:
            print("  -", c)
        print("Aborting so you can fix the feature mismatch.")
        return

    # Extract feature matrix for scheduled games and handle any missing values
    X_sched = scheduled[feature_cols].copy()

    # If any NaNs are present in the feature columns, fill them before scoring.
    # NOTE: This mirrors a simple "fill with 0" strategy. If your training
    # pipeline used a different imputation (e.g., means/medians), update this
    # to match that behavior.
    if X_sched.isna().any().any():
        n_rows_with_nans = X_sched.isna().any(axis=1).sum()
        print(f"Detected NaNs in feature matrix for {n_rows_with_nans} scheduled game(s); filling with 0 before prediction.")
        X_sched = X_sched.fillna(0)

    # -----------------------------
    # Predict home win probabilities
    # -----------------------------
    print("Scoring scheduled games with the model...")

    proba = model.predict_proba(X_sched)  # shape (n_games, n_classes)

    # Find index of the "home win" class.
    # We assume the model was trained with y in {0,1} where 1 = home win.
    classes = list(model.classes_)  # type: ignore[attr-defined]
    try:
        home_idx = classes.index(1)
    except ValueError:
        # Fallback: assume the positive / last class is "home win"
        print("WARNING: Could not find class '1' in model.classes_. "
              "Defaulting to last column as home win.")
        home_idx = len(classes) - 1

    home_win_prob = proba[:, home_idx]
    away_win_prob = 1.0 - home_win_prob

    # -----------------------------
    # Convert to American odds
    # -----------------------------
    home_odds = [prob_to_american(p) for p in home_win_prob]
    away_odds = [prob_to_american(p) for p in away_win_prob]

    # -----------------------------
    # Write predictions back into df
    # -----------------------------
    # These column names are up to you — adjust if you want a different naming scheme.
    df.loc[is_scheduled, "model_home_win_prob"] = home_win_prob
    df.loc[is_scheduled, "model_away_win_prob"] = away_win_prob
    df.loc[is_scheduled, "model_home_american_odds"] = home_odds
    df.loc[is_scheduled, "model_away_american_odds"] = away_odds

    print("Example scored row (first scheduled game):")
    first_idx = scheduled.index[0]
    row = df.loc[first_idx, [
        "date",
        "home_team",
        "away_team",
        "model_home_win_prob",
        "model_away_win_prob",
        "model_home_american_odds",
        "model_away_american_odds",
    ]]
    print(row.to_string())

    # -----------------------------
    # Save parquet
    # -----------------------------
    df.to_parquet(COMBINED_PATH, index=False)
    print("Wrote updated parquet with model odds to:", COMBINED_PATH)


if __name__ == "__main__":
    main()