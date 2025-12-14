#!/usr/bin/env python3
"""
Train a baseline NHL win-probability model (logistic regression).

Steps:
- Load model/data/processed/nhl/nhl_model_games.parquet
- Filter to games with final scores (no leakage)
- Time-based train/validation split
- Save artifacts under model/artifacts/nhl/
"""

from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import dump
from pandas.api.types import is_numeric_dtype
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss, roc_auc_score
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import StandardScaler

ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT_DIR / "model" / "data" / "processed" / "nhl"
ARTIFACTS_DIR = ROOT_DIR / "model" / "artifacts" / "nhl"
ARTIFACTS_DIR.mkdir(parents=True, exist_ok=True)
FEATURE_BASES: list[str] = [
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


def load_dataset() -> pd.DataFrame:
    path = PROCESSED_DIR / "nhl_model_games.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "nhl_model_games.parquet not found. Run build_nhl_model_games.py first."
        )
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def select_features(df: pd.DataFrame) -> list[str]:
    """Use engineered rolling features only (home_/away_ prefixes from FEATURE_BASES)."""
    allowed = {f"home_{b}" for b in FEATURE_BASES} | {f"away_{b}" for b in FEATURE_BASES}
    cols = [c for c in df.columns if c in allowed and is_numeric_dtype(df[c])]
    return cols


def main() -> None:
    df = load_dataset()

    # Keep only games with outcomes
    scored = df.dropna(subset=["home_pts", "away_pts"]).copy()
    scored = scored.sort_values("date")

    feature_cols = select_features(scored)
    if not feature_cols:
        raise RuntimeError("No feature columns found for NHL model training.")

    X = scored[feature_cols].fillna(0.0).to_numpy()
    y = (scored["home_pts"] > scored["away_pts"]).astype(int).to_numpy()

    # Time-based split (80/20)
    split_idx = int(0.8 * len(scored))
    X_train, X_val = X[:split_idx], X[split_idx:]
    y_train, y_val = y[:split_idx], y[split_idx:]

    pipeline = Pipeline(
        [
            ("scaler", StandardScaler()),
            (
                "clf",
                LogisticRegression(
                    max_iter=2000,
                    solver="lbfgs",
                    C=0.5,
                    class_weight="balanced",
                ),
            ),
        ]
    )
    pipeline.fit(X_train, y_train)

    proba_val = pipeline.predict_proba(X_val)[:, 1]
    # Sanity: avoid over-confident model by inspecting distribution
    extreme_mask = (proba_val < 0.1) | (proba_val > 0.9)
    extreme_frac = float(extreme_mask.mean()) if len(proba_val) else 0.0

    metrics = {
        "n_train": int(len(X_train)),
        "n_val": int(len(X_val)),
        "log_loss": float(log_loss(y_val, proba_val)),
        "brier": float(brier_score_loss(y_val, proba_val)),
        "auc": float(roc_auc_score(y_val, proba_val)) if len(np.unique(y_val)) > 1 else None,
        "extreme_frac_val": extreme_frac,
    }

    dump(pipeline, ARTIFACTS_DIR / "nhl_model.joblib")
    (ARTIFACTS_DIR / "feature_columns.json").write_text(
        json.dumps({"features": feature_cols}, indent=2)
    )
    (ARTIFACTS_DIR / "metrics.json").write_text(json.dumps(metrics, indent=2))

    print("✅ Trained NHL baseline model saved to", ARTIFACTS_DIR)
    print("📊 Metrics:", metrics)


if __name__ == "__main__":
    main()
