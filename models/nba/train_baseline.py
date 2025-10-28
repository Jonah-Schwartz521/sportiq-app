# models/nba/train_baseline.py
from __future__ import annotations
from pathlib import Path
import os
import json
import numpy as np
import pandas as pd
from joblib import dump
from sklearn.linear_model import LogisticRegression
from sklearn.model_selection import train_test_split
from sklearn.metrics import brier_score_loss, log_loss
from dotenv import load_dotenv
import psycopg

# -----------------------------
# Versioning & artifact paths
# -----------------------------
VERSION = "0.3.0"
ART = Path("models/nba/artifacts")
ART.mkdir(parents=True, exist_ok=True)

# -------------------------------------------
# EXACT feature order used in API + database
# -------------------------------------------
FEATURES = [
    "home_elo_proxy",
    "away_elo_proxy",
    "home_rest_days",
    "away_rest_days",
    "home_3pa_rate",
    "away_3pa_rate",
    "home_reb_margin",
    "away_reb_margin",
]

# -------------------------------------------
# Load DSN (same as API)
# -------------------------------------------
load_dotenv(override=True)
POSTGRES_DSN = (
    f"postgresql://{os.getenv('POSTGRES_USER','sportiq')}:"
    f"{os.getenv('POSTGRES_PASSWORD','sportiq')}@"
    f"{os.getenv('POSTGRES_HOST','localhost')}:"
    f"{os.getenv('POSTGRES_PORT','5433')}/"
    f"{os.getenv('POSTGRES_DB','sportiq')}"
)

def load_features_from_db() -> pd.DataFrame:
    """
    Pulls numeric features directly from core.events + core.teams.
    This ensures the training set matches serving inputs.
    """
    sql = """
        SELECT
            e.event_id,
            t_home.elo                     AS home_elo_proxy,
            t_away.elo                     AS away_elo_proxy,
            (CURRENT_DATE - t_home.last_game_date)::int AS home_rest_days,
            (CURRENT_DATE - t_away.last_game_date)::int AS away_rest_days,
            t_home.three_pt_rate           AS home_3pa_rate,
            t_away.three_pt_rate           AS away_3pa_rate,
            t_home.rebound_margin          AS home_reb_margin,
            t_away.rebound_margin          AS away_reb_margin
        FROM core.events e
        JOIN core.teams t_home ON e.home_team_id = t_home.team_id
        JOIN core.teams t_away ON e.away_team_id = t_away.team_id;
    """
    with psycopg.connect(POSTGRES_DSN) as conn:
        df = pd.read_sql(sql, conn)
    if df.empty:
        raise RuntimeError("No events found — seed teams and at least one event first.")
    return df

def make_synthetic_labels(df: pd.DataFrame, rng: np.random.Generator) -> np.ndarray:
    """
    Generate synthetic outcomes from known statistical relationships.
    This lets us test model logic without real historical results.
    """
    diff_elo = (df["home_elo_proxy"] - df["away_elo_proxy"]) / 100.0
    rest_edge = 0.03 * (df["home_rest_days"] - df["away_rest_days"])
    style_edge = (
        0.4 * (df["home_3pa_rate"] - df["away_3pa_rate"]) +
        0.05 * (df["home_reb_margin"] - df["away_reb_margin"])
    )

    logit = 1.2 * diff_elo + 0.5 * style_edge + 0.1 * rest_edge
    base = 1 / (1 + np.exp(-logit))
    p = np.clip(base, 0.05, 0.95)
    return (rng.random(len(df)) < p).astype(int)  # 1 = home win

def main():
    rng = np.random.default_rng(2024)
    df = load_features_from_db()
    y = make_synthetic_labels(df, rng)
    X = df[FEATURES].to_numpy()

    Xtr, Xval, ytr, yval = train_test_split(
        X, y, test_size=0.25, random_state=2024, stratify=y
    )

    model = LogisticRegression(max_iter=2000)
    model.fit(Xtr, ytr)

    proba_val = model.predict_proba(Xval)[:, 1]
    metrics = {
        "brier": float(brier_score_loss(yval, proba_val)),
        "logloss": float(log_loss(yval, proba_val)),
        "n_train": int(len(Xtr)),
        "n_val": int(len(Xval)),
        "version": VERSION,
    }

    dump(model, ART / "model.joblib")
    (ART / "feature_meta.json").write_text(json.dumps({
        "features": FEATURES,
        "target": "y_home_win",
        "version": VERSION
    }, indent=2))
    (ART / "metrics.json").write_text(json.dumps(metrics, indent=2))

    print("✅ Model trained and saved to:", ART.resolve())
    print("📊 Metrics:", metrics)

if __name__ == "__main__":
    main()