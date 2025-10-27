# apps/api/app/services/nba_model.py
from __future__ import annotations
from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd
from joblib import load
import psycopg
from apps.api.app.core.config import POSTGRES_DSN

ART = Path("models/nba/artifacts")
MODEL_PATH = ART / "model.joblib"
FEATURE_META = ART / "feature_meta.json"

_model = None
_features = None

def _load():
    """Load trained model + feature list with guardrails."""
    global _model, _features
    if _model is None:
        if not MODEL_PATH.exists():
            raise RuntimeError("NBA model artifact missing. Run `make train-nba` first.")
        _model = load(MODEL_PATH)

    if _features is None:
        meta = json.loads(FEATURE_META.read_text())
        feats = meta.get("features")
        # Guardrails: must be a list of strings
        if not isinstance(feats, list) or not all(isinstance(f, str) for f in feats):
            raise RuntimeError(f"feature_meta.json corrupted: features={feats!r}")
        _features = feats
    return _model, _features

def _raw_row_from_db(event_id: int) -> pd.DataFrame:
    """Pull raw inputs for features from Postgres."""
    query = """
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
        JOIN core.teams t_away ON e.away_team_id = t_away.team_id
        WHERE e.event_id = %s;
    """
    with psycopg.connect(POSTGRES_DSN) as conn:
        df = pd.read_sql(query, conn, params=(event_id,))
    if df.empty:
        raise ValueError(f"No event found with id {event_id}")
    return df

def _prepare_features(df_raw: pd.DataFrame, feature_names: list[str]) -> np.ndarray:
    """Match training preprocessing and column order exactly."""
    df = df_raw.copy()

    # Normalize to match training script (v0.2.0)
    # (Elo - 1500) / 100; rest as float
    for col in ["home_elo_proxy", "away_elo_proxy"]:
        df[col] = (df[col].astype(float) - 1500.0) / 100.0

    for col in ["home_rest_days", "away_rest_days"]:
        df[col] = df[col].astype(float)

    for col in ["home_3pa_rate", "away_3pa_rate", "home_reb_margin", "away_reb_margin"]:
        df[col] = df[col].astype(float)

    # Ensure required columns exist
    missing = [c for c in feature_names if c not in df.columns]
    if missing:
        raise RuntimeError(f"Missing required features from DB: {missing}")

    # Build 2D array in the exact order the model expects
    X = df[feature_names].to_numpy(dtype=float)
    return X

def predict_winprob(event_id: int) -> dict:
    """Compute win probability for an NBA event."""
    model, feats = _load()
    raw = _raw_row_from_db(event_id)
    X = _prepare_features(raw, feats)

    proba_home = float(model.predict_proba(X)[0, 1])
    # safety clip
    proba_home = float(np.clip(proba_home, 0.01, 0.99))

    return {
        "model_key": "nba-winprob-0.2.0",
        "win_probabilities": {"home": proba_home, "away": 1.0 - proba_home},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }