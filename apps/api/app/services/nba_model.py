from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd
from joblib import load

import psycopg
from apps.api.app.core.config import POSTGRES_DSN

# ---- Artifacts ----
ART = Path("models/nba/artifacts")
MODEL_PATH = ART / "model.joblib"
FEATURE_META = ART / "feature_meta.json"

_model = None
_features = None


def _load():
    """Load the trained NBA model and its expected feature names."""
    global _model, _features
    if _model is None:
        if not MODEL_PATH.exists():
            raise RuntimeError("NBA model artifact missing. Run `make train-nba` first.")
        _model = load(MODEL_PATH)
    if _features is None:
        meta = json.loads(FEATURE_META.read_text())
        _features = meta["features"]
    return _model, _features


def _fetch_features_from_db(event_id: int) -> pd.DataFrame:
    """
    Fetch real features from Postgres for a given event.
    Keep this query minimal for now; we can expand as we add more features.
    """
    query = """
        SELECT
            e.event_id,
            t_home.elo AS home_elo_proxy,
            t_away.elo AS away_elo_proxy,
            (CURRENT_DATE - t_home.last_game_date) AS home_rest_days,
            (CURRENT_DATE - t_away.last_game_date) AS away_rest_days
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


def _ensure_expected_features(df: pd.DataFrame, expected: list) -> pd.DataFrame:
    """
    Make sure all features the model expects are present.
    If any are missing, fill with sensible defaults.
    Then reorder columns to match the model's feature order.
    """
    # Defaults for known engineered features
    defaults = {
        "home_flag": 1,            # this is a home-team perspective model
        "prior_home_adv": 0.58,    # simple baseline home-court edge (placeholder)
    }

    # Add any missing expected columns
    for col in expected:
        if col not in df.columns:
            if col in defaults:
                df[col] = defaults[col]
            else:
                # Fallback default for unknown missing features
                df[col] = 0.0

    # Coerce numeric types where appropriate
    for col in expected:
        if col in df.columns and df[col].dtype == "object":
            # best-effort numeric conversion; non-numeric stays as-is
            df[col] = pd.to_numeric(df[col], errors="ignore")

    # Reorder to match the model
    df = df[expected]
    return df


def predict_winprob(event_id: int) -> dict:
    """
    Generate win probability prediction for the home team given an event_id.
    """
    model, feats = _load()

    # Pull from DB
    base = _fetch_features_from_db(event_id)

    # Ensure all expected features exist (add defaults if missing) and align order
    row = _ensure_expected_features(base, feats)

    # Predict
    X = row.to_numpy(dtype=float)
    home_wp = float(model.predict_proba(X)[0, 1])
    home_wp = max(0.01, min(0.99), home_wp)
    return {
        "model_key": "nba-winprob-0.1.0",
        "win_probabilities": {"home": home_wp, "away": 1.0 - home_wp},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }