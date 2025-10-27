from pathlib import Path
from datetime import datetime, timezone
import json
import numpy as np
import pandas as pd
from joblib import load

# Define where the model artifacts are located
ART = Path("models/nba/artifacts")
MODEL_PATH = ART / "model.joblib"
FEATURE_META = ART / "feature_meta.json"

_model = None
_features = None

def _load():
    """Load the trained NBA model and its feature metadata."""
    global _model, _features
    if _model is None:
        if not MODEL_PATH.exists():
            raise RuntimeError("NBA model artifact missing. Run `make train-nba` first.")
        _model = load(MODEL_PATH)
    if _features is None:
        meta = json.loads(FEATURE_META.read_text())
        _features = meta["features"]
    return _model, _features


def _feature_row_from_event(event_id: int) -> pd.DataFrame:
    """Temporary placeholder: synthesizes feature values for the given event."""
    return pd.DataFrame([{
        "event_id": event_id,
        "home_flag": 1,
        "prior_home_adv": 0.58,
        "home_elo_proxy": 0.1,
        "away_elo_proxy": -0.1,
    }])


def predict_winprob(event_id: int):
    """Generate win probability predictions for an event."""
    model, feats = _load()
    row = _feature_row_from_event(event_id)
    X = row[feats].to_numpy()
    home_wp = float(model.predict_proba(X)[0, 1])
    return {
        "model_key": "nba-winprob-0.1.0",
        "win_probabilities": {"home": home_wp, "away": 1.0 - home_wp},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }
