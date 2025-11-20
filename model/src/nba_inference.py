import joblib
import pandas as pd
from pathlib import Path
from .paths import MODEL_DIR

_ARTIFACT_PATH = MODEL_DIR / "nba_logreg_b2b_v1.joblib"
_artifact_cache = None

def load_nba_model():
    global _artifact_cache
    if _artifact_cache is None:
        print("Loading NBA model from:", _ARTIFACT_PATH)
        if not _ARTIFACT_PATH.exists():
            raise FileNotFoundError(f"Model file not found at {_ARTIFACT_PATH}")
        obj = joblib.load(_ARTIFACT_PATH)
        if not isinstance(obj, dict) or "model" not in obj or "features" not in obj:
            raise ValueError(
                f"Loaded artifact from {_ARTIFACT_PATH} is not a valid dict "
                "with 'model' and 'features' keys."
            )
        _artifact_cache = obj
    return _artifact_cache


def predict_home_win_proba(game_row: pd.Series) -> float:
    artifact = load_nba_model()
    model = artifact["model"]
    features = artifact["features"]

    X = game_row[features].to_frame().T
    proba = model.predict_proba(X)[0, 1]
    return float(proba)