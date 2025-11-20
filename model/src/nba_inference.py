import joblib
import pandas as pd 
from pathlib import Path 
from .paths import MODEL_DIR

_ARTIFACT_PATH = MODEL_DIR / "nba_logreg_b2b_v1.joblib"

_artifact_cache = None

def load_nba_model():
    global _artifact_cache
    if _artifact_cache is None:
        _artifact_cache = joblib.load(_ARTIFACT_PATH)

def predict_home_win_proba(game_row: pd.Series) -> float:
    """
    game_row: a single row with all engineered features already computed.
    Must contain the same columns as artifact['features'].

    Returns: P(home_win=1) as float.
    """
    artifact = load_nba_model()
    model = artifact["model"]
    features = artifact["features"]

    X = game_row[features].to_frame().T
    proba = model.predict_proba(X)[0,1]
    return float(proba)