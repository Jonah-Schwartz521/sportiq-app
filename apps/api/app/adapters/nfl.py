from datetime import datetime, timezone
from typing import Dict

def predict_winprob(event_id: int) -> Dict:
    """
    Stub NFL predictor. Replace with real model logic later.
    """
    return {
        "model_key": "nfl-winprob-0.1.0",
        "win_probabilities": {"home": 0.57, "away": 0.43},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }