from datetime import datetime, timezone
from typing import Dict

def predict_winprob(event_id: int) -> Dict:
    """
    Stub MLB predictor. Replace with real model logic later.
    """
    return {
        "model_key": "mlb-winprob-0.1.0",
        "win_probabilities": {"home": 0.52, "away": 0.48},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }