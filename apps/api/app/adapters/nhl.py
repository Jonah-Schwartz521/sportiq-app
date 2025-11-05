from datetime import datetime, timezone
from typing import Dict

def predict_winprob(event_id: int) -> Dict:
    """
    Stub NHL predictor. Replace with real model logic later.
    """
    return {
        "model_key": "nhl-winprob-0.1.0",
        "win_probabilities": {"home": 0.51, "away": 0.49},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }