from datetime import datetime, timezone
from typing import Dict

def predict_winprob(event_id: int) -> Dict:
    """
    Stub NBA predictor. Replace with real model logic later.
    """
    # Dummy logic example 
    return {
        "model_key": "nba-winprob-0.1.0",
        "win_probabilities": {"home": 0.55, "away": 0.45},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }