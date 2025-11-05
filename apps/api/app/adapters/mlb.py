from datetime import datetime, timezone
from typing import Dict, Any

def predict_winprob(event_id: int) -> Dict[str, Any]:
    """
    Stub NFL predictor.
    """
    p_home = 0.58 if (event_id % 5 in (0, 1)) else 0.42
    return {
        "model_key": "nfl-winprob-0.1.0",
        "win_probabilities": {"home": p_home, "away": 1 - p_home},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }