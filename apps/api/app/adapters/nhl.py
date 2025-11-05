from datetime import datetime, timezone
from typing import Dict, Any

def predict_winprob(event_id: int) -> Dict[str, Any]:
    """
    Stub NHL predictor.
    """
    p_home = 0.51 if (event_id % 2 == 0) else 0.49
    return {
        "model_key": "nhl-winprob-0.1.0",
        "win_probabilities": {"home": p_home, "away": 1 - p_home},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }