from datetime import datetime, timezone
from typing import Dict, Any

def predict_winprob(event_id: int) -> Dict[str, Any]:
    # demo stub — replace later with real model call
    return {
        "model_key": "nba-winprob-0.1.0",
        "win_probabilities": {"home": 0.55, "away": 0.45},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }