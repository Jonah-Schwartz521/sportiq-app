from datetime import datetime, timezone
from typing import Dict, Any

def predict_winprob(event_id: int) -> Dict[str, Any]:
    """
    Stub UFC predictor (event-based). The /predict/ufc test that sends
    fighter_a/fighter_b without event_id is handled directly in the router;
    this function covers the event_id path and returns home/away keys.
    """
    p_home = 0.53 if (event_id % 2 == 0) else 0.47
    return {
        "model_key": "ufc-winprob-0.1.0",
        "win_probabilities": {"home": p_home, "away": 1 - p_home},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }