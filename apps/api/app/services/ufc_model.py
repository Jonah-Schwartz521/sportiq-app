from datetime import datetime, timezone

def predict_winprob(even_id: int): 
    # Map fighter_a/b into a home/away style for persistance consistency 
    a = 0.55
    b = 0.45
    return {
        "model_key": "ufc-winprob-0.1.0",
        "win_probabilities": {"home": a, "away": b},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }   
    