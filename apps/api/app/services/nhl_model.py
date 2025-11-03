from datetime import datetime, timezone

def predict_winprob(even_id: int): 
    # TODO replace with real features/model 
    home = 0.48
    return {
        "model_key": "nhl-winprob-0.1.0",
        "win_probabilities": {"home": home, "away": 1.0 - home},
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }   
    