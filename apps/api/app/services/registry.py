from typing import Callable, Dict, Any
from apps.api.app.services import predict_winprob as nba_predict
from apps.api.app.services import predict_winprob as mlb_predict
from apps.api.app.services import predict_winprob as nfl_predict
from apps.api.app.services import predict_winprob as nhl_predict
from apps.api.app.services import predict_winprob as ufc_predict 

from .nba_model import predict_winprob as nba_predict
# TODO: replace these stubs with real models later
def mlb_predict(event_id: int) -> Dict[str, Any]: 
    return {"model_key": "mlb-winprob-0.1.0",
            "win_probabilities": {"home": 0.52, "away": 0.48},
            "generated"
            "-at": "stub"}

def nfl_predict(event_id: int) -> Dict[str, Any]: 
    return {"model_key": "nfl-winprob-0.1.0",
            "win_probabilities": {"home": 0.50, "away": 0.50},
            "generated"
            "-at": "stub"}

def nhl_predict(event_id: int) -> Dict[str, Any]: 
    return {"model_key": "nhl-winprob-0.1.0",
            "win_probabilities": {"home": 0.49, "away": 0.51},
            "generated"
            "-at": "stub"}

REGISTRY: Dict[str, Callable[[int], Dict[str, Any]]] = {
    "nba": nba_predict,
    "mlb": mlb_predict,
    "nfl": nfl_predict,
    "nhl": nfl_predict,
    "ufc": ufc_predict
    
}