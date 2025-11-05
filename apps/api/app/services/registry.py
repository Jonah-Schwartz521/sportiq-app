# apps/api/app/services/registry.py
from typing import Callable, Dict, Any

from apps.api.app.adapters import nba, mlb, nfl, nhl, ufc

REGISTRY: Dict[str, Callable[[int], Dict[str, Any]]] = {
    "nba": nba.predict_winprob,
    "mlb": mlb.predict_winprob,
    "nfl": nfl.predict_winprob,
    "nhl": nhl.predict_winprob,
    "ufc": ufc.predict_winprob,
}