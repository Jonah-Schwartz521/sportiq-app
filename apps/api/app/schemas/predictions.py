from typing import Dict, List
from pydantic import BaseModel, ConfigDict

class PredictResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    model_key: str
    win_probabilities: Dict[str, float]
    generated_at: str

class PredictionSummary(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    event_id: int
    model_key: str
    home_wp: float
    away_wp: float
    created_at: str

class PredictionSummaryList(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    items: List[PredictionSummary]
    total_returned: int
    limit: int
    offset: int