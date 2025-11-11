from typing import List
from pydantic import BaseModel, ConfigDict

class Insight(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    type: str          # e.g. "key_stat", "angle", "trend"
    label: str         # short label
    detail: str        # full sentence

class InsightsResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    event_id: int
    sport: str
    model_key: str
    insights: List[Insight]