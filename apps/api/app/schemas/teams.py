from typing import List
from pydantic import BaseModel, ConfigDict

class Team(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    team_id: int
    sport_id: int
    name: str

class TeamList(BaseModel):
    model_config = ConfigDict(protected_namespaces=())
    items: List[Team]
    total_returned: int
    limit: int
    offset: int