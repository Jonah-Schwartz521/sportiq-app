# apps/api/app/schemas/events.py
from typing import List, Optional
from pydantic import BaseModel, ConfigDict

class Event(BaseModel):
    # correct name
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())

    event_id: int
    sport_id: int
    season: int
    date: str
    home_team_id: Optional[int] = None
    away_team_id: Optional[int] = None
    venue: Optional[str] = None
    status: Optional[str] = None
    start_time: Optional[str] = None

class EventList(BaseModel):
    model_config = ConfigDict(from_attributes=True, protected_namespaces=())
    items: List[Event]
    total_returned: int
    limit: int
    offset: int