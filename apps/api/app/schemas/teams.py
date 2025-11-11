from pydantic import BaseModel

class Team(BaseModel):
    team_id: int
    sport_id: int
    name: str

class TeamList(BaseModel):
    items: list[Team]
    total_returned: int
    limit: int
    offset: int
