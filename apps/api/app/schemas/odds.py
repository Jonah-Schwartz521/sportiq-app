# apps/api/app/schemas/odds.py
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime


class OddsRecord(BaseModel):
    """Single odds record from a bookmaker."""
    sport: str
    commence_time_utc: datetime
    home_team: str
    away_team: str
    bookmaker: str
    market: str  # 'h2h' or 'spreads'
    outcome_name: str
    outcome_price: float  # American odds
    point: Optional[float] = None  # Spread value (null for moneyline)
    last_update_utc: datetime
    source: str


class OddsForEvent(BaseModel):
    """Odds for a specific event/game."""
    event_id: Optional[int] = None
    home_team: str
    away_team: str
    commence_time_utc: datetime
    moneyline: List[OddsRecord] = []
    spreads: List[OddsRecord] = []


class OddsList(BaseModel):
    """List of odds records."""
    items: List[OddsRecord]
    total_returned: int
