# model_api/schemas.py

from sqlalchemy import Column, Integer, String
from model_api.db import Base

class Event(Base):
    __tablename__ = "events"

    event_id = Column(Integer, primary_key=True, index=True)
    sport_id = Column(Integer, index=True)
    date = Column(String)
    home_team_id = Column(Integer, nullable=True)
    away_team_id = Column(Integer, nullable=True)
    venue = Column(String, nullable=True)
    status = Column(String, nullable=True)