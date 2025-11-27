
from sqlalchemy import(
    Column,
    Integer, 
    String, 
    Boolean, 
    Date, 
    DateTime, 
    ForeignKey, 
    Float,
) 

from model_api.db import Base
from datetime import datetime

class Event(Base):
    __tablename__ = "events"

    event_id = Column(Integer, primary_key=True, index=True)
    sport_id = Column(Integer, index=True)
    date = Column(String)
    home_team_id = Column(Integer, ForeignKey("teams.teams_id"), nullable=True)
    away_team_id = Column(Integer, ForeignKey("teams.teams_id"), nullable=True)
    venue = Column(String, nullable=True)
    status = Column(String, nullable=True)

    home_scores = Column(Integer, nullable=True)
    away_scores = Column(Integer, nullable=True)
    home_win = Column(Boolean, nullable=True)

class Prediction(Base):
    __tablename__ = "predictions"

    id = Column(Integer, primary_key=True, autoincrement=True)
    game_id = Column(Integer, nullable=False, index=True)
    model_key = Column(String(100), nullable=False)
    p_home = Column(Float, nullable=False)
    p_away = Column(Float, nullable=False)
    created_at = Column(DateTime, nullable=False, default=datetime.utcnow)