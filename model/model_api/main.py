from __future__ import annotations

from datetime import date as date_type, datetime
from pathlib import Path
from typing import Optional, Dict, List

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys

# --- Create app ONCE and add CORS ---------------------------------

app = FastAPI(title="SportIQ NBA Model API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- Ensure we can import src.* ------------------------------------

ROOT = Path(__file__).resolve().parents[1]  # /model
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.paths import PROCESSED_DIR
from src.nba_inference import load_nba_model, predict_home_win_proba

# --- Global objects (loaded once at startup) -----------------------

GAMES_DF: Optional[pd.DataFrame] = None

# simple in-memory lookup tables
TEAM_NAME_TO_ID: Dict[str, int] = {}
TEAM_ID_TO_NAME: Dict[int, str] = {}
SPORT_ID_NBA = 1


def load_games_table() -> pd.DataFrame:
    """Load the processed B2B modeling table once."""
    global GAMES_DF
    if GAMES_DF is None:
        path = PROCESSED_DIR / "processed_games_b2b_model.parquet"
        print(f"Loading games table from {path} ...")
        GAMES_DF = pd.read_parquet(path)

        # Ensure we have a stable game_id (if not already there)
        if "game_id" not in GAMES_DF.columns:
            GAMES_DF = (
                GAMES_DF.sort_values(["date", "home_team", "away_team"])
                .reset_index(drop=True)
            )
            GAMES_DF["game_id"] = GAMES_DF.index.astype(int)

    return GAMES_DF


def build_team_lookups(df: pd.DataFrame) -> None:
    """Build TEAM_NAME_TO_ID / TEAM_ID_TO_NAME from the games table."""
    global TEAM_NAME_TO_ID, TEAM_ID_TO_NAME

    all_teams = pd.unique(pd.concat([df["home_team"], df["away_team"]]))
    # Sort just to keep IDs stable
    sorted_names = sorted(str(t) for t in all_teams)

    TEAM_NAME_TO_ID = {name: i + 1 for i, name in enumerate(sorted_names)}
    TEAM_ID_TO_NAME = {v: k for k, v in TEAM_NAME_TO_ID.items()}

    print(f"Built team lookups for {len(TEAM_NAME_TO_ID)} NBA teams.")


# --- Schemas -------------------------------------------------------

class HealthResponse(BaseModel):
    status: str
    num_games: int
    model_loaded: bool


class PredictionResponse(BaseModel):
    game_id: int
    date: str
    home_team: str
    away_team: str
    p_home: float
    p_away: float


class TeamOut(BaseModel):
    team_id: int
    sport_id: int
    name: str


class EventOut(BaseModel):
    event_id: int
    sport_id: int
    date: str
    home_team_id: Optional[int]
    away_team_id: Optional[int]
    venue: Optional[str] = None
    status: Optional[str] = None
    start_time: Optional[str] = None


class ListTeamsResponse(BaseModel):
    items: List[TeamOut]


class ListEventsResponse(BaseModel):
    items: List[EventOut]


class InsightItem(BaseModel):
    type: str
    label: str
    detail: str
    value: Optional[float] = None  # e.g. probability edge (0–1)


class InsightsResponse(BaseModel):
    game_id: int
    model_key: str
    generated_at: str
    insights: List[InsightItem]


# --- Startup hook --------------------------------------------------

@app.on_event("startup")
def startup_event():
    """
    Warm up the API:
    - Load games table
    - Build team lookups
    - Load model artifact
    """
    games = load_games_table()
    build_team_lookups(games)
    _ = load_nba_model()
    print("Startup complete: games + model + lookups loaded.")


# --- Core routes ---------------------------------------------------

@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    games = load_games_table()
    try:
        artifact = load_nba_model()
        model_loaded = artifact is not None
    except Exception:
        model_loaded = False

    return HealthResponse(
        status="ok",
        num_games=len(games),
        model_loaded=model_loaded,
    )


@app.get("/teams", response_model=ListTeamsResponse)
def list_teams(limit: int = 100) -> ListTeamsResponse:
    """Return NBA teams as simple id/name objects."""
    teams = [
        TeamOut(team_id=team_id, sport_id=SPORT_ID_NBA, name=name)
        for name, team_id in TEAM_NAME_TO_ID.items()
    ]
    teams.sort(key=lambda t: t.name.lower())
    return ListTeamsResponse(items=teams[:limit])


@app.get("/events", response_model=ListEventsResponse)
def list_events(limit: int = 50) -> ListEventsResponse:
    """Return games as EventOut objects."""
    games = load_games_table()

    if "game_id" not in games.columns:
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    subset = games.sort_values("date").head(limit)

    items: List[EventOut] = []
    for _, row in subset.iterrows():
        items.append(
            EventOut(
                event_id=int(row["game_id"]),
                sport_id=SPORT_ID_NBA,
                date=str(row["date"].date()),
                home_team_id=TEAM_NAME_TO_ID.get(str(row["home_team"])),
                away_team_id=TEAM_NAME_TO_ID.get(str(row["away_team"])),
                venue=None,
                status="final",  # or use a real status column if you have one
                start_time=None,
            )
        )

    return ListEventsResponse(items=items)


@app.get("/events/{event_id}", response_model=EventOut)
def get_event(event_id: int) -> EventOut:
    """Return a single event by its id."""
    games = load_games_table()

    if "game_id" not in games.columns:
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    match = games[games["game_id"] == event_id]
    if match.empty:
        raise HTTPException(status_code=404, detail=f"No event found with id={event_id}")

    row = match.iloc[0]
    return EventOut(
        event_id=int(row["game_id"]),
        sport_id=SPORT_ID_NBA,
        date=str(row["date"].date()),
        home_team_id=TEAM_NAME_TO_ID.get(str(row["home_team"])),
        away_team_id=TEAM_NAME_TO_ID.get(str(row["away_team"])),
        venue=None,
        status="final",
        start_time=None,
    )


@app.get("/predict_by_game_id", response_model=PredictionResponse)
def predict_by_game_id(game_id: int) -> PredictionResponse:
    """
    Predict home win probability for a game by its game_id.
    """
    games = load_games_table()

    if "game_id" not in games.columns:
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    match = games[games["game_id"] == game_id]

    if match.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No game found with game_id={game_id}",
        )

    row = match.iloc[0]
    p_home = predict_home_win_proba(row)
    p_away = 1.0 - p_home

    return PredictionResponse(
        game_id=int(row["game_id"]),
        date=str(row["date"].date()),
        home_team=str(row["home_team"]),
        away_team=str(row["away_team"]),
        p_home=float(p_home),
        p_away=float(p_away),
    )


@app.get("/predict", response_model=PredictionResponse)
def predict(
    home_team: str,
    away_team: str,
    game_date: date_type,
) -> PredictionResponse:
    """
    Predict home win probability based on (date, home_team, away_team).
    Uses the pre-engineered features from the processed table.
    """
    games = load_games_table()

    # Filter by date and teams
    mask = (
        (games["date"].dt.date == game_date)
        & (games["home_team"] == home_team)
        & (games["away_team"] == away_team)
    )

    candidates = games[mask]

    if candidates.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No game found for {game_date} {home_team} vs {away_team}",
        )

    # If multiple rows match, just pick the first for now
    row = candidates.iloc[0]

    p_home = predict_home_win_proba(row)
    p_away = 1.0 - p_home

    game_id = int(row["game_id"]) if "game_id" in row else -1

    return PredictionResponse(
        game_id=game_id,
        date=str(row["date"].date()),
        home_team=str(row["home_team"]),
        away_team=str(row["away_team"]),
        p_home=float(p_home),
        p_away=float(p_away),
    )


# --- Insights ------------------------------------------------------

@app.get("/insights/{game_id}", response_model=InsightsResponse)
def game_insights(game_id: int) -> InsightsResponse:
    """
    Return simple model-driven insights for a given game_id.
    Currently uses only the win probabilities from the NBA model
    (no extra feature columns required).
    """
    games = load_games_table()

    if "game_id" not in games.columns:
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    match = games[games["game_id"] == game_id]
    if match.empty:
        raise HTTPException(
            status_code=404,
            detail=f"No game found with game_id={game_id}",
        )

    row = match.iloc[0]

    # Use the same model as /predict_by_game_id
    p_home = float(predict_home_win_proba(row))
    p_away = float(1.0 - p_home)

    home_team = str(row["home_team"])
    away_team = str(row["away_team"])

    edge = abs(p_home - p_away)
    home_is_fav = p_home >= p_away

    insights: List[InsightItem] = []

    # 1) Favorite / underdog insight
    fav_team = home_team if home_is_fav else away_team
    dog_team = away_team if home_is_fav else home_team
    fav_prob = p_home if home_is_fav else p_away

    insights.append(
        InsightItem(
            type="favorite",
            label="Favorite",
            detail=f"{fav_team} are favored over {dog_team} with a win probability of {fav_prob:.1%}.",
            value=fav_prob,
        )
    )

    # 2) Edge strength insight
    if edge < 0.05:
        desc = "This looks like a near coin-flip matchup."
    elif edge < 0.15:
        desc = "One team has a modest edge, but the game is still fairly close."
    else:
        desc = "The model sees a clear favorite in this matchup."

    insights.append(
        InsightItem(
            type="edge",
            label="Model edge",
            detail=f"{desc} The probability difference between sides is {edge:.1%}.",
            value=edge,
        )
    )

    # 3) Home court insight
    insights.append(
        InsightItem(
            type="home_court",
            label="Home court",
            detail=f"{home_team} are at home. The model gives them a win probability of {p_home:.1%}.",
            value=p_home,
        )
    )

    return InsightsResponse(
        game_id=int(row["game_id"]),
        model_key="nba_logreg_b2b_v1",
        generated_at=datetime.utcnow().isoformat() + "Z",
        insights=insights,
    )