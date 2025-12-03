from __future__ import annotations

from datetime import date as date_type, datetime
from pathlib import Path
from typing import Optional, Dict, List

from collections import deque

import pandas as pd
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import sys

import logging
from sqlalchemy import select
from sqlalchemy.orm import Session
from sqlalchemy.exc import OperationalError
from .db import SessionLocal
from .schemas import Event, Prediction


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)



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

# Will be assigned after PredictionLogItem is defined
RECENT_PREDICTIONS = deque(maxlen=200)


def load_games_table() -> pd.DataFrame:
    """Load the processed B2B modeling table once."""
    global GAMES_DF
    if GAMES_DF is None:
        path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
        logger.info("Loading games table from %s ...", path)
        GAMES_DF = pd.read_parquet(path)

        # Ensure we have a stable game_id (if not already there)
        if "game_id" not in GAMES_DF.columns:
            GAMES_DF = (
                GAMES_DF.sort_values(["date", "home_team", "away_team"])
                .reset_index(drop=True)
            )
            GAMES_DF["game_id"] = GAMES_DF.index.astype(int)

        logger.info("Loaded games table with %d rows", len(GAMES_DF))
    return GAMES_DF


def build_team_lookups(df: pd.DataFrame) -> None:
    """Build TEAM_NAME_TO_ID / TEAM_ID_TO_NAME from the games table."""
    global TEAM_NAME_TO_ID, TEAM_ID_TO_NAME

    all_teams = pd.unique(pd.concat([df["home_team"], df["away_team"]]))
    # Sort just to keep IDs stable
    sorted_names = sorted(str(t) for t in all_teams)

    TEAM_NAME_TO_ID = {name: i + 1 for i, name in enumerate(sorted_names)}
    TEAM_ID_TO_NAME = {v: k for k, v in TEAM_NAME_TO_ID.items()}

    logger.info("Built team lookups for %d NBA teams.", len(TEAM_NAME_TO_ID))

def log_prediction_row(row: pd.Series, p_home: float, p_away: float) -> None:
    """Append a prediction to the in-memory log for admin/debug."""
    item = PredictionLogItem(
        game_id=int(row["game_id"]),
        date=str(row["date"].date()),
        home_team=str(row["home_team"]),
        away_team=str(row["away_team"]),
        p_home=float(p_home),
        p_away=float(p_away),
        created_at=datetime.utcnow().isoformat() + "Z",
    )
    RECENT_PREDICTIONS.appendleft(item)


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
    has_prediction: bool = True

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_win: Optional[bool] = None
    model_home_win_prob: Optional[float] = None
    model_away_win_prob: Optional[float] = None
    model_home_american_odds: Optional[float] = None
    model_away_american_odds: Optional[float] = None


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

class PredictionLogItem(BaseModel):
    game_id: int
    date: str
    home_team: str
    away_team: str
    p_home: float
    p_away: float
    created_at: str

RECENT_PREDICTIONS: deque[PredictionLogItem]  # type: ignore


class PredictionLogResponse(BaseModel):
    items: List[PredictionLogItem]


# --- Prediction History Models ---
class PredictionHistoryItem(BaseModel):
    game_id: int
    date: str
    home_team: str
    away_team: str
    p_home: float
    p_away: float
    home_win: int
    model_pick: str  # "home" or "away"
    is_correct: bool
    edge: float

class PredictionHistoryResponse(BaseModel):
    items: List[PredictionHistoryItem]


class GameDebugRow(BaseModel):
    game_id: int
    data: Dict[str, Optional[float | str | int | bool]]

class MetricsResponse(BaseModel):
    num_games: int
    accuracy: float
    brier_score: float

@app.get("/debug/games/{game_id}", response_model=GameDebugRow)
def debug_game_row(game_id: int) -> GameDebugRow:
    games = load_games_table()
    if "game_id" not in games.columns:
        raise HTTPException(status_code=500, detail="game_id column missing in games table")

    match = games[games["game_id"] == game_id]
    if match.empty:
        raise HTTPException(status_code=404, detail=f"No game found with game_id={game_id}")

    row = match.iloc[0]

    # convert to plain dict for JSON (strings, floats, etc.)
    payload = {}
    for col, val in row.items():
        if isinstance(val, (pd.Timestamp, datetime)):
            payload[col] = val.isoformat()
        elif pd.isna(val):
            payload[col] = None
        else:
            payload[col] = val.item() if hasattr(val, "item") else val

    return GameDebugRow(
        game_id=int(row["game_id"]),
        data=payload,
    )
    

# --- Insight helpers (NEW) -----------------------------------------

def compute_insight_features(row: pd.Series) -> pd.Series:
    """
    Compute the extra feature differences we used in the notebook:
    - season_wp_diff
    - recent_wp_diff
    - rest_diff
    - last_pd_diff
    """
    r = row.copy()

    r["season_wp_diff"] = r["home_season_win_pct"] - r["away_season_win_pct"]
    r["recent_wp_diff"] = (
        r["home_recent_win_pct_20g"] - r["away_recent_win_pct_20g"]
    )
    r["rest_diff"] = r["home_days_rest"] - r["away_days_rest"]
    r["last_pd_diff"] = r["home_last_pd"] - r["away_last_pd"]

    return r


def build_feature_insights(row: pd.Series) -> List[InsightItem]:
    """
    Derive feature-based insights from a single game row:
    - season win% difference
    - recent 20-game form
    - rest (days)
    - back-to-back flags
    - last-game point differential
    """
    insights: List[InsightItem] = []

    home_team = str(row["home_team"])
    away_team = str(row["away_team"])

    def safe(col: str) -> Optional[float]:
        """Return float(row[col]) or None if missing/NaN."""
        val = row.get(col, None)
        if val is None or pd.isna(val):
            return None
        return float(val)

    # --- Season strength (overall season win %) ---
    home_season = safe("home_season_win_pct")
    away_season = safe("away_season_win_pct")

    if home_season is not None and away_season is not None:
        season_wp_diff = home_season - away_season
        if abs(season_wp_diff) > 0.15:
            if season_wp_diff > 0:
                insights.append(
                    InsightItem(
                        type="season_strength",
                        label="Season strength",
                        detail=(
                            f"{home_team} have a stronger season performance "
                            f"(+{season_wp_diff:.1%} win rate vs {away_team})."
                        ),
                        value=home_season,
                    )
                )
            else:
                insights.append(
                    InsightItem(
                        type="season_strength",
                        label="Season strength",
                        detail=(
                            f"{away_team} have a stronger season performance "
                            f"(+{-season_wp_diff:.1%} win rate vs {home_team})."
                        ),
                        value=away_season,
                    )
                )

    # --- Recent form (last 20 games win %) ---
    home_recent = safe("home_recent_win_pct_20g")
    away_recent = safe("away_recent_win_pct_20g")

    if home_recent is not None and away_recent is not None:
        recent_wp_diff = home_recent - away_recent
        if abs(recent_wp_diff) > 0.20:
            if recent_wp_diff > 0:
                insights.append(
                    InsightItem(
                        type="recent_form",
                        label="Recent form",
                        detail=(
                            f"{home_team} are in better recent form over the last 20 games."
                        ),
                        value=home_recent,
                    )
                )
            else:
                insights.append(
                    InsightItem(
                        type="recent_form",
                        label="Recent form",
                        detail=(
                            f"{away_team} are in better recent form over the last 20 games."
                        ),
                        value=away_recent,
                    )
                )

    # --- Rest difference (days since last game) ---
    home_rest = safe("home_days_rest")
    away_rest = safe("away_days_rest")

    if (
        home_rest is not None
        and away_rest is not None
        and home_rest < 20
        and away_rest < 20
    ):
        rest_diff = home_rest - away_rest
        if abs(rest_diff) >= 2:
            if rest_diff > 0:
                insights.append(
                    InsightItem(
                        type="rest",
                        label="Rest advantage",
                        detail=(
                            f"{home_team} are more rested (+{rest_diff:.0f} days) than "
                            f"{away_team}."
                        ),
                        value=home_rest,
                    )
                )
            else:
                insights.append(
                    InsightItem(
                        type="rest",
                        label="Rest advantage",
                        detail=(
                            f"{away_team} are more rested (+{-rest_diff:.0f} days) than "
                            f"{home_team}."
                        ),
                        value=away_rest,
                    )
                )

    # --- Back-to-back fatigue flags ---
    home_b2b = safe("home_b2b")
    away_b2b = safe("away_b2b")

    if home_b2b == 1.0:
        insights.append(
            InsightItem(
                type="fatigue",
                label="Fatigue",
                detail=f"{home_team} are on a back-to-back (may be more fatigued).",
                value=None,
            )
        )
    if away_b2b == 1.0:
        insights.append(
            InsightItem(
                type="fatigue",
                label="Fatigue",
                detail=f"{away_team} are on a back-to-back (may be more fatigued).",
                value=None,
            )
        )

    # --- Last-game performance (point diff) ---
    home_last_pd = safe("home_last_pd")
    away_last_pd = safe("away_last_pd")

    if home_last_pd is not None and away_last_pd is not None:
        last_pd_diff = home_last_pd - away_last_pd
        if last_pd_diff > 0.5:
            insights.append(
                InsightItem(
                    type="momentum",
                    label="Momentum",
                    detail=f"{home_team} had a stronger last game performance.",
                    value=home_last_pd,
                )
            )
        elif last_pd_diff < -0.5:
            insights.append(
                InsightItem(
                    type="momentum",
                    label="Momentum",
                    detail=f"{away_team} had a stronger last game performance.",
                    value=away_last_pd,
                )
            )

    return insights


# --- Startup hook --------------------------------------------------

@app.on_event("startup")
def startup_event():
    """
    Warm up the API:
    - Load games table
    - Build team lookups
    - Load model artifact
    """
    logger.info("Startup: loading games table, team lookups, and NBA model.")
    games = load_games_table()
    build_team_lookups(games)
    _ = load_nba_model()
    logger.info(
        "Startup complete: games + model + lookups loaded (num_games=%d).",
        len(games),
    )


# --- Core routes ---------------------------------------------------

@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    logger.info("Health check requested.")
    games = load_games_table()
    try:
        artifact = load_nba_model()
        model_loaded = artifact is not None
    except Exception:
        logger.exception("Error while loading NBA model during health check.")
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
def list_events(
    limit: int | None = None,
    year: int | None = None,
) -> ListEventsResponse:
    """
    Return a list of NBA events derived from the processed games table.

    We ignore the Event ORM here and instead:
    - pull games from the parquet via load_games_table()
    - optionally filter by calendar year
    - sort newest first
    - map home/away team names to IDs using TEAM_NAME_TO_ID
    - surface final scores + home_win flag when available
    """
    games = load_games_table()
    df = games

    # Ensure date column is datetime-like
    if not hasattr(df["date"], "dt"):
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

    if year is not None:
        df = df[df["date"].dt.year == year]

    # Newest games first, limited
    df = df.sort_values(["date", "game_id"], ascending=[False, False])
    if limit is not None: 
        df = df.head(limit)

    items: List[EventOut] = []
    for _, row in df.iterrows():
        # Safely pull score + outcome columns if present
        home_score = None
        away_score = None
        home_win = None

        if "home_pts" in row and pd.notna(row["home_pts"]):
            home_score = int(row["home_pts"])
        if "away_pts" in row and pd.notna(row["away_pts"]):
            away_score = int(row["away_pts"])
        if "home_win" in row and not pd.isna(row["home_win"]):
            try:
                home_win = bool(int(row["home_win"]))
            except (ValueError, TypeError):
                home_win = None

        # Determine status based on whether we have final scores
        if home_score is not None and away_score is not None:
            status = "final"
        else:
            status = "scheduled"

        # Safely pull model probability / odds columns if present
        model_home_win_prob = None
        model_away_win_prob = None
        model_home_american_odds = None
        model_away_american_odds = None

        if "model_home_win_prob" in row and pd.notna(row["model_home_win_prob"]):
            model_home_win_prob = float(row["model_home_win_prob"])
        if "model_away_win_prob" in row and pd.notna(row["model_away_win_prob"]):
            model_away_win_prob = float(row["model_away_win_prob"])
        if "model_home_american_odds" in row and pd.notna(row["model_home_american_odds"]):
            model_home_american_odds = float(row["model_home_american_odds"])
        if "model_away_american_odds" in row and pd.notna(row["model_away_american_odds"]):
            model_away_american_odds = float(row["model_away_american_odds"])

        items.append(
            EventOut(
                event_id=int(row["game_id"]),
                sport_id=SPORT_ID_NBA,
                date=str(row["date"].date()),
                home_team_id=TEAM_NAME_TO_ID.get(str(row["home_team"])),
                away_team_id=TEAM_NAME_TO_ID.get(str(row["away_team"])),
                venue=None,
                status=status,
                start_time=None,
                home_score=home_score,
                away_score=away_score,
                home_win=home_win,
                model_home_win_prob=model_home_win_prob,
                model_away_win_prob=model_away_win_prob,
                model_home_american_odds=model_home_american_odds,
                model_away_american_odds=model_away_american_odds,
            )
        )

    return ListEventsResponse(items=items)
    



@app.get("/events/{event_id}", response_model=EventOut)
def get_event(event_id: int) -> EventOut:
    """Return a single event by its id, including final score + outcome when available."""
    games = load_games_table()

    if "game_id" not in games.columns:
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    match = games[games["game_id"] == event_id]
    if match.empty:
        raise HTTPException(status_code=404, detail=f"No event found with id={event_id}")

    row = match.iloc[0]

    home_score = None
    away_score = None
    home_win = None

    if "home_pts" in row and pd.notna(row["home_pts"]):
        home_score = int(row["home_pts"])
    if "away_pts" in row and pd.notna(row["away_pts"]):
        away_score = int(row["away_pts"])
    if "home_win" in row and not pd.isna(row["home_win"]):
        try:
            home_win = bool(int(row["home_win"]))
        except (ValueError, TypeError):
            home_win = None

    # Determine status based on whether we have final scores
    if home_score is not None and away_score is not None:
        status = "final"
    else:
        status = "scheduled"

    # Safely pull model probability / odds columns if present
    model_home_win_prob = None
    model_away_win_prob = None
    model_home_american_odds = None
    model_away_american_odds = None

    if "model_home_win_prob" in row and pd.notna(row["model_home_win_prob"]):
        model_home_win_prob = float(row["model_home_win_prob"])
    if "model_away_win_prob" in row and pd.notna(row["model_away_win_prob"]):
        model_away_win_prob = float(row["model_away_win_prob"])
    if "model_home_american_odds" in row and pd.notna(row["model_home_american_odds"]):
        model_home_american_odds = float(row["model_home_american_odds"])
    if "model_away_american_odds" in row and pd.notna(row["model_away_american_odds"]):
        model_away_american_odds = float(row["model_away_american_odds"])

    return EventOut(
        event_id=int(row["game_id"]),
        sport_id=SPORT_ID_NBA,
        date=str(row["date"].date()),
        home_team_id=TEAM_NAME_TO_ID.get(str(row["home_team"])),
        away_team_id=TEAM_NAME_TO_ID.get(str(row["away_team"])),
        venue=None,
        status=status,
        start_time=None,
        home_score=home_score,
        away_score=away_score,
        home_win=home_win,
        model_home_win_prob=model_home_win_prob,
        model_away_win_prob=model_away_win_prob,
        model_home_american_odds=model_home_american_odds,
        model_away_american_odds=model_away_american_odds,
    )


@app.get("/predict_by_game_id", response_model=PredictionResponse)
def predict_by_game_id(game_id: int) -> PredictionResponse:
    """
    Predict home win probability for a game by its game_id.
    """
    logger.info("Predict by game_id called for game_id=%s", game_id)
    games = load_games_table()

    if "game_id" not in games.columns:
        logger.error("game_id column missing in games table during /predict_by_game_id.")
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    match = games[games["game_id"] == game_id]

    if match.empty:
        logger.warning("No game found with game_id=%s", game_id)
        raise HTTPException(
            status_code=404,
            detail=f"No game found with game_id={game_id}",
        )

    row = match.iloc[0]
    row_for_model = row.fillna(0)

    # Protect the model call so we see useful errors instead of a generic 500
    try:
        p_home = predict_home_win_proba(row_for_model)
    except Exception as e:
        logger.exception("Model prediction failed for game_id=%s", game_id)
        raise HTTPException(
            status_code=500,
            detail=f"Model prediction failed for game_id={game_id}: {e}",
        )

    p_away = 1.0 - p_home

    logger.info(
        "Prediction for game_id=%s -> p_home=%.3f, p_away=%.3f",
        game_id,
        p_home,
        p_away,
    )

    log_prediction_row(row, p_home, p_away)

    session: Session = SessionLocal()
    try:
        pred_row = Prediction(
            game_id=int(row["game_id"]),
            model_key="nba_logreg_b2b_v1",
            p_home=float(p_home),
            p_away=float(p_away),
        )
        session.add(pred_row)
        session.commit()
    except Exception:
        # Don't blow up the API if logging fails; just log the error.
        logger.exception("Failed to log prediction to DB for game_id=%s", game_id)
        session.rollback()
    finally:
        session.close()

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
    Also logs the prediction to RECENT_PREDICTIONS.
    """
    logger.info(
        "Predict by teams called for %s vs %s on %s",
        home_team,
        away_team,
        game_date,
    )
    games = load_games_table()

    # Filter by date and teams
    mask = (
        (games["date"].dt.date == game_date)
        & (games["home_team"] == home_team)
        & (games["away_team"] == away_team)
    )

    candidates = games[mask]

    if candidates.empty:
        logger.warning(
            "No game found for %s %s vs %s",
            game_date,
            home_team,
            away_team,
        )
        raise HTTPException(
            status_code=404,
            detail=f"No game found for {game_date} {home_team} vs {away_team}",
        )

    # If multiple rows match, just pick the first for now
    row = candidates.iloc[0]
    row_for_model = row.fillna(0)

    try:
        p_home = predict_home_win_proba(row_for_model)
    except Exception as e:
        logger.exception(
            "Model prediction failed for %s vs %s on %s",
            home_team,
            away_team,
            game_date,
        )
        raise HTTPException(
            status_code=500,
            detail=(
                f"Model prediction failed for {game_date} "
                f"{home_team} vs {away_team}: {e}"
            ),
        )

    p_away = 1.0 - p_home

    logger.info(
        "Prediction for %s vs %s on %s -> p_home=%.3f, p_away=%.3f",
        home_team,
        away_team,
        game_date,
        p_home,
        p_away,
    )

    # log it for the admin recent-predictions panel
    log_prediction_row(row, p_home, p_away)

    game_id = int(row["game_id"]) if "game_id" in row else -1

    return PredictionResponse(
        game_id=game_id,
        date=str(row["date"].date()),
        home_team=str(row["home_team"]),
        away_team=str(row["away_team"]),
        p_home=float(p_home),
        p_away=float(p_away),
    )

# --- NBA-specific convenience routes for the web app ---------------

class NBAPredictRequest(BaseModel):
    game_id: int


@app.post("/predict/nba", response_model=PredictionResponse)
def predict_nba(req: NBAPredictRequest) -> PredictionResponse:
    """
    Thin wrapper so the web app can POST /predict/nba with a JSON body:
      { "game_id": 12171 }

    Internally just calls the existing predict_by_game_id() logic.
    """
    return predict_by_game_id(req.game_id)



# --- Insights ------------------------------------------------------

@app.get("/insights/{game_id}", response_model=InsightsResponse)
def game_insights(game_id: int) -> InsightsResponse:
    """
    Return model-driven insights for a given game_id.

    Combines:
    - base win-probability insights (favorite / edge / home court)
    - feature-based insights from season, recent form, rest, B2B, last game
    """
    logger.info("Insights requested for game_id=%s", game_id)
    games = load_games_table()

    if "game_id" not in games.columns:
        logger.error("game_id column missing in games table during /insights.")
        raise HTTPException(
            status_code=500, detail="game_id column missing in games table"
        )

    match = games[games["game_id"] == game_id]
    if match.empty:
        logger.warning("No game found with game_id=%s for /insights", game_id)
        raise HTTPException(
            status_code=404,
            detail=f"No game found with game_id={game_id}",
        )

    row = match.iloc[0]
    row_for_model = row.fillna(0)

    # Use the same model as /predict_by_game_id
    try:
        p_home = float(predict_home_win_proba(row_for_model))
    except Exception as e:
        logger.exception("Model prediction (for insights) failed for game_id=%s", game_id)
        raise HTTPException(
            status_code=500,
            detail=f"Model prediction failed for game_id={game_id} while building insights: {e}",
        )

    p_away = float(1.0 - p_home)

    home_team = str(row["home_team"])
    away_team = str(row["away_team"])

    edge = abs(p_home - p_away)
    home_is_fav = p_home >= p_away

    insights: List[InsightItem] = []

    # --- 1) Favorite / underdog ---
    fav_team = home_team if home_is_fav else away_team
    dog_team = away_team if home_is_fav else home_team
    fav_prob = p_home if home_is_fav else p_away

    insights.append(
        InsightItem(
            type="favorite",
            label="Favorite",
            detail=(
                f"{fav_team} are favored over {dog_team} "
                f"with a win probability of {fav_prob:.1%}."
            ),
            value=fav_prob,
        )
    )

    # --- 2) Edge strength (how close the game is) ---
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

    # --- 3) Home court ---
    insights.append(
        InsightItem(
            type="home_court",
            label="Home court",
            detail=(
                f"{home_team} are at home. The model gives them a "
                f"win probability of {p_home:.1%}."
            ),
            value=p_home,
        )
    )

    # --- 4) Feature-based insights from season / recent form / rest / momentum ---
    insights.extend(build_feature_insights(row))

    return InsightsResponse(
        game_id=int(row["game_id"]),
        model_key="nba_logreg_b2b_v1",
        generated_at=datetime.utcnow().isoformat() + "Z",
        insights=insights,
    )


@app.get("/insights/nba/{event_id}", response_model=InsightsResponse)
def game_insights_nba(event_id: int) -> InsightsResponse:
    """
    Convenience wrapper for the web app expecting GET /insights/nba/{event_id}.
    Reuses the core game_insights() implementation.
    """
    return game_insights(event_id)


@app.get("/predictions", response_model=PredictionLogResponse)
def list_predictions(limit: int = 20) -> PredictionLogResponse:
    """Return most recent logged predictions for admin/debug."""
    items = list(RECENT_PREDICTIONS)[:limit]
    logger.info(
        "Listing %d recent predictions (requested limit=%d).",
        len(items),
        limit,
    )
    return PredictionLogResponse(items=items)


@app.get("/prediction_history", response_model=PredictionHistoryResponse)
def prediction_history(limit: int = 200) -> PredictionHistoryResponse:
    """
    Return per-game prediction history joined with ground truth.

    For each game where:
      - we have a logged prediction in the DB, and
      - the games table has a home_win label,
    we return the matchup, probabilities, actual result, and a correctness flag.
    """
    games = load_games_table()

    if "game_id" not in games.columns or "home_win" not in games.columns:
        raise HTTPException(
            status_code=500,
            detail="games table must include game_id and home_win columns for prediction history",
        )

    # index games by game_id for fast lookups
    games_by_id = games.set_index("game_id")

    session: Session = SessionLocal()
    try:
        # order by game_id descending as a simple proxy for recency,
        # and cap to `limit` rows
        preds: List[Prediction] = (
            session.query(Prediction)
            .order_by(Prediction.game_id.desc())
            .limit(limit)
            .all()
        )
    finally:
        session.close()

    items: List[PredictionHistoryItem] = []

    for p in preds:
        gid = p.game_id
        if gid not in games_by_id.index:
            continue

        row = games_by_id.loc[gid]

        # ground truth: 1 if home actually won, 0 otherwise
        home_win = int(row["home_win"])

        # predicted probabilities
        p_home = float(p.p_home)
        p_away = float(p.p_away)

        # model pick at a 0.5 threshold
        model_pick = "home" if p_home >= 0.5 else "away"
        predicted_label = 1 if model_pick == "home" else 0
        is_correct = predicted_label == home_win

        edge = abs(p_home - p_away)

        # date handling: if it's a Timestamp, convert to date string
        date_val = row["date"]
        if hasattr(date_val, "date"):
            date_str = str(date_val.date())
        else:
            date_str = str(date_val)

        items.append(
            PredictionHistoryItem(
                game_id=int(gid),
                date=date_str,
                home_team=str(row["home_team"]),
                away_team=str(row["away_team"]),
                p_home=p_home,
                p_away=p_away,
                home_win=home_win,
                model_pick=model_pick,
                is_correct=is_correct,
                edge=edge,
            )
        )

    return PredictionHistoryResponse(items=items)




@app.get("/metrics", response_model=MetricsResponse)
def metrics() -> MetricsResponse:
    """
    Compute simple accuracy + Brier score for all games
    where we have both:
      - a logged prediction in the DB
      - a ground-truth home_win label in the games table

    If the predictions table doesn't exist yet, we just return zeros
    instead of throwing a 500.
    """
    games = load_games_table()

    if "game_id" not in games.columns or "home_win" not in games.columns:
        raise HTTPException(
            status_code=500,
            detail=(
                "games table must include game_id and home_win columns for metrics"
            ),
        )

    # index games by game_id for fast lookups
    games_by_id = games.set_index("game_id")

    session: Session = SessionLocal()
    try:
        try:
            preds: List[Prediction] = session.query(Prediction).all()
        except OperationalError:
            logger.exception(
                "metrics(): predictions table missing; returning empty metrics."
            )
            return MetricsResponse(num_games=0, accuracy=0.0, brier_score=0.0)
    finally:
        session.close()

    if not preds:
        return MetricsResponse(num_games=0, accuracy=0.0, brier_score=0.0)

    total = 0
    num_correct = 0
    brier_sum = 0.0

    for p in preds:
        gid = p.game_id
        if gid not in games_by_id.index:
            continue

        row = games_by_id.loc[gid]

        # ground truth: 1 if home actually won, 0 otherwise
        home_win = int(row["home_win"])  # assumes 0/1 in your parquet

        # predicted probability of home win
        p_home = float(p.p_home)

        # classification accuracy: did we pick the right side at 0.5 threshold?
        predicted_label = 1 if p_home >= 0.5 else 0
        if predicted_label == home_win:
            num_correct += 1

        # Brier score contribution
        brier_sum += (p_home - home_win) ** 2

        total += 1

    if total == 0:
        return MetricsResponse(num_games=0, accuracy=0.0, brier_score=0.0)

    accuracy = num_correct / total
    brier_score = brier_sum / total

    return MetricsResponse(
        num_games=total,
        accuracy=accuracy,
        brier_score=brier_score,
    )

