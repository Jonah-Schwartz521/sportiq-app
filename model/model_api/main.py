from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Optional
from pathlib import Path
import sys
import pandas as pd
from datetime import date as date_type

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


def load_games_table() -> pd.DataFrame:
    global GAMES_DF
    if GAMES_DF is None:
        path = PROCESSED_DIR / "processed_games_b2b_model.parquet"
        print(f"Loading games table from {path} ...")
        GAMES_DF = pd.read_parquet(path)

        if "game_id" not in GAMES_DF.columns:
            GAMES_DF = (
                GAMES_DF.sort_values(["date", "home_team", "away_team"])
                .reset_index(drop=True)
            )
            GAMES_DF["game_id"] = GAMES_DF.index.astype(int)

    return GAMES_DF


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


# --- Startup hook --------------------------------------------------

@app.on_event("startup")
def startup_event():
    _ = load_games_table()
    _ = load_nba_model()
    print("Startup complete: games + model loaded.")


# --- Routes --------------------------------------------------------

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


@app.get("/predict_by_game_id", response_model=PredictionResponse)
def predict_by_game_id(game_id: int):
    games = load_games_table()

    if "game_id" not in games.columns:
        raise HTTPException(status_code=500, detail="game_id column missing in games table")

    match = games[games["game_id"] == game_id]

    if match.empty:
        raise HTTPException(status_code=404, detail=f"No game found with game_id={game_id}")

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
):
    games = load_games_table()

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