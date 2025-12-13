from __future__ import annotations

from datetime import date as date_type, datetime
from pathlib import Path
from typing import Optional, Dict, List

from collections import deque
import os  # NEW: for reading environment variables

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
import requests  # NEW: for calling the Sports Odds API


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

from src.paths import PROCESSED_DIR, MLB_PROCESSED_DIR, NFL_PROCESSED_DIR, NHL_PROCESSED_DIR, UFC_PROCESSED_DIR
from src.nba_inference import load_nba_model, predict_home_win_proba

# --- Global objects (loaded once at startup) -----------------------

GAMES_DF: Optional[pd.DataFrame] = None

# simple in-memory lookup tables
TEAM_NAME_TO_ID: Dict[str, int] = {}
TEAM_ID_TO_NAME: Dict[int, str] = {}
SPORT_ID_NBA = 1
SPORT_ID_MLB = 2
SPORT_ID_NFL = 3
SPORT_ID_NHL = 4
SPORT_ID_UFC = 5
TEAM_ID_TO_SPORT_ID: Dict[int, int] = {}

# --- Sports Odds API config ---------------------------------------
# Default to your free Odds API key, but allow overriding via environment
SPORTS_ODDS_API_KEY = os.getenv(
    "SPORTS_ODDS_API_KEY",
    "38953b0ca0f6dd6c9d60f549190b6cf0",
)

# Feature flag: real odds lookups are DISABLED by default.
# Turn them back on later with ENABLE_REAL_ODDS=1 in your environment.
ENABLE_REAL_ODDS = os.getenv("ENABLE_REAL_ODDS", "0") == "1"

# Simple in-memory cache so we don't hammer the odds API
# key: "YYYY-MM-DD|Home Team|Away Team" -> {"home": float | None, "away": float | None}
REAL_ODDS_CACHE: Dict[str, Dict[str, Optional[float]]] = {}

# Will be assigned after PredictionLogItem is defined
RECENT_PREDICTIONS = deque(maxlen=200)


def load_games_table() -> pd.DataFrame:
    """
    Load the processed games table once.

    - NBA: games_with_scores_and_future.parquet
    - MLB: mlb_model_input.parquet (if present)
    - NFL: nfl_games.parquet (historical) + nfl_future_games.parquet (schedule)

    Guarantees:
    - A 'sport' column exists and is 'NBA', 'MLB', or 'NFL'
    - A numeric 'game_id' exists for every row (no None/NaN)
    - Unified team/score columns:
        home_team, away_team, home_pts, away_pts
    """
    global GAMES_DF
    if GAMES_DF is not None:
        return GAMES_DF

    frames: list[pd.DataFrame] = []

    # --- Load NBA games (primary source) ---
    nba_path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
    logger.info("Loading NBA games table from %s ...", nba_path)
    nba_df = pd.read_parquet(nba_path).copy()

    # BUGFIX: Filter out non-NBA games if the parquet contains multiple leagues
    # The games_with_scores_and_future.parquet file may contain NHL games with league='NHL'.
    # We only want actual NBA games here.
    if "league" in nba_df.columns:
        before_count = len(nba_df)
        nba_df = nba_df[nba_df["league"].fillna("NBA").str.upper() == "NBA"]
        filtered_count = before_count - len(nba_df)
        if filtered_count > 0:
            logger.info(
                "Filtered out %d non-NBA games from NBA parquet (league != 'NBA')",
                filtered_count
            )

    # Ensure we have a sport label for NBA
    if "sport" not in nba_df.columns:
        nba_df["sport"] = "NBA"
    else:
        nba_df["sport"] = nba_df["sport"].fillna("NBA").astype(str).str.upper()

    # Normalize NBA column names into the unified schema
    nba_df = nba_df.rename(
        columns={
            "home_team_name": "home_team",
            "away_team_name": "away_team",
            "home_score": "home_pts",
            "away_score": "away_pts",
        }
    )

    frames.append(nba_df)

    # --- Optionally load MLB games ---
    mlb_path = MLB_PROCESSED_DIR / "mlb_model_input.parquet"
    if mlb_path.exists():
        logger.info("Loading MLB model_input from %s ...", mlb_path)
        mlb_df = pd.read_parquet(mlb_path).copy()

        # Normalize MLB -> unified schema
        mlb_df["sport"] = "MLB"
        mlb_df = mlb_df.rename(
            columns={
                "home_team_name": "home_team",
                "away_team_name": "away_team",
                "home_score": "home_pts",
                "away_score": "away_pts",
            }
        )

        frames.append(mlb_df)
    else:
        logger.info("MLB model_input not found at %s; skipping MLB.", mlb_path)

    # --- NFL historical games ---
    nfl_hist_with_scores = NFL_PROCESSED_DIR / "nfl_games_with_scores.parquet"
    nfl_hist_path = NFL_PROCESSED_DIR / "nfl_games.parquet"
    nfl_hist_source: Path | None = None

    if nfl_hist_with_scores.exists():
        nfl_hist_source = nfl_hist_with_scores
    elif nfl_hist_path.exists():
        nfl_hist_source = nfl_hist_path

    if nfl_hist_source is not None:
        logger.info("Loading NFL historical games from %s ...", nfl_hist_source)
        nfl_hist = pd.read_parquet(nfl_hist_source).copy()

        nfl_hist["sport"] = "NFL"
        rename_map = {
            "home_team_name": "home_team",
            "away_team_name": "away_team",
            "home_score": "home_pts",
            "away_score": "away_pts",
            "gameday": "date",  # Normalize NFL date column
        }
        nfl_hist = nfl_hist.rename(
            columns={k: v for k, v in rename_map.items() if k in nfl_hist.columns}
        )

        # Ensure required columns exist even if schema differs
        for col in ["home_team", "away_team", "home_pts", "away_pts", "date"]:
            if col not in nfl_hist.columns:
                nfl_hist[col] = None

        frames.append(nfl_hist)
    else:
        logger.info(
            "NFL historical games parquet not found at %s or %s; skipping.",
            nfl_hist_with_scores,
            nfl_hist_path,
        )

    # --- NFL future schedule ---
    nfl_future_path = NFL_PROCESSED_DIR / "nfl_future_games.parquet"
    if nfl_future_path.exists():
        logger.info("Loading NFL future schedule from %s ...", nfl_future_path)
        nfl_future = pd.read_parquet(nfl_future_path).copy()

        nfl_future["sport"] = "NFL"
        nfl_future = nfl_future.rename(
            columns={
                "home_team_name": "home_team",
                "away_team_name": "away_team",
                "home_score": "home_pts",
                "away_score": "away_pts",
            }
        )

        # Future rows should have no final scores yet
        if "home_pts" not in nfl_future.columns:
            nfl_future["home_pts"] = None
        if "away_pts" not in nfl_future.columns:
            nfl_future["away_pts"] = None

        # Ensure start_et exists for time display
        if "start_et" not in nfl_future.columns:
            nfl_future["start_et"] = "13:00"  # Default to 1:00 PM ET

        frames.append(nfl_future)
    else:
        logger.info("NFL future schedule parquet not found at %s; skipping.", nfl_future_path)

    # --- NHL games (MoneyPuck data) ---
    nhl_path = NHL_PROCESSED_DIR / "nhl_games_for_app.parquet"
    if nhl_path.exists():
        logger.info("Loading NHL games from %s ...", nhl_path)
        nhl_df = pd.read_parquet(nhl_path).copy()

        nhl_df["sport"] = "NHL"
        nhl_df = nhl_df.rename(
            columns={
                "game_datetime": "date",
                "home_team_name": "home_team",
                "away_team_name": "away_team",
                "home_score": "home_pts",
                "away_score": "away_pts",
            }
        )

        # Ensure score columns exist (future games may not have scores yet)
        if "home_pts" not in nhl_df.columns:
            nhl_df["home_pts"] = None
        if "away_pts" not in nhl_df.columns:
            nhl_df["away_pts"] = None

        frames.append(nhl_df)
    else:
        logger.info("NHL games parquet not found at %s; skipping NHL.", nhl_path)

    # --- UFC fights ---
    ufc_path = UFC_PROCESSED_DIR / "ufc_fights_for_app.parquet"
    if ufc_path.exists():
        logger.info("Loading UFC fights from %s ...", ufc_path)
        ufc_df = pd.read_parquet(ufc_path).copy()

        ufc_df["sport"] = "UFC"
        ufc_df = ufc_df.rename(
            columns={
                "fight_datetime": "date",
                "home_team_name": "home_team",
                "away_team_name": "away_team",
                "home_score": "home_pts",
                "away_score": "away_pts",
            }
        )

        # Ensure score columns exist (all UFC fights should have outcomes)
        if "home_pts" not in ufc_df.columns:
            ufc_df["home_pts"] = None
        if "away_pts" not in ufc_df.columns:
            ufc_df["away_pts"] = None

        frames.append(ufc_df)
    else:
        logger.info("UFC fights parquet not found at %s; skipping UFC.", ufc_path)

    # --- Combine all sports into a single games table ---
    if not frames:
        raise RuntimeError("load_games_table(): no game frames loaded for any sport")

    # 1) Drop duplicate columns within each frame (keep first occurrence)
    cleaned_frames: list[pd.DataFrame] = []
    for f in frames:
        # Ensure unique column labels BEFORE we reindex; duplicate labels
        # can cause `cannot reindex on an axis with duplicate labels` errors.
        f = f.loc[:, ~f.columns.duplicated()]
        cleaned_frames.append(f)

    frames = cleaned_frames

    # 2) Build the superset of columns across all sports
    all_cols: set[str] = set()
    for f in frames:
        all_cols.update(f.columns)

    all_cols_list = sorted(all_cols)

    # 3) Reindex each frame to the same column set
    frames = [f.reindex(columns=all_cols_list) for f in frames]

    # 4) Concatenate into one master games table
    games = pd.concat(frames, ignore_index=True)

    # --- Normalize date column to timezone-naive pandas datetime ---
    if "date" in games.columns:
        # Convert any mix of strings / python datetimes (with or without tz)
        # into a unified datetime64[ns] column, dropping timezone info.
        games = games.copy()
        games["date"] = pd.to_datetime(games["date"], utc=True, errors="coerce")
        # Drop timezone info so downstream `.dt` access works without errors
        games["date"] = games["date"].dt.tz_localize(None)

    logger.info(
        "Loaded combined games table with %d rows (NBA=%d, MLB=%d, NFL=%d, NHL=%d, UFC=%d).",
        len(games),
        (games["sport"] == "NBA").sum() if "sport" in games.columns else 0,
        (games["sport"] == "MLB").sum() if "sport" in games.columns else 0,
        (games["sport"] == "NFL").sum() if "sport" in games.columns else 0,
        (games["sport"] == "NHL").sum() if "sport" in games.columns else 0,
        (games["sport"] == "UFC").sum() if "sport" in games.columns else 0,
    )

    # --- Ensure a clean numeric game_id for every row ---
    if "game_id" not in games.columns:
        games = games.sort_values(["date", "home_team", "away_team"]).reset_index(drop=True)
        games["game_id"] = games.index.astype(int)
    else:
        games = games.copy()
        games["game_id"] = pd.to_numeric(games["game_id"], errors="coerce")

        max_existing = games["game_id"].max()
        if pd.isna(max_existing):
            next_id = 1
        else:
            next_id = int(max_existing) + 1

        missing_mask = games["game_id"].isna()
        num_missing = int(missing_mask.sum())
        if num_missing > 0:
            logger.info(
                "Assigning %d missing game_id values starting at %d.",
                num_missing,
                next_id,
            )
            games.loc[missing_mask, "game_id"] = range(next_id, next_id + num_missing)

        games["game_id"] = games["game_id"].astype(int)

    GAMES_DF = games
    return GAMES_DF


def build_team_lookups(df: pd.DataFrame) -> None:
    """
    Build TEAM_NAME_TO_ID / TEAM_ID_TO_NAME / TEAM_ID_TO_SPORT_ID from the games table.

    We derive teams from both home and away columns, and keep a primary sport_id
    for each team name based on the 'sport' column.
    """
    global TEAM_NAME_TO_ID, TEAM_ID_TO_NAME, TEAM_ID_TO_SPORT_ID

    if "home_team" not in df.columns or "away_team" not in df.columns:
        logger.error("build_team_lookups: games table missing home_team/away_team columns.")
        TEAM_NAME_TO_ID = {}
        TEAM_ID_TO_NAME = {}
        TEAM_ID_TO_SPORT_ID = {}
        return

    # Build a long frame of (team, sport) pairs from home/away
    frames = []

    if "sport" in df.columns:
        frames.append(
            df[["home_team", "sport"]]
            .rename(columns={"home_team": "team"})
            .dropna(subset=["team"])
        )
        frames.append(
            df[["away_team", "sport"]]
            .rename(columns={"away_team": "team"})
            .dropna(subset=["team"])
        )
        pairs = pd.concat(frames, ignore_index=True)
    else:
        # Fallback: no sport column, treat everything as NBA
        tmp = pd.concat(
            [
                df[["home_team"]].rename(columns={"home_team": "team"}),
                df[["away_team"]].rename(columns={"away_team": "team"}),
            ],
            ignore_index=True,
        ).dropna(subset=["team"])
        tmp["sport"] = "NBA"
        pairs = tmp

    # Deduplicate (team, sport) pairs
    pairs["team"] = pairs["team"].astype(str)
    pairs["sport"] = pairs["sport"].astype(str).str.upper()
    pairs = pairs.drop_duplicates(subset=["team", "sport"])

    # Sort by name for stable ids
    pairs = pairs.sort_values(["team", "sport"]).reset_index(drop=True)

    TEAM_NAME_TO_ID = {}
    TEAM_ID_TO_NAME = {}
    TEAM_ID_TO_SPORT_ID = {}

    next_id = 1
    for _, row in pairs.iterrows():
        name = row["team"]
        sport_str = row["sport"]

        if name in TEAM_NAME_TO_ID:
            # Already assigned an id for this team name; keep the first one.
            continue

        team_id = next_id
        next_id += 1

        TEAM_NAME_TO_ID[name] = team_id
        TEAM_ID_TO_NAME[team_id] = name

        if sport_str == "MLB":
            sport_id = SPORT_ID_MLB
        elif sport_str == "NFL":
            sport_id = SPORT_ID_NFL
        elif sport_str == "NHL":
            sport_id = SPORT_ID_NHL
        elif sport_str == "UFC":
            sport_id = SPORT_ID_UFC
        else:
            sport_id = SPORT_ID_NBA

        TEAM_ID_TO_SPORT_ID[team_id] = sport_id

    logger.info("Built team lookups for %d teams.", len(TEAM_NAME_TO_ID))

def compute_event_status(row: pd.Series) -> str:
    """
    Compute event status based on date, time, and scores.

    Rules:
    - If game_dt.date < today: "final" if scores exist, else "scheduled"
    - If game_dt.date > today: "upcoming"
    - If game_dt.date == today:
        - If game_dt > now: "upcoming"
        - Else if scores exist: "in_progress"
        - Else: "upcoming" (safety fallback)

    Returns: "final", "in_progress", or "upcoming"
    """
    from datetime import timezone
    from zoneinfo import ZoneInfo

    # Get current time in ET
    et_tz = ZoneInfo("America/New_York")
    now_et = datetime.now(et_tz)
    today_et = now_et.date()

    # Parse the game date (YYYY-MM-DD string)
    game_date_str = str(row.get("date", "")).split("T")[0]  # handle both date and datetime strings
    try:
        game_date_parts = game_date_str.split("-")
        game_date = date_type(
            int(game_date_parts[0]),
            int(game_date_parts[1]),
            int(game_date_parts[2])
        )
    except (ValueError, IndexError, AttributeError):
        # If we can't parse the date, fall back to score-based status
        has_scores = pd.notna(row.get("home_pts")) and pd.notna(row.get("away_pts"))
        return "final" if has_scores else "upcoming"

    # Try to get the game time (start_et field, format like "19:00" or "7:00 PM")
    start_time_str = row.get("start_et")
    game_dt_et = None

    if start_time_str and pd.notna(start_time_str):
        # Parse time string (could be "19:00" or "7:00 PM" format)
        try:
            # Try parsing as HH:MM 24-hour format
            time_parts = str(start_time_str).split(":")
            hour = int(time_parts[0])
            minute = int(time_parts[1].split()[0]) if len(time_parts) > 1 else 0

            # Create datetime in ET
            game_dt_et = datetime(
                game_date.year,
                game_date.month,
                game_date.day,
                hour,
                minute,
                tzinfo=et_tz
            )
        except (ValueError, AttributeError, IndexError):
            # If time parsing fails, we'll just use date comparison
            pass

    # Check if we have final scores
    has_scores = pd.notna(row.get("home_pts")) and pd.notna(row.get("away_pts"))

    # Apply status logic based on date
    if game_date < today_et:
        # Past game
        return "final" if has_scores else "scheduled"
    elif game_date > today_et:
        # Future game
        return "upcoming"
    else:
        # Today's game - check tipoff time
        start_time_str = row.get("start_et")
        if start_time_str and pd.notna(start_time_str):
            try:
                # Parse "HH:MM" 24-hour format
                time_str = str(start_time_str).strip()
                parts = time_str.split(":")
                hour = int(parts[0])
                minute = int(parts[1]) if len(parts) > 1 else 0

                tipoff_et = datetime(
                    game_date.year, game_date.month, game_date.day,
                    hour, minute, tzinfo=et_tz
                )

                if now_et < tipoff_et:
                    return "upcoming"
                else:
                    return "in_progress" if not has_scores else "final"
            except (ValueError, AttributeError, IndexError):
                pass

        # Fallback for today without valid time
        return "upcoming" if not has_scores else "final"


def format_start_time_display(row: pd.Series) -> str | None:
    """
    Format start_et field (24-hour "HH:MM") into display string like "7:00 PM ET"
    """
    start_time_str = row.get("start_et")
    if not start_time_str or pd.isna(start_time_str):
        return None

    try:
        time_str = str(start_time_str).strip()

        # Parse "HH:MM" format
        parts = time_str.split(":")
        hour = int(parts[0])
        minute = parts[1] if len(parts) > 1 else "00"

        # Convert to 12-hour format
        if hour == 0:
            display = f"12:{minute} AM ET"
        elif hour < 12:
            display = f"{hour}:{minute} AM ET"
        elif hour == 12:
            display = f"12:{minute} PM ET"
        else:
            display = f"{hour - 12}:{minute} PM ET"

        return display
    except (ValueError, AttributeError, IndexError):
        return None


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


# --- Sportsbook odds helpers --------------------------------------

def _odds_cache_key(game_date_str: str, home_team: str, away_team: str) -> str:
    """Build a stable cache key for a game’s odds."""
    return f"{game_date_str}|{home_team}|{away_team}"


def fetch_real_odds_for_game(
    game_date: date_type | datetime | str,
    home_team: str,
    away_team: str,
) -> tuple[Optional[float], Optional[float]]:
    """
    Fetch real American odds for a single NBA game from The Odds API.

    Returns (home_american_odds, away_american_odds), or (None, None)
    if odds cannot be found or lookups are disabled.
    """
    # Respect the feature flag
    if not ENABLE_REAL_ODDS:
        logger.debug(
            "Real odds lookup disabled (ENABLE_REAL_ODDS!=1); "
            "returning None for sportsbook odds."
        )
        return None, None

    # If no API key, bail out
    if not SPORTS_ODDS_API_KEY:
        logger.warning(
            "ENABLE_REAL_ODDS=1 but SPORTS_ODDS_API_KEY is not set; "
            "skipping real odds lookup."
        )
        return None, None

    # Normalize the game date to YYYY-MM-DD string
    if isinstance(game_date, datetime):
        date_str = game_date.date().isoformat()
    elif isinstance(game_date, date_type):
        date_str = game_date.isoformat()
    else:
        # string or pandas Timestamp – try to extract date portion
        try:
            # pandas Timestamp has .date(), others we just split on "T"
            if hasattr(game_date, "date"):
                date_str = game_date.date().isoformat()
            else:
                date_str = str(game_date).split("T")[0]
        except Exception:
            date_str = str(game_date)

    cache_key = _odds_cache_key(date_str, home_team, away_team)
    if cache_key in REAL_ODDS_CACHE:
        cached = REAL_ODDS_CACHE[cache_key]
        return cached.get("home"), cached.get("away")

    # Call The Odds API
    url = "https://api.the-odds-api.com/v4/sports/basketball_nba/odds"
    params = {
        "apiKey": SPORTS_ODDS_API_KEY,
        "regions": "us",
        "markets": "h2h",
        "oddsFormat": "american",
        "dateFormat": "iso",
    }

    def _norm_name(name: str) -> str:
        return (
            name.lower()
            .replace(".", "")
            .replace("-", " ")
            .replace("&", "and")
            .strip()
        )

    try:
        resp = requests.get(url, params=params, timeout=5)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.HTTPError as e:
        logger.error(
            "Error fetching real odds for %s vs %s on %s: %s",
            home_team,
            away_team,
            date_str,
            e,
        )
        REAL_ODDS_CACHE[cache_key] = {"home": None, "away": None}
        return None, None
    except Exception as e:
        logger.exception(
            "Unexpected error calling The Odds API for %s vs %s on %s",
            home_team,
            away_team,
            date_str,
        )
        REAL_ODDS_CACHE[cache_key] = {"home": None, "away": None}
        return None, None

    home_norm = _norm_name(home_team)
    away_norm = _norm_name(away_team)

    home_price: Optional[float] = None
    away_price: Optional[float] = None

    # data is a list of games
    for game in data:
        api_home = _norm_name(game.get("home_team", ""))
        api_away = _norm_name(game.get("away_team", ""))

        # Quick team-name check
        if {api_home, api_away} != {home_norm, away_norm}:
            continue

        # Optional: filter by date using commence_time
        commence_time = game.get("commence_time", "")
        api_date = str(commence_time).split("T")[0] if commence_time else None
        if api_date and api_date != date_str:
            continue

        bookmakers = game.get("bookmakers") or []
        if not bookmakers:
            continue

        # Just take the first bookmaker with an h2h market
        for book in bookmakers:
            markets = book.get("markets") or []
            for m in markets:
                if m.get("key") != "h2h":
                    continue
                outcomes = m.get("outcomes") or []
                for o in outcomes:
                    name = _norm_name(o.get("name", ""))
                    price = o.get("price")
                    if price is None:
                        continue
                    if name == home_norm:
                        home_price = float(price)
                    elif name == away_norm:
                        away_price = float(price)
                break  # only need the first h2h market

            if home_price is not None or away_price is not None:
                break

        if home_price is not None or away_price is not None:
            break

    REAL_ODDS_CACHE[cache_key] = {
        "home": home_price,
        "away": away_price,
    }
    return home_price, away_price


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
    home_team: Optional[str] = None  # Team name/abbrev for display
    away_team: Optional[str] = None  # Team name/abbrev for display
    venue: Optional[str] = None
    status: Optional[str] = None
    start_time: Optional[str] = None
    start_time_display: Optional[str] = None
    has_prediction: bool = True

    home_score: Optional[int] = None
    away_score: Optional[int] = None
    home_win: Optional[bool] = None
    model_home_win_prob: Optional[float] = None
    model_away_win_prob: Optional[float] = None
    model_home_american_odds: Optional[float] = None
    model_away_american_odds: Optional[float] = None

    # Real sportsbook odds from external provider
    sportsbook_home_american_odds: Optional[float] = None
    sportsbook_away_american_odds: Optional[float] = None

    # UFC-specific fields (optional, only present for UFC fights)
    method: Optional[str] = None
    finish_round: Optional[float] = None
    finish_details: Optional[str] = None
    finish_time: Optional[str] = None
    weight_class: Optional[str] = None
    title_bout: Optional[bool] = None
    gender: Optional[str] = None
    location: Optional[str] = None
    scheduled_rounds: Optional[int] = None


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
    """Return teams as simple id/name objects for all sports."""
    teams = [
        TeamOut(
            team_id=team_id,
            sport_id=TEAM_ID_TO_SPORT_ID.get(team_id, SPORT_ID_NBA),
            name=name,
        )
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
        # Newest games first. We intentionally ignore `limit` here so that the
    # frontend can see the full year range (2015–present) for filters.
    df = df.sort_values(["date", "game_id"], ascending=[False, False])

    items: List[EventOut] = []
    for _, row in df.iterrows():
        # Some rows (especially from newly added sports) may have missing/None game_id.
        # Skip those to avoid 500s when casting to int.
        raw_game_id = row.get("game_id", None)
        try:
            game_id = int(raw_game_id)
        except (TypeError, ValueError):
            logger.warning("Skipping row with invalid game_id=%r in /events", raw_game_id)
            continue

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

        # Compute status using date/time-aware logic
        status = compute_event_status(row)

        # Extract start time if available
        start_time = None
        if "start_et" in row and pd.notna(row["start_et"]):
            start_time = str(row["start_et"])

        # Format start time for display
        start_time_display = format_start_time_display(row)

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

        # Real sportsbook odds (only for scheduled games, and only if enabled)
        sportsbook_home_american_odds: Optional[float] = None
        sportsbook_away_american_odds: Optional[float] = None

        if status == "scheduled" and ENABLE_REAL_ODDS:
            try:
                real_home, real_away = fetch_real_odds_for_game(
                    row["date"],
                    str(row["home_team"]),
                    str(row["away_team"]),
                )
                sportsbook_home_american_odds = real_home
                sportsbook_away_american_odds = real_away
            except Exception as e:
                logger.error(
                    "Real odds lookup failed for game_id=%s: %s",
                    int(row["game_id"]),
                    e,
                )
                # leave sportsbook_* as None and keep going

        # Determine sport_id per row (NBA vs MLB vs NFL vs NHL vs UFC)
        sport_str = str(row.get("sport", "NBA")).upper()
        if sport_str == "MLB":
            sport_id = SPORT_ID_MLB
        elif sport_str == "NFL":
            sport_id = SPORT_ID_NFL
        elif sport_str == "NHL":
            sport_id = SPORT_ID_NHL
        elif sport_str == "UFC":
            sport_id = SPORT_ID_UFC
        else:
            sport_id = SPORT_ID_NBA

        items.append(
            EventOut(
                event_id=game_id,
                sport_id=sport_id,
                date=str(row["date"].date()),
                home_team_id=TEAM_NAME_TO_ID.get(str(row["home_team"])),
                away_team_id=TEAM_NAME_TO_ID.get(str(row["away_team"])),
                home_team=str(row["home_team"]) if pd.notna(row.get("home_team")) else None,
                away_team=str(row["away_team"]) if pd.notna(row.get("away_team")) else None,
                venue=None,
                status=status,
                start_time=start_time,
                start_time_display=start_time_display,
                home_score=home_score,
                away_score=away_score,
                home_win=home_win,
                model_home_win_prob=model_home_win_prob,
                model_away_win_prob=model_away_win_prob,
                model_home_american_odds=model_home_american_odds,
                model_away_american_odds=model_away_american_odds,
                sportsbook_home_american_odds=sportsbook_home_american_odds,
                sportsbook_away_american_odds=sportsbook_away_american_odds,
                # UFC-specific fields (only present for UFC fights)
                method=str(row["method"]) if pd.notna(row.get("method")) else None,
                finish_round=float(row["finish_round"]) if pd.notna(row.get("finish_round")) else None,
                finish_details=str(row["finish_details"]) if pd.notna(row.get("finish_details")) else None,
                finish_time=str(row["finish_time"]) if pd.notna(row.get("finish_time")) else None,
                weight_class=str(row["weight_class"]) if pd.notna(row.get("weight_class")) else None,
                title_bout=bool(row["title_bout"]) if pd.notna(row.get("title_bout")) else None,
                gender=str(row["gender"]) if pd.notna(row.get("gender")) else None,
                location=str(row["location"]) if pd.notna(row.get("location")) else None,
                scheduled_rounds=int(row["scheduled_rounds"]) if pd.notna(row.get("scheduled_rounds")) else None,
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

    # Compute status using date/time-aware logic
    status = compute_event_status(row)

    # Extract start time if available
    start_time = None
    if "start_et" in row and pd.notna(row["start_et"]):
        start_time = str(row["start_et"])

    # Format start time for display
    start_time_display = format_start_time_display(row)

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

    # Real sportsbook odds (only for scheduled games, and only if enabled)
    sportsbook_home_american_odds: Optional[float] = None
    sportsbook_away_american_odds: Optional[float] = None

    if status == "scheduled" and ENABLE_REAL_ODDS:
        try:
            real_home, real_away = fetch_real_odds_for_game(
                row["date"],
                str(row["home_team"]),
                str(row["away_team"]),
            )
            sportsbook_home_american_odds = real_home
            sportsbook_away_american_odds = real_away
        except Exception as e:
            logger.error(
                "Real odds lookup failed for event_id=%s: %s",
                event_id,
                e,
            )
            # keep sportsbook_* as None

    sport_str = str(row.get("sport", "NBA")).upper()
    if sport_str == "MLB":
        sport_id = SPORT_ID_MLB
    elif sport_str == "NFL":
        sport_id = SPORT_ID_NFL
    elif sport_str == "NHL":
        sport_id = SPORT_ID_NHL
    elif sport_str == "UFC":
        sport_id = SPORT_ID_UFC
    else:
        sport_id = SPORT_ID_NBA

    return EventOut(
        event_id=int(row["game_id"]),
        sport_id=sport_id,
        date=str(row["date"].date()),
        home_team_id=TEAM_NAME_TO_ID.get(str(row["home_team"])),
        away_team_id=TEAM_NAME_TO_ID.get(str(row["away_team"])),
        home_team=str(row["home_team"]) if pd.notna(row.get("home_team")) else None,
        away_team=str(row["away_team"]) if pd.notna(row.get("away_team")) else None,
        venue=None,
        status=status,
        start_time=start_time,
        start_time_display=start_time_display,
        home_score=home_score,
        away_score=away_score,
        home_win=home_win,
        model_home_win_prob=model_home_win_prob,
        model_away_win_prob=model_away_win_prob,
        model_home_american_odds=model_home_american_odds,
        model_away_american_odds=model_away_american_odds,
        sportsbook_home_american_odds=sportsbook_home_american_odds,
        sportsbook_away_american_odds=sportsbook_away_american_odds,
        # UFC-specific fields (only present for UFC fights)
        method=str(row["method"]) if pd.notna(row.get("method")) else None,
        finish_round=float(row["finish_round"]) if pd.notna(row.get("finish_round")) else None,
        finish_details=str(row["finish_details"]) if pd.notna(row.get("finish_details")) else None,
        finish_time=str(row["finish_time"]) if pd.notna(row.get("finish_time")) else None,
        weight_class=str(row["weight_class"]) if pd.notna(row.get("weight_class")) else None,
        title_bout=bool(row["title_bout"]) if pd.notna(row.get("title_bout")) else None,
        gender=str(row["gender"]) if pd.notna(row.get("gender")) else None,
        location=str(row["location"]) if pd.notna(row.get("location")) else None,
        scheduled_rounds=int(row["scheduled_rounds"]) if pd.notna(row.get("scheduled_rounds")) else None,
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
