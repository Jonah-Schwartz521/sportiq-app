from __future__ import annotations

from datetime import date as date_type, datetime
from pathlib import Path
from typing import Optional, Dict, List, Any

from collections import deque
import os  # NEW: for reading environment variables
import re

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

from src.paths import PROCESSED_DIR, MLB_PROCESSED_DIR, NFL_PROCESSED_DIR, NHL_PROCESSED_DIR, UFC_PROCESSED_DIR, NFL_ARTIFACTS_DIR
from src.nba_inference import load_nba_model, predict_home_win_proba

# --- Global objects (loaded once at startup) -----------------------

GAMES_DF: Optional[pd.DataFrame] = None
NFL_PREDICTIONS_DF: Optional[pd.DataFrame] = None  # NFL model predictions
NBA_PREDICTIONS_DF: Optional[pd.DataFrame] = None  # NBA model predictions

# simple in-memory lookup tables
TEAM_NAME_TO_ID: Dict[str, int] = {}
TEAM_ID_TO_NAME: Dict[int, str] = {}
SPORT_ID_NBA = 1
SPORT_ID_MLB = 2
SPORT_ID_NFL = 3
SPORT_ID_NHL = 4
SPORT_ID_UFC = 5
TEAM_ID_TO_SPORT_ID: Dict[int, int] = {}

# --- NFL Team Name Canonicalization Map ---------------------------
# Use a lookup csv when available so UI always shows full team names.
DEFAULT_NFL_TEAM_LOOKUP = {
    "ARI": "Arizona Cardinals",
    "ATL": "Atlanta Falcons",
    "BAL": "Baltimore Ravens",
    "BUF": "Buffalo Bills",
    "CAR": "Carolina Panthers",
    "CHI": "Chicago Bears",
    "CIN": "Cincinnati Bengals",
    "CLE": "Cleveland Browns",
    "DAL": "Dallas Cowboys",
    "DEN": "Denver Broncos",
    "DET": "Detroit Lions",
    "GB": "Green Bay Packers",
    "HOU": "Houston Texans",
    "IND": "Indianapolis Colts",
    "JAX": "Jacksonville Jaguars",
    "KC": "Kansas City Chiefs",
    "LAC": "Los Angeles Chargers",
    "LAR": "Los Angeles Rams",
    "LV": "Las Vegas Raiders",
    "MIA": "Miami Dolphins",
    "MIN": "Minnesota Vikings",
    "NE": "New England Patriots",
    "NO": "New Orleans Saints",
    "NYG": "New York Giants",
    "NYJ": "New York Jets",
    "PHI": "Philadelphia Eagles",
    "PIT": "Pittsburgh Steelers",
    "SF": "San Francisco 49ers",
    "SEA": "Seattle Seahawks",
    "TB": "Tampa Bay Buccaneers",
    "TEN": "Tennessee Titans",
    "WAS": "Washington Commanders",
}

# Map any historical/alternate abbreviations to the canonical key above
NFL_TEAM_ABBR_ALIASES = {
    "ARZ": "ARI",
    "GNB": "GB",
    "KAN": "KC",
    "JAC": "JAX",
    "LA": "LAR",
    "LVR": "LV",
    "OAK": "LV",
    "SD": "LAC",
    "STL": "LAR",
    "TAM": "TB",
    "WSH": "WAS",
    "WFT": "WAS",
}

NFL_TEAM_LOOKUP_CACHE: Optional[Dict[str, str]] = None


def get_nfl_team_lookup() -> Dict[str, str]:
    """
    Load NFL team lookup (abbr -> full name) from csv if present, else defaults.
    """
    global NFL_TEAM_LOOKUP_CACHE
    if NFL_TEAM_LOOKUP_CACHE is not None:
        return NFL_TEAM_LOOKUP_CACHE

    lookup_path = NFL_PROCESSED_DIR / "nfl_team_lookup.csv"
    mapping: Dict[str, str] = DEFAULT_NFL_TEAM_LOOKUP.copy()

    if lookup_path.exists():
        try:
            df = pd.read_csv(lookup_path)
            df = df.rename(
                columns={
                    "team": "team_name",
                    "name": "team_name",
                    "abbr": "abbr",
                    "abbrev": "abbr",
                }
            )
            df = df[[c for c in ["abbr", "team_name"] if c in df.columns]]
            df = df.dropna(subset=["abbr", "team_name"])
            csv_map = {
                str(row["abbr"]).strip().upper(): str(row["team_name"]).strip()
                for _, row in df.iterrows()
            }
            if csv_map:
                mapping.update(csv_map)
                logger.info(
                    "Loaded NFL team lookup from %s (rows=%d)",
                    lookup_path,
                    len(csv_map),
                )
        except Exception as e:
            logger.warning(
                "Failed to read nfl_team_lookup.csv at %s (%s); using defaults.",
                lookup_path,
                e,
            )

    NFL_TEAM_LOOKUP_CACHE = mapping
    return mapping


def _normalize_single_team_value(value: Any, team_lookup: Dict[str, str], name_to_abbr: Dict[str, str]) -> tuple[Optional[str], Optional[str]]:
    """
    Normalize a single team value into (abbr, full_name).
    """
    if value is None or pd.isna(value):
        return None, None

    raw = str(value).strip()
    upper = raw.upper()

    abbr = NFL_TEAM_ABBR_ALIASES.get(upper, upper)
    if abbr in team_lookup:
        return abbr, team_lookup[abbr]

    if upper in name_to_abbr:
        canonical_abbr = name_to_abbr[upper]
        return canonical_abbr, team_lookup.get(canonical_abbr, raw)

    return abbr, raw


def normalize_nfl_team_columns(df: pd.DataFrame, team_lookup: Dict[str, str]) -> pd.DataFrame:
    """
    Ensure NFL frames have:
      - home_team/away_team as full team names
      - home_team_abbr/away_team_abbr as canonical abbreviations
    """
    if df.empty:
        return df

    name_to_abbr = {v.upper(): k for k, v in team_lookup.items()}

    def normalize_row(row: pd.Series) -> pd.Series:
        for prefix in ("home", "away"):
            raw_val = None
            for col in (
                f"{prefix}_team_abbr",
                f"{prefix}_team_id",
                f"{prefix}_team",
                f"{prefix}_team_name",
            ):
                if col in row and pd.notna(row[col]):
                    raw_val = row[col]
                    break

            abbr, full = _normalize_single_team_value(raw_val, team_lookup, name_to_abbr)
            row[f"{prefix}_team_abbr"] = abbr
            row[f"{prefix}_team"] = full
        return row

    return df.apply(normalize_row, axis=1)


def attach_nfl_game_id(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add a canonical nfl_game_id column for joining predictions/deduping.

    Priority:
      1) Non-numeric game_id already present (e.g., 2025_15_CLE_CHI)
      2) Existing nfl_game_id/nfl_game_id_str columns
      3) game_date/date + team abbreviations -> YYYY_MM_DD_HOME_AWAY
      4) season/week + team abbreviations -> {season}_{week:02d}_{home}_{away}
    """
    if df.empty:
        return df

    def build_id(row: pd.Series) -> Optional[str]:
        def _clean(val: Any) -> Optional[str]:
            if val is None or pd.isna(val):
                return None
            s = str(val).strip()
            return s if s else None

        gid = _clean(row.get("game_id"))
        if gid and not gid.isdigit():
            gid_normalized = gid.replace("-", "_").upper()
            return gid_normalized
        else:
            gid = None  # numeric or missing → ignore as canonical id

        season_week_candidate = None
        for fallback_col in ["nfl_game_id", "nfl_game_id_str"]:
            fb_val = _clean(row.get(fallback_col))
            if fb_val and not fb_val.isdigit():
                fb_val_norm = fb_val.replace("-", "_").upper()
                parts = fb_val_norm.split("_")
                # If this already looks like a date-based id (YYYY_MM_DD_HOME_AWAY), keep it
                if len(parts) == 5:
                    return fb_val_norm
                season_week_candidate = fb_val_norm

        home_abbr = _clean(row.get("home_team_abbr")) or _clean(row.get("home_team"))
        away_abbr = _clean(row.get("away_team_abbr")) or _clean(row.get("away_team"))

        date_val = row.get("game_date", row.get("date"))
        if date_val is not None and home_abbr and away_abbr:
            try:
                date_str = pd.to_datetime(date_val).strftime("%Y_%m_%d")
                return f"{date_str}_{home_abbr}_{away_abbr}"
            except Exception:
                pass

        season_val = row.get("season")
        week_val = row.get("week")
        if (
            home_abbr
            and away_abbr
            and season_val is not None
            and week_val is not None
            and not pd.isna(season_val)
            and not pd.isna(week_val)
        ):
            try:
                season_int = int(season_val)
                week_int = int(week_val)
                return f"{season_int}_{week_int:02d}_{home_abbr}_{away_abbr}"
            except (ValueError, TypeError):
                pass

        if season_week_candidate:
            return season_week_candidate

        return None

    df = df.copy()
    df["nfl_game_id"] = df.apply(build_id, axis=1)
    return df


def dedupe_nfl_games(df: pd.DataFrame) -> pd.DataFrame:
    """
    ROBUST NFL deduplication with canonical keys and off-by-one-day handling.

    Strategy:
    1. Build matchup_key = NFL|home_team|away_team (normalized)
    2. Build kickoff_bucket = floor(date_utc to game day)
    3. Find candidates with same matchup_key and dates within 36 hours
    4. Force candidates to share the same kickoff_bucket
    5. Build canonical_key = matchup_key|kickoff_bucket
    6. Dedupe on canonical_key with priority: FINAL score > predictions > market data
    """
    if df.empty or "sport" not in df.columns:
        return df

    nfl_mask = df["sport"].astype(str).str.upper() == "NFL"
    if nfl_mask.sum() == 0:
        return df

    nfl_df = df[nfl_mask].copy()
    other_df = df[~nfl_mask].copy()

    initial_nfl_count = len(nfl_df)
    logger.info("🏈 NFL dedupe starting: %d rows before deduplication", initial_nfl_count)

    # Normalize team names to full names
    lookup = get_nfl_team_lookup()
    nfl_df = normalize_nfl_team_columns(nfl_df, lookup)

    # CRITICAL: Classify is_final and is_future BEFORE deduplication
    default_series = pd.Series([None] * len(nfl_df), index=nfl_df.index)

    # is_final = has scores OR status is FINAL
    nfl_df["_is_final"] = (
        (nfl_df.get("home_pts", default_series).notna() & nfl_df.get("away_pts", default_series).notna()) |
        (nfl_df.get("status", default_series).astype(str).str.upper() == "FINAL")
    )

    # is_future = no scores AND date is in the future (after now - 6 hours)
    from datetime import datetime, timedelta
    now_minus_6h = datetime.utcnow() - timedelta(hours=6)

    nfl_df["_date_utc_dt"] = pd.to_datetime(nfl_df.get("date"), utc=True, errors="coerce").dt.tz_localize(None)
    nfl_df["_is_future"] = (
        nfl_df.get("home_pts", default_series).isna() &
        nfl_df.get("away_pts", default_series).isna() &
        (nfl_df["_date_utc_dt"] > now_minus_6h)
    )

    initial_final_count = nfl_df["_is_final"].sum()
    initial_future_count = nfl_df["_is_future"].sum()
    initial_past_no_scores = (
        nfl_df.get("home_pts", default_series).isna() &
        nfl_df.get("away_pts", default_series).isna() &
        (nfl_df["_date_utc_dt"] <= now_minus_6h)
    ).sum()

    logger.info("📊 NFL BEFORE DEDUPE: total=%d final=%d future=%d past_no_scores=%d",
                initial_nfl_count, initial_final_count, initial_future_count, initial_past_no_scores)

    # Step 1: Create matchup_key (order-independent for home/away)
    home_norm = nfl_df.get("home_team", pd.Series([""] * len(nfl_df))).astype(str).str.strip().str.upper()
    away_norm = nfl_df.get("away_team", pd.Series([""] * len(nfl_df))).astype(str).str.strip().str.upper()

    # Create ordered matchup key (always home|away for consistency)
    nfl_df["_matchup_key"] = "NFL|" + home_norm + "|" + away_norm

    # Step 2: Use the _date_utc_dt already created during classification
    # Initial bucket: just the date part
    nfl_df["_kickoff_bucket"] = nfl_df["_date_utc_dt"].dt.strftime("%Y-%m-%d")

    # Step 3: Find candidates with same matchup but different dates (within 36 hours)
    # Group by matchup_key and find duplicates
    matchup_groups = nfl_df.groupby("_matchup_key")

    dedupe_log_entries = []

    for matchup_key, grp in matchup_groups:
        if len(grp) <= 1:
            continue

        # Only align buckets for rows that are within 36 hours of a FINAL row.
        # This prevents collapsing entire matchup history (spanning seasons)
        # into a single bucket just because two old rows were close in time.
        final_rows = grp[grp["_is_final"]].copy()
        if final_rows.empty:
            continue

        for idx, row in grp.iterrows():
            row_dt = row["_date_utc_dt"]
            if pd.isna(row_dt):
                continue

            hours_diff = (final_rows["_date_utc_dt"] - row_dt).abs().dt.total_seconds() / 3600
            if (hours_diff <= 36).any():
                nearest_final_idx = hours_diff.idxmin()
                best_bucket = final_rows.loc[nearest_final_idx, "_kickoff_bucket"]
                if pd.notna(best_bucket):
                    nfl_df.at[idx, "_kickoff_bucket"] = best_bucket
            else:
                # No nearby FINAL → keep the row's own bucket (do not merge seasons)
                continue

    # Step 4: Build canonical_key
    nfl_df["_canonical_key"] = nfl_df["_matchup_key"] + "|" + nfl_df["_kickoff_bucket"].fillna("UNKNOWN")

    # Step 5: Compute row quality scores
    # CRITICAL RULE: FINAL beats FUTURE, but FUTURE never deletes FUTURE
    nfl_df["_quality_score"] = 0

    # Priority 1: FINAL scores (weight=10000)
    # Use the pre-computed _is_final flag
    nfl_df.loc[nfl_df["_is_final"], "_quality_score"] += 10000

    # Priority 2: Model predictions (weight=1000)
    default_series = pd.Series([None] * len(nfl_df), index=nfl_df.index)
    has_predictions = nfl_df.get("p_home_win", default_series).notna()
    nfl_df.loc[has_predictions, "_quality_score"] += 1000

    # Priority 3: Market data (weight=100)
    has_market = (
        nfl_df.get("home_moneyline", default_series).notna() |
        nfl_df.get("spread_line", default_series).notna() |
        nfl_df.get("market_snapshot", default_series).notna()
    )
    nfl_df.loc[has_market, "_quality_score"] += 100

    # Priority 4: Has event_id (weight=10)
    has_event_id = nfl_df.get("event_id", default_series).notna()
    nfl_df.loc[has_event_id, "_quality_score"] += 10

    # Step 6: Log specific matchups before deduplication
    for matchup_check in ["ATLANTA FALCONS|TAMPA BAY BUCCANEERS", "TAMPA BAY BUCCANEERS|ATLANTA FALCONS",
                          "PHILADELPHIA EAGLES|LOS ANGELES CHARGERS", "LOS ANGELES CHARGERS|PHILADELPHIA EAGLES"]:
        matchup_rows = nfl_df[nfl_df["_matchup_key"].str.contains(matchup_check, na=False)]
        if len(matchup_rows) > 1:
            # Group by canonical key
            canonical_groups = matchup_rows.groupby("_canonical_key")
            for canon_key, canon_grp in canonical_groups:
                if len(canon_grp) > 1:
                    teams = matchup_check.replace("NFL|", "").split("|")
                    logger.warning(
                        "🔍 PRE-DEDUPE: %s|%s candidates=%d, canonical_key=%s",
                        teams[0] if len(teams) > 0 else "?",
                        teams[1] if len(teams) > 1 else "?",
                        len(canon_grp),
                        canon_key
                    )

    # Step 7: Sort by canonical_key and quality_score, then deduplicate
    nfl_df = nfl_df.sort_values(["_canonical_key", "_quality_score"], ascending=[True, False])

    # Detect duplicates
    dup_mask = nfl_df.duplicated(subset=["_canonical_key"], keep=False)
    dup_count = dup_mask.sum()

    if dup_count > 0:
        # Log details about duplicates
        dup_df = nfl_df[dup_mask].copy()
        unique_matchups = dup_df["_matchup_key"].unique()
        logger.info("🔍 Found %d duplicate rows across %d unique matchups", dup_count, len(unique_matchups))

        # Log specific matchups
        for matchup in ["ATLANTA FALCONS|TAMPA BAY BUCCANEERS", "TAMPA BAY BUCCANEERS|ATLANTA FALCONS",
                       "PHILADELPHIA EAGLES|LOS ANGELES CHARGERS", "LOS ANGELES CHARGERS|PHILADELPHIA EAGLES"]:
            matchup_dups = dup_df[dup_df["_matchup_key"].str.contains(matchup, na=False)]
            if not matchup_dups.empty:
                canonical_groups = matchup_dups.groupby("_canonical_key")
                for canon_key, canon_grp in canonical_groups:
                    if len(canon_grp) > 1:
                        kept_row = canon_grp.iloc[0]
                        removed_count = len(canon_grp) - 1

                        # Determine reason for keeping
                        reason = "UNKNOWN"
                        if kept_row["_quality_score"] >= 10000:
                            reason = "FINAL_SCORE"
                        elif kept_row["_quality_score"] >= 1000:
                            reason = "HAS_PREDICTIONS"
                        elif kept_row["_quality_score"] >= 100:
                            reason = "HAS_MARKET_DATA"
                        elif kept_row["_quality_score"] >= 10:
                            reason = "HAS_EVENT_ID"

                        teams = matchup.replace("NFL|", "").split("|")
                        logger.warning(
                            "NFL dedupe: %s|%s candidates=%d kept=1 removed=%d chosen_reason=%s date=%s",
                            teams[0] if len(teams) > 0 else "?",
                            teams[1] if len(teams) > 1 else "?",
                            len(canon_grp),
                            removed_count,
                            reason,
                            kept_row.get("date", "UNKNOWN")
                        )

    # Step 8: Perform deduplication with FUTURE preservation rule
    # CRITICAL RULE: FUTURE games can only be removed if a FINAL version exists
    # If all duplicates for a canonical_key are FUTURE, keep ALL of them

    groups_to_keep = []
    removed_count = 0
    removed_final = 0
    removed_future = 0
    preserved_future_groups = 0

    for canon_key, grp in nfl_df.groupby("_canonical_key"):
        if len(grp) == 1:
            # No duplicates, keep it
            groups_to_keep.append(grp)
        else:
            # Has duplicates - check if any are FINAL
            has_final = grp["_is_final"].any()

            if has_final:
                # At least one FINAL exists - keep only the highest quality row (which will be FINAL)
                # Sort by quality score descending and keep first
                best_row = grp.sort_values("_quality_score", ascending=False).iloc[:1]
                groups_to_keep.append(best_row)

                # Count what was removed
                removed_in_group = len(grp) - 1
                removed_count += removed_in_group
                removed_final += grp[grp["_is_final"]].shape[0] - 1  # Removed FINALs
                removed_future += grp[grp["_is_future"]].shape[0]  # All FUTUREs removed
            else:
                # ALL are FUTURE - keep the best row to avoid duplicate cards,
                # but do not delete the matchup entirely.
                best_future = grp.iloc[:1]
                groups_to_keep.append(best_future)
                preserved_future_groups += 1

                removed_in_group = len(grp) - 1
                removed_count += removed_in_group
                removed_future += removed_in_group
                if removed_in_group > 0:
                    logger.info(
                        "✓ Future-only dedupe: kept 1, removed %d for canonical_key=%s",
                        removed_in_group,
                        canon_key[:80],
                    )

    logger.info("📋 Dedupe stats: removed_final=%d removed_future=%d preserved_future_groups=%d",
                removed_final, removed_future, preserved_future_groups)

    nfl_df = pd.concat(groups_to_keep, ignore_index=True)
    # Log snapshot after dedupe
    future_after = nfl_df["_is_future"].sum() if "_is_future" in nfl_df.columns else 0
    final_after = nfl_df["_is_final"].sum() if "_is_final" in nfl_df.columns else 0
    logger.info("NFL AFTER DEDUPE: total=%d final=%d future=%d", len(nfl_df), int(final_after), int(future_after))

    # Clean up temporary columns (but keep _is_final and _is_future for now - they'll be dropped later)
    nfl_df = nfl_df.drop(columns=["_matchup_key", "_date_utc_dt", "_kickoff_bucket", "_canonical_key", "_quality_score"], errors="ignore")

    final_nfl_count = len(nfl_df)
    final_final_count = nfl_df["_is_final"].sum()
    final_future_count = nfl_df["_is_future"].sum()
    total_removed = initial_nfl_count - final_nfl_count

    logger.info(
        "✅ NFL dedupe complete: total=%d final=%d future=%d (removed %d duplicates, %.1f%% reduction)",
        final_nfl_count,
        final_final_count,
        final_future_count,
        total_removed,
        100 * total_removed / initial_nfl_count if initial_nfl_count > 0 else 0,
    )

    # Drop the classification flags before returning
    nfl_df = nfl_df.drop(columns=["_is_final", "_is_future"], errors="ignore")

    return pd.concat([other_df, nfl_df], ignore_index=True)

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
    nfl_team_lookup = get_nfl_team_lookup()

    # --- Load NBA games (primary source) ---
    nba_path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
    logger.info("Loading NBA games table from %s ...", nba_path)

    # Try reading with pyarrow engine first, fallback if metadata is corrupted
    try:
        nba_df = pd.read_parquet(nba_path, engine='pyarrow').copy()
    except (TypeError, ValueError) as e:
        logger.warning("Failed to read parquet with pyarrow engine (%s), trying alternative method...", e)
        # Fallback: read using pyarrow and strip the problematic pandas metadata
        import pyarrow.parquet as pq
        table = pq.read_table(nba_path)
        # Remove all metadata to avoid deserialization issues
        clean_table = table.replace_schema_metadata({})
        nba_df = clean_table.to_pandas().copy()

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
        logger.info("NFL historical loaded: %d rows", len(nfl_hist))

        # Remove duplicate columns immediately after loading
        nfl_hist = nfl_hist.loc[:, ~nfl_hist.columns.duplicated()]

        nfl_hist["sport"] = "NFL"

        # Rename columns, but only if target doesn't already exist
        rename_map = {}
        if "home_team_name" in nfl_hist.columns and "home_team" not in nfl_hist.columns:
            rename_map["home_team_name"] = "home_team"
        if "away_team_name" in nfl_hist.columns and "away_team" not in nfl_hist.columns:
            rename_map["away_team_name"] = "away_team"
        if "home_score" in nfl_hist.columns and "home_pts" not in nfl_hist.columns:
            rename_map["home_score"] = "home_pts"
        if "away_score" in nfl_hist.columns and "away_pts" not in nfl_hist.columns:
            rename_map["away_score"] = "away_pts"
        if "gameday" in nfl_hist.columns and "date" not in nfl_hist.columns:
            rename_map["gameday"] = "date"

        if rename_map:
            nfl_hist = nfl_hist.rename(columns=rename_map)

        # Ensure required columns exist even if schema differs
        for col in ["home_team", "away_team", "home_pts", "away_pts", "date"]:
            if col not in nfl_hist.columns:
                nfl_hist[col] = None

        nfl_hist = normalize_nfl_team_columns(nfl_hist, nfl_team_lookup)
        nfl_hist = attach_nfl_game_id(nfl_hist)

        # DEBUG: Count final vs future games in historical
        hist_final = ((nfl_hist.get("home_pts", pd.Series([None]*len(nfl_hist))).notna()) |
                      (nfl_hist.get("status", pd.Series([None]*len(nfl_hist))).astype(str).str.upper() == "FINAL")).sum()
        hist_upcoming = ((nfl_hist.get("home_pts", pd.Series([None]*len(nfl_hist))).isna()) &
                        (nfl_hist.get("away_pts", pd.Series([None]*len(nfl_hist))).isna())).sum()
        logger.info("📊 NFL HISTORICAL: total=%d final=%d upcoming=%d", len(nfl_hist), hist_final, hist_upcoming)

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
        logger.info("NFL future loaded: %d rows", len(nfl_future))

        nfl_future["sport"] = "NFL"

        # Rename team columns if needed
        rename_map = {}
        if "home_team_id" in nfl_future.columns and "home_team" not in nfl_future.columns:
            rename_map["home_team_id"] = "home_team"
        if "away_team_id" in nfl_future.columns and "away_team" not in nfl_future.columns:
            rename_map["away_team_id"] = "away_team"
        if "home_team_name" in nfl_future.columns:
            rename_map["home_team_name"] = "home_team"
        if "away_team_name" in nfl_future.columns:
            rename_map["away_team_name"] = "away_team"
        if "home_score" in nfl_future.columns:
            rename_map["home_score"] = "home_pts"
        if "away_score" in nfl_future.columns:
            rename_map["away_score"] = "away_pts"

        nfl_future = nfl_future.rename(columns=rename_map)

        nfl_future = normalize_nfl_team_columns(nfl_future, nfl_team_lookup)
        nfl_future = attach_nfl_game_id(nfl_future)

        # DEBUG: Count upcoming games in future
        future_upcoming = len(nfl_future)
        logger.info("📊 NFL FUTURE: total=%d (all should be upcoming)", future_upcoming)

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
    if "sport" in games.columns:
        nfl_rows = games[games["sport"] == "NFL"]
        future_mask = nfl_rows.get("home_pts", pd.Series([None] * len(nfl_rows))).isna() & nfl_rows.get("away_pts", pd.Series([None] * len(nfl_rows))).isna()
        logger.info("NFL concat snapshot: total=%d future_no_scores=%d", len(nfl_rows), int(future_mask.sum()))

    # --- Normalize date column to timezone-naive pandas datetime ---
    if "date" in games.columns:
        # Convert any mix of strings / python datetimes (with or without tz)
        # into a unified datetime64[ns] column, dropping timezone info.
        games = games.copy()
        games["date"] = pd.to_datetime(games["date"], utc=True, errors="coerce")
        # Drop timezone info so downstream `.dt` access works without errors
        games["date"] = games["date"].dt.tz_localize(None)

    # --- Create canonical event_key for deduplication and prediction joins ---
    # Format: {sport}|{date_yyyy_mm_dd}|{home_team}|{away_team}
    if "sport" in games.columns and "date" in games.columns and "home_team" in games.columns and "away_team" in games.columns:
        games = games.copy()

        # Normalize date to YYYY-MM-DD string (no timezone)
        games["date_str"] = pd.to_datetime(games["date"]).dt.strftime("%Y-%m-%d")

        # Create event_key (canonical unique identifier per game)
        games["event_key"] = (
            games["sport"].astype(str).str.strip() + "|" +
            games["date_str"] + "|" +
            games["home_team"].astype(str).str.strip() + "|" +
            games["away_team"].astype(str).str.strip()
        )

        logger.info("Created event_key for %d games", len(games))

    # --- DEBUG: Count NFL games after concatenation ---
    if "sport" in games.columns:
        nfl_mask = games["sport"].astype(str).str.upper() == "NFL"
        nfl_debug = games[nfl_mask].copy()

        # Count final vs upcoming after concatenation
        concat_final = ((nfl_debug.get("home_pts", pd.Series([None]*len(nfl_debug))).notna()) |
                       (nfl_debug.get("status", pd.Series([None]*len(nfl_debug))).astype(str).str.upper() == "FINAL")).sum()
        concat_upcoming = ((nfl_debug.get("home_pts", pd.Series([None]*len(nfl_debug))).isna()) &
                          (nfl_debug.get("away_pts", pd.Series([None]*len(nfl_debug))).isna())).sum()
        logger.info("📊 NFL AFTER CONCATENATION: total=%d final=%d upcoming=%d", len(nfl_debug), concat_final, concat_upcoming)

    # --- DEBUG: Find Falcons-Buccaneers and Eagles-Chargers duplicates ---
    if "sport" in games.columns:
        nfl_mask = games["sport"].astype(str).str.upper() == "NFL"
        nfl_debug = games[nfl_mask].copy()

        # Debug specific matchups
        debug_matchups = [
            ("ATLANTA FALCONS", "TAMPA BAY BUCCANEERS"),
            ("TAMPA BAY BUCCANEERS", "ATLANTA FALCONS"),
            ("PHILADELPHIA EAGLES", "LOS ANGELES CHARGERS"),
            ("LOS ANGELES CHARGERS", "PHILADELPHIA EAGLES"),
        ]

        for team1, team2 in debug_matchups:
            candidates = nfl_debug[
                ((nfl_debug["home_team"].astype(str).str.upper().str.contains(team1.split()[0], na=False) &
                  nfl_debug["away_team"].astype(str).str.upper().str.contains(team2.split()[0], na=False)) |
                 (nfl_debug["home_team"].astype(str).str.upper().str.contains(team2.split()[0], na=False) &
                  nfl_debug["away_team"].astype(str).str.upper().str.contains(team1.split()[0], na=False)))
            ].copy()

            if len(candidates) > 0:
                # Group by approximate date (within 3 days)
                if "date" in candidates.columns:
                    candidates_sorted = candidates.sort_values("date")
                    matchup_desc = f"{team1[:20]} vs {team2[:20]}"

                    # Check for duplicates within 3 days
                    for i in range(len(candidates_sorted)):
                        for j in range(i + 1, len(candidates_sorted)):
                            row1 = candidates_sorted.iloc[i]
                            row2 = candidates_sorted.iloc[j]
                            date1 = pd.to_datetime(row1["date"])
                            date2 = pd.to_datetime(row2["date"])
                            hours_diff = abs((date2 - date1).total_seconds() / 3600)

                            if hours_diff <= 72:  # Within 3 days
                                debug_cols = ["date", "home_team", "away_team", "home_pts", "away_pts", "status", "event_id", "game_id", "p_home_win"]
                                debug_cols = [c for c in debug_cols if c in candidates_sorted.columns]

                                logger.warning(
                                    "🐛 DUPLICATE FOUND: %s (diff=%.1f hours)\nRow 1: %s\nRow 2: %s",
                                    matchup_desc,
                                    hours_diff,
                                    row1[debug_cols].to_dict(),
                                    row2[debug_cols].to_dict()
                                )

    # --- Deduplicate games (NFL handled separately) ---
    games = dedupe_nfl_games(games)

    initial_count = len(games)
    if "event_key" in games.columns:
        non_nfl_mask = games["sport"].astype(str).str.upper() != "NFL"
        non_nfl = games[non_nfl_mask].copy()
        nfl_only = games[~non_nfl_mask].copy()

        if not non_nfl.empty:
            non_nfl["row_quality_score"] = 0

            default_series = pd.Series([None] * len(non_nfl), index=non_nfl.index)
            has_pred = (
                non_nfl.get("p_home_win", default_series).notna()
                | non_nfl.get("nba_p_home_win", default_series).notna()
            )
            non_nfl.loc[has_pred, "row_quality_score"] += 100

            has_scores = non_nfl.get("home_pts", default_series).notna() & non_nfl.get("away_pts", default_series).notna()
            non_nfl.loc[has_scores, "row_quality_score"] += 50

            has_market = (
                non_nfl.get("home_moneyline", default_series).notna() |
                non_nfl.get("spread_line", default_series).notna()
            )
            non_nfl.loc[has_market, "row_quality_score"] += 10

            non_nfl = non_nfl.sort_values(["event_key", "row_quality_score"], ascending=[True, False])

            duplicate_mask = non_nfl.duplicated(subset=["event_key"], keep="first")
            duplicates_by_sport = {}
            duplicated_event_keys = {}

            if "sport" in non_nfl.columns:
                for sport in non_nfl["sport"].unique():
                    if pd.notna(sport):
                        sport_mask = non_nfl["sport"] == sport
                        sport_dupes = duplicate_mask & sport_mask
                        count = sport_dupes.sum()
                        if count > 0:
                            duplicates_by_sport[sport] = count
                            sport_df = non_nfl[sport_mask]
                            dup_keys = sport_df[sport_df.duplicated(subset=["event_key"], keep=False)]["event_key"].unique()
                            duplicated_event_keys[sport] = list(dup_keys)

            non_nfl = non_nfl.drop_duplicates(subset=["event_key"], keep="first")
            non_nfl = non_nfl.drop(columns=["row_quality_score"])

            total_removed = initial_count - len(nfl_only) - len(non_nfl)
            if total_removed > 0:
                logger.info(
                    "Removed %d duplicate games via event_key (non-NFL).",
                    total_removed
                )
                for sport, count in duplicates_by_sport.items():
                    logger.info("  - %s: %d duplicates removed", sport, count)
                    if sport in duplicated_event_keys:
                        sample_keys = duplicated_event_keys[sport][:10]
                        logger.info("    Duplicated event_keys: %s", sample_keys)
                        if len(duplicated_event_keys[sport]) > 10:
                            logger.info("    ... and %d more", len(duplicated_event_keys[sport]) - 10)

            games = pd.concat([nfl_only, non_nfl], ignore_index=True)

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


def load_nfl_predictions() -> Optional[pd.DataFrame]:
    """
    Load NFL model predictions from parquet files (historical + future).

    Returns a DataFrame with columns: nfl_game_id, p_home_win, p_away_win, source.
    Returns None if no predictions files exist.
    """
    global NFL_PREDICTIONS_DF

    if NFL_PREDICTIONS_DF is not None:
        return NFL_PREDICTIONS_DF

    historical_path = NFL_ARTIFACTS_DIR / "nfl_predictions.parquet"
    future_path = PROCESSED_DIR / "nfl" / "nfl_predictions_future.parquet"

    all_preds: list[pd.DataFrame] = []
    team_lookup = get_nfl_team_lookup()

    # Historical predictions (baseline)
    if historical_path.exists():
        try:
            logger.info("Loading historical NFL predictions from %s ...", historical_path)
            hist_df = pd.read_parquet(historical_path)
            hist_df["source"] = hist_df.get("source", "nfl_historical_baseline")
            hist_df["home_team_abbr"] = hist_df.get("home_team")
            hist_df["away_team_abbr"] = hist_df.get("away_team")
            hist_df = normalize_nfl_team_columns(hist_df, team_lookup)
            hist_df = attach_nfl_game_id(hist_df)
            if "p_away_win" not in hist_df.columns and "p_home_win" in hist_df.columns:
                hist_df["p_away_win"] = 1 - hist_df["p_home_win"]
            all_preds.append(hist_df)
        except Exception as e:
            logger.error("Failed to load historical NFL predictions: %s", e)
    else:
        logger.warning("Historical NFL predictions not found at %s", historical_path)

    # Future predictions
    if future_path.exists():
        try:
            logger.info("Loading future NFL predictions from %s ...", future_path)
            future_df = pd.read_parquet(future_path)
            future_df["source"] = future_df.get("source", "nfl_future")
            future_df["home_team_abbr"] = future_df.get("home_team")
            future_df["away_team_abbr"] = future_df.get("away_team")
            future_df = normalize_nfl_team_columns(future_df, team_lookup)
            future_df = attach_nfl_game_id(future_df)
            if "p_away_win" not in future_df.columns and "p_home_win" in future_df.columns:
                future_df["p_away_win"] = 1 - future_df["p_home_win"]
            all_preds.append(future_df)
        except Exception as e:
            logger.error("Failed to load future NFL predictions: %s", e)
    else:
        logger.warning("Future NFL predictions not found at %s", future_path)

    if not all_preds:
        logger.warning(
            "No NFL predictions found at %s or %s. NFL games will have no model_snapshot.",
            historical_path,
            future_path,
        )
        NFL_PREDICTIONS_DF = None
        return None

    try:
        preds_df = pd.concat(all_preds, ignore_index=True)
        preds_df["nfl_game_id"] = preds_df["nfl_game_id"].astype(str)
        preds_df.loc[
            preds_df["nfl_game_id"].isin(["None", "nan", "NaN"]), "nfl_game_id"
        ] = None
        preds_df["p_home_win"] = pd.to_numeric(preds_df["p_home_win"], errors="coerce")
        preds_df["p_away_win"] = pd.to_numeric(preds_df["p_away_win"], errors="coerce")

        dedupe_key = preds_df["nfl_game_id"]
        if "event_key" in preds_df.columns:
            dedupe_key = dedupe_key.fillna(preds_df["event_key"])
        preds_df["__dedupe_key"] = dedupe_key

        duplicate_mask = preds_df.duplicated(subset=["__dedupe_key"], keep="last")
        removed = int(duplicate_mask.sum())
        preds_df = preds_df.drop_duplicates(subset=["__dedupe_key"], keep="last")

        preds_df = preds_df.rename(columns={"source": "nfl_source"})
        needed_cols = ["nfl_game_id", "p_home_win", "p_away_win", "nfl_source"]
        preds_df = preds_df[[c for c in needed_cols if c in preds_df.columns]]
        preds_df = preds_df.drop(columns=["__dedupe_key"], errors="ignore")

        logger.info(
            "Loaded %d NFL predictions (unique game_ids=%d, dropped_duplicates=%d)",
            len(preds_df),
            preds_df["nfl_game_id"].nunique(),
            removed,
        )

        NFL_PREDICTIONS_DF = preds_df
        return NFL_PREDICTIONS_DF

    except Exception as e:
        logger.error("Failed to combine NFL predictions: %s", e, exc_info=True)
        NFL_PREDICTIONS_DF = None
        return None


def load_nba_predictions() -> Optional[pd.DataFrame]:
    """
    Load NBA model predictions from parquet files (historical + future).

    Returns a DataFrame with columns: event_key, p_home_win, p_away_win, source.
    event_key format: NBA|{date_yyyy_mm_dd}|{home_team}|{away_team}
    Returns None if no predictions files exist.
    """
    global NBA_PREDICTIONS_DF

    if NBA_PREDICTIONS_DF is not None:
        return NBA_PREDICTIONS_DF

    historical_path = PROCESSED_DIR / "nba_predictions_b2b_2022plus.parquet"
    future_path = PROCESSED_DIR / "nba_predictions_future.parquet"

    all_preds = []

    # Load historical predictions
    if historical_path.exists():
        try:
            logger.info("Loading historical NBA predictions from %s ...", historical_path)
            hist_df = pd.read_parquet(historical_path)

            # Check for required columns to create event_key
            if "date" in hist_df.columns and "home_team" in hist_df.columns and "away_team" in hist_df.columns:
                # Create event_key in canonical format
                hist_df["event_key"] = (
                    "NBA|" +
                    pd.to_datetime(hist_df["date"]).dt.strftime("%Y-%m-%d") + "|" +
                    hist_df["home_team"].astype(str).str.strip() + "|" +
                    hist_df["away_team"].astype(str).str.strip()
                )

                # Rename p_home/p_away to p_home_win/p_away_win for consistency
                if "p_home" in hist_df.columns:
                    hist_df["p_home_win"] = hist_df["p_home"]
                if "p_away" in hist_df.columns:
                    hist_df["p_away_win"] = hist_df["p_away"]
                elif "p_home" in hist_df.columns:
                    hist_df["p_away_win"] = 1 - hist_df["p_home"]

                hist_df["source"] = "nba_historical"
                all_preds.append(hist_df)
                logger.info("  Loaded %d historical predictions with event_key", len(hist_df))
            else:
                logger.warning("Historical NBA predictions missing required columns (date, home_team, away_team)")

        except Exception as e:
            logger.error("Failed to load historical NBA predictions: %s", e)

    # Load future predictions
    if future_path.exists():
        try:
            logger.info("Loading future NBA predictions from %s ...", future_path)
            future_df = pd.read_parquet(future_path)

            # Check for required columns to create event_key
            if "date" in future_df.columns and "home_team" in future_df.columns and "away_team" in future_df.columns:
                # Create event_key in canonical format
                future_df["event_key"] = (
                    "NBA|" +
                    pd.to_datetime(future_df["date"]).dt.strftime("%Y-%m-%d") + "|" +
                    future_df["home_team"].astype(str).str.strip() + "|" +
                    future_df["away_team"].astype(str).str.strip()
                )

                # Rename p_home/p_away to p_home_win/p_away_win for consistency
                if "p_home" in future_df.columns:
                    future_df["p_home_win"] = future_df["p_home"]
                if "p_away" in future_df.columns:
                    future_df["p_away_win"] = future_df["p_away"]
                elif "p_home" in future_df.columns:
                    future_df["p_away_win"] = 1 - future_df["p_home"]

                future_df["source"] = "nba_future"
                all_preds.append(future_df)
                logger.info("  Loaded %d future predictions with event_key", len(future_df))
            else:
                logger.warning("Future NBA predictions missing required columns (date, home_team, away_team)")

        except Exception as e:
            logger.error("Failed to load future NBA predictions: %s", e)

    if not all_preds:
        logger.warning(
            "No NBA predictions found at %s or %s. NBA games will have no model_snapshot.",
            historical_path,
            future_path
        )
        NBA_PREDICTIONS_DF = None
        return None

    try:
        # Combine all predictions
        preds_df = pd.concat(all_preds, ignore_index=True)
        logger.info("Combined %d total NBA predictions", len(preds_df))

        # Keep only needed columns
        needed_cols = ["event_key", "p_home_win", "p_away_win", "source"]
        available_cols = [col for col in needed_cols if col in preds_df.columns]

        if "event_key" not in available_cols or "p_home_win" not in available_cols:
            logger.error(
                "NBA predictions missing required columns (event_key, p_home_win). "
                "Found columns: %s",
                preds_df.columns.tolist()
            )
            NBA_PREDICTIONS_DF = None
            return None

        preds_df = preds_df[available_cols].copy()

        # Remove duplicates (prefer later predictions)
        preds_df = preds_df.drop_duplicates(subset=["event_key"], keep="last")

        logger.info(
            "Loaded %d NBA predictions (%.1f%% unique event_keys)",
            len(preds_df),
            100 * preds_df["event_key"].nunique() / len(preds_df) if len(preds_df) > 0 else 0
        )

        NBA_PREDICTIONS_DF = preds_df
        return NBA_PREDICTIONS_DF

    except Exception as e:
        logger.error("Failed to load NBA predictions: %s", e, exc_info=True)
        NBA_PREDICTIONS_DF = None
        return None


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

    # Model snapshot (for sports with pre-computed predictions like NFL baseline)
    model_snapshot: Optional[Dict[str, Any]] = None

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
    - Load model artifacts (NBA, NFL)
    """
    logger.info("Startup: loading games table, team lookups, and models.")
    games = load_games_table()
    build_team_lookups(games)
    _ = load_nba_model()

    if "sport" in games.columns:
        nfl_rows = games[games["sport"].astype(str).str.upper() == "NFL"]
    else:
        nfl_rows = games.iloc[0:0]
    logger.info(
        "Startup NFL snapshot: games=%d, unique_nfl_ids=%d",
        len(nfl_rows),
        nfl_rows["nfl_game_id"].nunique() if "nfl_game_id" in nfl_rows.columns else 0,
    )

    # --- Join predictions using canonical ids ---
    global GAMES_DF

    # Load NFL predictions and join via canonical nfl_game_id
    nfl_preds = load_nfl_predictions()
    if nfl_preds is not None:
        if "nfl_game_id" not in games.columns:
            games["nfl_game_id"] = None

        logger.info(
            "Joining NFL predictions via nfl_game_id (pred_rows=%d, unique_ids=%d)...",
            len(nfl_preds),
            nfl_preds["nfl_game_id"].nunique() if "nfl_game_id" in nfl_preds.columns else 0,
        )
        games = games.merge(
            nfl_preds,
            on="nfl_game_id",
            how="left",
            suffixes=("", "_nfl_pred")
        )

        # Count coverage
        nfl_games_mask = games["sport"] == "NFL"
        nfl_games_count = nfl_games_mask.sum()
        nfl_with_preds = games[nfl_games_mask & games["p_home_win"].notna()].shape[0]
        coverage_pct = 100 * nfl_with_preds / nfl_games_count if nfl_games_count > 0 else 0

        logger.info(
            "Joined NFL predictions: %d/%d NFL games have predictions (%.1f%%)",
            nfl_with_preds,
            nfl_games_count,
            coverage_pct
        )

        # Sanity: log duplicate keys before final NFL dedupe
        nfl_subset = games[nfl_games_mask].copy()
        date_str = pd.to_datetime(nfl_subset.get("date")).dt.strftime("%Y-%m-%d")
        nfl_subset["__dedupe_key_tmp"] = nfl_subset["nfl_game_id"].fillna(
            "NFL|" + date_str + "|" +
            nfl_subset.get("home_team", pd.Series([""] * len(nfl_subset))).astype(str).str.strip() + "|" +
            nfl_subset.get("away_team", pd.Series([""] * len(nfl_subset))).astype(str).str.strip()
        )
        dup_before = nfl_subset.duplicated(subset=["__dedupe_key_tmp"], keep=False).sum()
        logger.info("NFL duplicate-key rows before final dedupe: %d", dup_before)
        nfl_subset = nfl_subset.drop(columns=["__dedupe_key_tmp"], errors="ignore")

        # Fallback fill: only for future games missing predictions (no scores yet)
        missing_mask = (
            nfl_games_mask
            & games["p_home_win"].isna()
            & games.get("home_pts", pd.Series([None] * len(games))).isna()
            & games.get("away_pts", pd.Series([None] * len(games))).isna()
        )
        if missing_mask.any():
            # Build matchup key for games
            home_abbr = games.get("home_team_abbr", games.get("home_team")).astype(str).str.strip().str.upper()
            away_abbr = games.get("away_team_abbr", games.get("away_team")).astype(str).str.strip().str.upper()
            games["_match_key"] = home_abbr + "|" + away_abbr

            preds_match = nfl_preds.copy()
            def _mk_match_key(gid: str) -> str | None:
                if not isinstance(gid, str):
                    return None
                parts = gid.split("_")
                if len(parts) < 3:
                    return None
                # home/away are the last two tokens
                home = parts[-2].upper()
                away = parts[-1].upper()
                return f"{home}|{away}"

            preds_match["_match_key"] = preds_match["nfl_game_id"].apply(_mk_match_key)
            preds_match = preds_match.dropna(subset=["_match_key"])
            preds_match = preds_match.drop_duplicates(subset=["_match_key"], keep="last")
            games = games.merge(
                preds_match[["_match_key", "p_home_win", "p_away_win", "nfl_source"]],
                on="_match_key",
                how="left",
                suffixes=("", "_matchfill"),
            )
            fill_mask = missing_mask & games["p_home_win_matchfill"].notna()
            if fill_mask.any():
                games.loc[fill_mask, "p_home_win"] = games.loc[fill_mask, "p_home_win_matchfill"]
                games.loc[fill_mask, "p_away_win"] = games.loc[fill_mask, "p_away_win_matchfill"]
                # Do not overwrite nfl_source if already set
                games.loc[fill_mask & games["nfl_source"].isna(), "nfl_source"] = games.loc[fill_mask, "nfl_source_matchfill"]
                logger.info("NFL fallback fill applied for %d games via matchup key.", int(fill_mask.sum()))
            games = games.drop(columns=["_match_key", "p_home_win_matchfill", "p_away_win_matchfill", "nfl_source_matchfill"], errors="ignore")

    # Load NBA predictions and join via event_key
    nba_preds = load_nba_predictions()
    if nba_preds is not None and "event_key" in games.columns:
        logger.info("Joining NBA predictions via event_key...")

        # Rename NBA columns to avoid conflicts (since NFL uses p_home_win/p_away_win)
        # For NBA, use nba_p_home_win / nba_p_away_win
        nba_preds_renamed = nba_preds.rename(columns={
            "p_home_win": "nba_p_home_win",
            "p_away_win": "nba_p_away_win",
            "source": "nba_source"
        })

        games = games.merge(
            nba_preds_renamed,
            on="event_key",
            how="left",
            suffixes=("", "_nba_pred")
        )

        # Count coverage
        nba_games_mask = games["sport"] == "NBA"
        nba_games_count = nba_games_mask.sum()
        nba_with_preds = games[nba_games_mask & games["nba_p_home_win"].notna()].shape[0]
        coverage_pct = 100 * nba_with_preds / nba_games_count if nba_games_count > 0 else 0

        logger.info(
            "Joined NBA predictions: %d/%d NBA games have predictions (%.1f%%)",
            nba_with_preds,
            nba_games_count,
            coverage_pct
        )

    # Update global games table
    games = dedupe_nfl_games(games)
    GAMES_DF = games

    logger.info(
        "Startup complete: games + models + lookups loaded (num_games=%d).",
        len(games),
    )
    if "sport" in games.columns:
        nfl_rows = games[games["sport"] == "NFL"]
        future_mask = nfl_rows.get("home_pts", pd.Series([None] * len(nfl_rows))).isna() & nfl_rows.get("away_pts", pd.Series([None] * len(nfl_rows))).isna()
        logger.info("NFL startup snapshot: total=%d future_no_scores=%d", len(nfl_rows), int(future_mask.sum()))


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
    sport_id: int | None = None,
) -> ListEventsResponse:
    """
    Return a list of events derived from the processed games table.

    Supports filtering by:
    - sport_id: 1=NBA, 2=MLB, 3=NFL, 4=NHL, 5=UFC
    - year: calendar year
    - limit: max number of results (applied after sorting)
    """
    games = load_games_table()
    df = games

    # Ensure date column is datetime-like
    if not hasattr(df["date"], "dt"):
        df = df.copy()
        df["date"] = pd.to_datetime(df["date"])

    # Filter by sport_id if provided
    if sport_id is not None and "sport" in df.columns:
        sport_map = {
            1: "NBA",
            2: "MLB",
            3: "NFL",
            4: "NHL",
            5: "UFC",
        }
        if sport_id in sport_map:
            df = df[df["sport"] == sport_map[sport_id]]

    # Filter by year if provided
    if year is not None:
        df = df[df["date"].dt.year == year]

    # Sort newest games first
    df = df.sort_values(["date", "game_id"], ascending=[False, False])

    # Apply limit if provided
    if limit is not None:
        df = df.head(limit)

    # --- AUTHORITATIVE FINAL DEDUPLICATION (NFL ALWAYS) ---
    # Ensures exactly one row per NFL game, even when caller does not pass sport_id
    # PRIORITY ORDER: scores (1000) > predictions (500) > market data (100)
    if "sport" in df.columns and (df["sport"] == "NFL").any():
        nfl_df = df[df["sport"] == "NFL"].copy()
        other_df = df[df["sport"] != "NFL"].copy()

        if len(nfl_df) > 0:
            # Use UTC-normalized date for dedup key
            date_normalized = pd.to_datetime(nfl_df["date"], utc=True).dt.tz_localize(None).dt.strftime("%Y-%m-%d")
            nfl_df["_dedup_key"] = (
                date_normalized + "|" +
                nfl_df["home_team"].astype(str).str.strip().str.upper() + "|" +
                nfl_df["away_team"].astype(str).str.strip().str.upper()
            )

            nfl_df["_priority"] = 0

            # CORRECTED PRIORITY ORDER: scores > predictions > odds
            has_scores = nfl_df.get("home_pts", pd.Series([None] * len(nfl_df))).notna() & nfl_df.get("away_pts", pd.Series([None] * len(nfl_df))).notna()
            nfl_df.loc[has_scores, "_priority"] += 1000

            has_model = nfl_df.get("p_home_win", pd.Series([None] * len(nfl_df))).notna()
            nfl_df.loc[has_model, "_priority"] += 500

            has_odds = (
                nfl_df.get("home_moneyline", pd.Series([None] * len(nfl_df))).notna()
                | nfl_df.get("spread_line", pd.Series([None] * len(nfl_df))).notna()
            )
            nfl_df.loc[has_odds, "_priority"] += 100

            nfl_df = nfl_df.sort_values(["_dedup_key", "_priority"], ascending=[True, False])
            duplicated_keys = nfl_df[nfl_df.duplicated(subset=["_dedup_key"], keep=False)]["_dedup_key"].unique()
            num_duplicates = int(nfl_df.duplicated(subset=["_dedup_key"], keep="first").sum())

            nfl_df = nfl_df.drop_duplicates(subset=["_dedup_key"], keep="first")
            nfl_df = nfl_df.drop(columns=["_dedup_key", "_priority"])

            if num_duplicates > 0:
                logger.warning(
                    "NFL final deduplication removed %d duplicate rows (before=%d, after=%d). Priority: scores (%d) > predictions (%d) > market (%d)",
                    num_duplicates,
                    len(df[df["sport"] == "NFL"]),
                    len(nfl_df),
                    has_scores.sum(),
                    has_model.sum(),
                    has_odds.sum(),
                )
                logger.info("Duplicated matchups: %s", list(duplicated_keys)[:10])
            else:
                logger.info(
                    "NFL final deduplication: no duplicates found (verified %d unique games)",
                    len(nfl_df),
                )

            # Extra pass for upcoming games within the same matchup to prefer predictions/odds
            match_key = (
                nfl_df["home_team"].astype(str).str.strip()
                + "|"
                + nfl_df["away_team"].astype(str).str.strip()
            )
            nfl_df["_match_key"] = match_key
            dup_mask = nfl_df.duplicated(subset=["_match_key"], keep=False)
            upcoming_mask = nfl_df.get("home_pts", pd.Series([None] * len(nfl_df))).isna() & nfl_df.get("away_pts", pd.Series([None] * len(nfl_df))).isna()
            sub = nfl_df[dup_mask & upcoming_mask].copy()
            if not sub.empty:
                sub["_priority"] = 0
                has_model = sub.get("p_home_win", pd.Series([None] * len(sub))).notna()
                sub.loc[has_model, "_priority"] += 100
                has_odds = (
                    sub.get("home_moneyline", pd.Series([None] * len(sub))).notna()
                    | sub.get("spread_line", pd.Series([None] * len(sub))).notna()
                )
                sub.loc[has_odds, "_priority"] += 50
                # Prefer later date for upcoming games (UTC vs local skew)
                sub["_date_dt"] = pd.to_datetime(sub.get("date"))
                sub = sub.sort_values(["_match_key", "_priority", "_date_dt"], ascending=[True, False, False])
                keep_idx = sub.groupby("_match_key").head(1).index
                drop_idx = set(sub.index) - set(keep_idx)
                if drop_idx:
                    nfl_df = nfl_df.drop(index=drop_idx)
                    logger.info("NFL final deduplication (match-level) dropped %d lower-priority upcoming duplicates.", len(drop_idx))
                nfl_df = nfl_df.drop(columns=["_priority", "_date_dt"], errors="ignore")
            nfl_df = nfl_df.drop(columns=["_match_key"], errors="ignore")

        df = pd.concat([other_df, nfl_df], ignore_index=True)

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

        # Build model_snapshot for NFL/NBA games with predictions
        model_snapshot: Optional[Dict[str, Any]] = None
        sport_str = str(row.get("sport", "NBA")).upper()

        if sport_str == "NFL" and "p_home_win" in row and pd.notna(row["p_home_win"]):
            p_home = float(row["p_home_win"])
            p_away = float(row.get("p_away_win", 1.0 - p_home))
            source = str(row.get("nfl_source", "nfl_baseline_logreg_v1"))
            model_snapshot = {
                "source": source,
                "p_home_win": p_home,
                "p_away_win": p_away
            }
        elif sport_str == "NBA" and "nba_p_home_win" in row and pd.notna(row["nba_p_home_win"]):
            p_home = float(row["nba_p_home_win"])
            p_away = float(row.get("nba_p_away_win", 1.0 - p_home))
            source = str(row.get("nba_source", "nba_b2b_logreg_v1"))
            model_snapshot = {
                "source": source,
                "p_home_win": p_home,
                "p_away_win": p_away
            }

        # Determine sport_id per row (NBA vs MLB vs NFL vs NHL vs UFC)
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
                model_snapshot=model_snapshot,
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

    # Build model_snapshot for NFL/NBA games with predictions
    model_snapshot: Optional[Dict[str, Any]] = None
    sport_str = str(row.get("sport", "NBA")).upper()

    if sport_str == "NFL" and "p_home_win" in row and pd.notna(row["p_home_win"]):
        p_home = float(row["p_home_win"])
        p_away = float(row.get("p_away_win", 1.0 - p_home))
        source = str(row.get("nfl_source", "nfl_baseline_logreg_v1"))
        model_snapshot = {
            "source": source,
            "p_home_win": p_home,
            "p_away_win": p_away
        }
    elif sport_str == "NBA" and "nba_p_home_win" in row and pd.notna(row["nba_p_home_win"]):
        p_home = float(row["nba_p_home_win"])
        p_away = float(row.get("nba_p_away_win", 1.0 - p_home))
        source = str(row.get("nba_source", "nba_b2b_logreg_v1"))
        model_snapshot = {
            "source": source,
            "p_home_win": p_home,
            "p_away_win": p_away
        }
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
        model_snapshot=model_snapshot,
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


@app.get("/debug/predictions_coverage")
def debug_predictions_coverage():
    """
    Debug endpoint to show prediction coverage and deduplication stats.

    Returns:
        - total games per sport
        - % with predictions
        - % with market snapshot
        - duplicates removed during last load
    """
    games = load_games_table()

    if games is None or len(games) == 0:
        return {
            "error": "No games loaded",
            "total_games": 0
        }

    result = {
        "total_games": len(games),
        "sports": {}
    }

    # Get stats per sport
    for sport in ["NBA", "NFL", "MLB", "NHL", "UFC"]:
        sport_mask = games["sport"] == sport
        sport_count = sport_mask.sum()

        if sport_count == 0:
            continue

        sport_games = games[sport_mask]

        # Check for predictions based on sport-specific columns
        if sport == "NFL":
            has_pred = sport_games["p_home_win"].notna()
        elif sport == "NBA":
            has_pred = sport_games["nba_p_home_win"].notna()
        else:
            has_pred = pd.Series([False] * len(sport_games))

        pred_count = has_pred.sum()
        pred_pct = 100 * pred_count / sport_count if sport_count > 0 else 0

        # Check for market snapshot (odds data)
        has_market = (
            sport_games.get("home_moneyline", pd.Series([None] * len(sport_games))).notna() |
            sport_games.get("spread_line", pd.Series([None] * len(sport_games))).notna()
        )
        market_count = has_market.sum()
        market_pct = 100 * market_count / sport_count if sport_count > 0 else 0

        result["sports"][sport] = {
            "total_games": int(sport_count),
            "with_predictions": int(pred_count),
            "predictions_pct": round(pred_pct, 1),
            "with_market_snapshot": int(market_count),
            "market_snapshot_pct": round(market_pct, 1)
        }

    # Note: Duplicates removed is logged during load_games_table() but not persisted
    # For now, we can just indicate that deduplication is active
    result["deduplication"] = "active (event_key-based)"

    return result


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
