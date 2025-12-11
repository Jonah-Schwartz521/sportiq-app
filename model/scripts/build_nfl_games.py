# model/scripts/build_nfl_games.py

"""
Build historical NFL games parquet from the main NFL-specific games.parquet.

Input:
  - NFL_PROCESSED_DIR / "games.parquet"

Output:
  - NFL_PROCESSED_DIR / "nfl_games.parquet"

This normalizes key columns so the API can treat NFL the same way as NBA/MLB in main.load_games_table().
"""

from pathlib import Path
import sys
import logging

import pandas as pd

# Ensure we can import src.*
ROOT = Path(__file__).resolve().parents[1]  # /model
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.paths import NFL_PROCESSED_DIR  # type: ignore[attr-defined]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def parse_nfl_kickoff_time(raw_time_str) -> str:
    """
    Parse NFL kickoff time from raw schedule.

    Input examples:
      - "13:00" (already 24-hour) → "13:00"
      - "16:25" → "16:25"
      - "20:30" → "20:30"
      - "1:00p" → "13:00"
      - "4:25p" → "16:25"
      - None/"TBD"/blank → "13:00" (default to 1:00 PM ET Sunday)

    Returns: 24-hour time string "HH:MM"
    """
    import re

    # Handle missing/blank times
    if pd.isna(raw_time_str) or str(raw_time_str).strip() in ("", "TBD", "None"):
        return "13:00"  # Default to 1:00 PM ET

    time_str = str(raw_time_str).strip().lower()

    # Already in 24-hour format like "13:00", "16:25"
    if re.match(r'^\d{1,2}:\d{2}$', time_str):
        parts = time_str.split(":")
        hour = int(parts[0])
        minute = parts[1]
        return f"{hour:02d}:{minute}"

    # Has AM/PM indicator like "1:00p" or "4:25p"
    if 'p' in time_str or 'a' in time_str:
        match = re.match(r'(\d{1,2}):?(\d{2})?\s*([ap])', time_str)
        if match:
            hour = int(match.group(1))
            minute = int(match.group(2)) if match.group(2) else 0
            period = match.group(3)

            if period == 'p' and hour != 12:
                hour += 12
            elif period == 'a' and hour == 12:
                hour = 0

            return f"{hour:02d}:{minute:02d}"

    # Fallback
    return "13:00"


def main() -> None:
    in_path = NFL_PROCESSED_DIR / "games.parquet"
    out_path = NFL_PROCESSED_DIR / "nfl_games.parquet"

    logger.info("=== Building NFL games from %s ===", in_path)

    if not in_path.exists():
        raise FileNotFoundError(f"Input games parquet not found: {in_path}")

    df = pd.read_parquet(in_path)
    logger.info("Input columns: %s", list(df.columns))

    # Ensure we have a 'date' column
    if "date" not in df.columns:
        # Common nflverse-style column names
        if "gameday" in df.columns:
            logger.info("Creating 'date' column from 'gameday'")
            df["date"] = pd.to_datetime(df["gameday"])
        elif "game_date" in df.columns:
            logger.info("Creating 'date' column from 'game_date'")
            df["date"] = pd.to_datetime(df["game_date"])
        else:
            raise ValueError(
                "NFL games parquet is missing a 'date' column and also "
                "does not have a recognizable alternative like 'gameday' or 'game_date'. "
                f"Available columns: {list(df.columns)}"
            )

    if "sport" not in df.columns:
        df["sport"] = "NFL"

    nfl = df.copy()

    # Ensure date is datetime
    if not hasattr(nfl["date"], "dt"):
        nfl["date"] = pd.to_datetime(nfl["date"])

    # Normalize team fields:
    # - ensure we always have home_team and away_team abbreviations
    # - ensure *_team_name are non-null strings (used directly by the API/UI)
    home_source = None
    if "home_team_name" in nfl.columns and nfl["home_team_name"].notna().any():
        home_source = "home_team_name"
    elif "home_team" in nfl.columns and nfl["home_team"].notna().any():
        home_source = "home_team"
    else:
        raise ValueError(
            "NFL rows missing usable home team columns (home_team_name/home_team)"
        )

    away_source = None
    if "away_team_name" in nfl.columns and nfl["away_team_name"].notna().any():
        away_source = "away_team_name"
    elif "away_team" in nfl.columns and nfl["away_team"].notna().any():
        away_source = "away_team"
    else:
        raise ValueError(
            "NFL rows missing usable away team columns (away_team_name/away_team)"
        )

    # Fill names from the chosen sources
    nfl["home_team_name"] = nfl[home_source].astype(str)
    nfl["away_team_name"] = nfl[away_source].astype(str)

    # Also make sure abbreviation columns exist for downstream logic
    if "home_team" not in nfl.columns:
        nfl["home_team"] = nfl["home_team_name"]
    if "away_team" not in nfl.columns:
        nfl["away_team"] = nfl["away_team_name"]

    # Ensure score columns exist
    if "home_score" not in nfl.columns:
        nfl["home_score"] = None
    if "away_score" not in nfl.columns:
        nfl["away_score"] = None

    # Ensure home_win exists (0/1), but it's ok if it's missing for some rows
    if "home_win" not in nfl.columns:
        nfl["home_win"] = None

    # Parse and normalize kickoff times to match NBA format
    if "gametime" in nfl.columns:
        logger.info("Parsing NFL kickoff times from 'gametime' column...")
        nfl["start_et"] = nfl["gametime"].apply(parse_nfl_kickoff_time)
    else:
        logger.warning("No 'gametime' column found - using default 1:00 PM for all games")
        nfl["start_et"] = "13:00"

    # Ensure event_id exists (string)
    if "event_id" not in nfl.columns:
        # fall back to game_id or a simple composite
        if "game_id" in nfl.columns:
            nfl["event_id"] = nfl["game_id"].astype(str)
        else:
            nfl["event_id"] = (
                nfl["date"].dt.strftime("%Y%m%d")
                + "_"
                + nfl["away_team_name"].astype(str)
                + "_"
                + nfl["home_team_name"].astype(str)
            )

    # Sort for stability
    sort_cols = [c for c in ["season", "date", "home_team_name", "away_team_name"] if c in nfl.columns]
    if sort_cols:
        nfl = nfl.sort_values(sort_cols).reset_index(drop=True)

    logger.info("Sample:")
    logger.info("%s", nfl.head()[["event_id", "sport", "home_team_name", "away_team_name", "home_win", "game_id"]])

    NFL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    nfl.to_parquet(out_path, index=False)

    logger.info("Wrote nfl_games.parquet to:\n  %s", out_path)


if __name__ == "__main__":
    main()
  