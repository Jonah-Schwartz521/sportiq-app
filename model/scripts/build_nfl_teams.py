# model/scripts/build_nfl_teams.py

"""
Build NFL teams parquet using teams_colors_logos.parquet and nfl_games.parquet.

Inputs:
  - PROCESSED_DIR / "teams_colors_logos.parquet"  (from nflverse, etc.)
  - NFL_PROCESSED_DIR / "nfl_games.parquet"

Output:
  - NFL_PROCESSED_DIR / "nfl_teams.parquet"

Columns (output):
  - season  (int)
  - team_abbr
  - team_name
  - team_id
  - team_conf
  - team_division
  - logo_url
"""

from pathlib import Path
import sys
import logging

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # /model
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.paths import PROCESSED_DIR, NFL_PROCESSED_DIR  # type: ignore[attr-defined]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    teams_path = PROCESSED_DIR / "teams_colors_logos.parquet"
    games_path = NFL_PROCESSED_DIR / "nfl_games.parquet"
    out_path = NFL_PROCESSED_DIR / "nfl_teams.parquet"

    logger.info("=== Building NFL teams from %s ===", teams_path)

    if not teams_path.exists():
        raise FileNotFoundError(f"teams_colors_logos.parquet not found: {teams_path}")
    if not games_path.exists():
        raise FileNotFoundError(f"nfl_games.parquet not found: {games_path}")

    teams_raw = pd.read_parquet(teams_path)
    logger.info("teams_raw columns: %s", list(teams_raw.columns))

    # Only NFL rows; filter by league or presence of NFL team_conf if needed.
    # Many nflverse exports already only contain NFL data, so we just keep as-is.
    teams = teams_raw.copy()

    # Normalize logo URL column
    if "team_logo_squared" in teams.columns:
        logo_col = "team_logo_squared"
    elif "team_logo_wikipedia" in teams.columns:
        logo_col = "team_logo_wikipedia"
    else:
        logo_col = None

    if logo_col is None:
        teams["logo_url"] = None
    else:
        teams["logo_url"] = teams[logo_col]

    # Keep a slimmer set of columns
    keep_cols = [
        "team_abbr",
        "team_name",
        "team_id",
        "team_nick",
        "team_conf",
        "team_division",
        "logo_url",
    ]
    teams = teams[[c for c in keep_cols if c in teams.columns]].copy()

    games = pd.read_parquet(games_path)
    if "season" not in games.columns:
        raise ValueError("nfl_games.parquet is missing 'season' column")

    seasons = sorted(pd.unique(games["season"]))
    logger.info("Seasons inferred from games: %s–%s", seasons[0], seasons[-1])

    # Cross product teams × seasons
    rows = []
    for s in seasons:
        t = teams.copy()
        t["season"] = int(s)
        rows.append(t)

    nfl_teams = pd.concat(rows, ignore_index=True)

    # Reorder columns
    out_cols = ["season"] + [c for c in nfl_teams.columns if c != "season"]
    nfl_teams = nfl_teams[out_cols]

    # Quick sample of latest season
    latest = int(seasons[-1])
    logger.info("Sample:")
    logger.info(
        "%s",
        nfl_teams[nfl_teams["season"] == latest]
        .head(10)[["season", "team_abbr", "team_name", "logo_url"]],
    )

    NFL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    nfl_teams.to_parquet(out_path, index=False)

    logger.info("Wrote nfl_teams.parquet to:\n  %s", out_path)


if __name__ == "__main__":
    main()