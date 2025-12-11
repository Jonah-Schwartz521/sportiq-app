# model/scripts/build_nfl_model_input.py

"""
Build a simple NFL model_input parquet for future modeling.

Input:
  - NFL_PROCESSED_DIR / "nfl_games.parquet"

Output:
  - NFL_PROCESSED_DIR / "nfl_model_input.parquet"

For now this is basically a cleaned copy of nfl_games with a couple of
back-to-back flags set to 0. That is enough for experimentation and
won't affect the current API.
"""

from pathlib import Path
import sys
import logging

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]  # /model
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.paths import NFL_PROCESSED_DIR  # type: ignore[attr-defined]

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger(__name__)


def main() -> None:
    in_path = NFL_PROCESSED_DIR / "nfl_games.parquet"
    out_path = NFL_PROCESSED_DIR / "nfl_model_input.parquet"

    logger.info("=== Building NFL model_input from games + teams ===")

    if not in_path.exists():
        raise FileNotFoundError(f"nfl_games.parquet not found: {in_path}")

    df = pd.read_parquet(in_path).copy()

    # Ensure core columns
    for col in ["sport", "season", "event_id", "game_id"]:
        if col not in df.columns:
            logger.warning("Column %s missing in nfl_games; filling with defaults.", col)
            if col == "sport":
                df[col] = "NFL"
            elif col == "season":
                df[col] = df["date"].dt.year
            else:
                df[col] = None

    # Simple back-to-back flags (all zero for now)
    if "home_is_b2b" not in df.columns:
        df["home_is_b2b"] = 0
    if "away_is_b2b" not in df.columns:
        df["away_is_b2b"] = 0

    logger.info("Sample:")
    logger.info("%s", df.head()[["sport", "season", "event_id", "game_id"]])

    NFL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path, index=False)

    logger.info("Wrote nfl_model_input to %s", out_path)


if __name__ == "__main__":
    main()