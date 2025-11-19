# model/src/paths.py

from pathlib import Path

# ---- Base directories ----
MODEL_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = MODEL_DIR / "data"

RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

# ---- NBA schedule results ----
RAW_NBA_SCHEDULE_DIR = RAW_DIR / "NBA_schedule_results"