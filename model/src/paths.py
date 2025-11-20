from pathlib import Path

MODEL_ROOT = Path(__file__).resolve().parents[1]

DATA_DIR = MODEL_ROOT / "data"
RAW_DIR = DATA_DIR / "raw"
INTERIM_DIR = DATA_DIR / "interim"
PROCESSED_DIR = DATA_DIR / "processed"

MODEL_DIR = MODEL_ROOT 