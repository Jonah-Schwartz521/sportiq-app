from pathlib import Path
import sys
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.paths import PROCESSED_DIR
from src.nba_inference import predict_home_win_proba, load_nba_model

def main():
    games = pd.read_parquet(PROCESSED_DIR / "processed_games_b2b_model.parquet")
    artifact = load_nba_model()
    features = artifact["features"]

    # Assert features all exist
    missing = [f for f in features if f not in games.columns]
    if missing:
        raise ValueError(f"Missing features in games table: {missing}")

    probs = games.apply(predict_home_win_proba, axis=1)
    games["p_home_win_model"] = probs

    out_path = PROCESSED_DIR / "processed_games_b2b_scored.parquet"
    games.to_parquet(out_path, index=False)
    print("Wrote scored file to:", out_path)

if __name__ == "__main__":
    main()