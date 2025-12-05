from pathlib import Path
import sys 
import pandas as pd

# Point to /model 
ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.append(str(ROOT))

from src.paths import PROCESSED_DIR
from src.nba_inference import predict_home_win_proba

def main():
    games = pd.read_parquet(PROCESSED_DIR / "processed_games_b2b_model.parquet")
    row = games.iloc[0]
    p = predict_home_win_proba(row)
    print("P(home win) =", p)

if __name__ == "__main__":
    main()