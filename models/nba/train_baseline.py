from pathlib import Path
import json
import numpy as np
import pandas as pd
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from sklearn.model_selection import train_test_split
from joblib import dump

ART = Path("models/nba/artifacts"); ART.mkdir(parents=True, exist_ok=True)
DATA_PARQUET = Path("data/processed/nba_train.parquet")
DATA_CSV = Path("data/processed/nba_train.csv")

FEATURES = ["home_flag", "prior_home_adv", "home_elo_proxy", "away_elo_proxy"]
VERSION = "0.1.0"

def load_or_make_dataset():
    # Try parquet, then csv, else synthesize a small dataset
    if DATA_PARQUET.exists():
        df = pd.read_parquet(DATA_PARQUET)
    elif DATA_CSV.exists():
        df = pd.read_csv(DATA_CSV)
    else:
        n = 800
        rng = np.random.default_rng(42)
        df = pd.DataFrame({
            "home_flag": np.ones(n, dtype=int),
            "prior_home_adv": rng.normal(0.58, 0.05, size=n).clip(0.4, 0.75),
            "home_elo_proxy": rng.normal(0.0, 1.0, size=n),
            "away_elo_proxy": rng.normal(0.0, 1.0, size=n),
        })
    # Fabricate a target from a logistic-ish probability (demo)
    rng = np.random.default_rng(123)
    base = (df["prior_home_adv"].to_numpy()
            + 0.12 * (df["home_elo_proxy"] - df["away_elo_proxy"]).to_numpy() / 4.0)
    p = np.clip(base, 0.05, 0.95)
    y = (rng.random(len(df)) < p).astype(int)  # 1 = home win
    df["y_home_win"] = y
    return df

def main():
    df = load_or_make_dataset()
    X = df[FEATURES].to_numpy()
    y = df["y_home_win"].to_numpy()

    Xtr, Xval, ytr, yval = train_test_split(X, y, test_size=0.25, random_state=2024, stratify=y)

    model = LogisticRegression(max_iter=1000)
    model.fit(Xtr, ytr)

    proba_val = model.predict_proba(Xval)[:, 1]
    metrics = {
        "brier": float(brier_score_loss(yval, proba_val)),
        "logloss": float(log_loss(yval, proba_val)),
        "n_train": int(len(Xtr)),
        "n_val": int(len(Xval)),
        "version": VERSION,
    }

    dump(model, ART / "model.joblib")
    (ART / "feature_meta.json").write_text(json.dumps({"features": FEATURES, "target": "y_home_win"}))
    (ART / "metrics.json").write_text(json.dumps(metrics, indent=2))

    print("Saved artifacts:", ART.resolve())
    print("Metrics:", metrics)

if __name__ == "__main__":
    main()
