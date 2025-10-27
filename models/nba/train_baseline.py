import numpy as np
import pandas as pd
import json
from pathlib import Path
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import brier_score_loss, log_loss
from joblib import dump

VERSION = "0.1.0"
ART = Path("models/nba/artifacts")
ART.mkdir(parents=True, exist_ok=True)

FEATURES = ["home_flag", "prior_home_adv", "home_elo_proxy", "away_elo_proxy"]

def load_or_make_dataset(n=500):
    rng = np.random.default_rng(2024)
    df = pd.DataFrame({
        "home_flag": np.ones(n),
        "prior_home_adv": rng.normal(0.05, 0.03, n),
        "home_elo_proxy": rng.normal(0, 1, n),
        "away_elo_proxy": rng.normal(0, 1, n)
    })
    base = 0.5 + 0.3 * df["prior_home_adv"] + 0.1 * (df["home_elo_proxy"] - df["away_elo_proxy"])
    p = np.clip(base, 0.05, 0.95)
    df["y_home_win"] = (rng.random(n) < p).astype(int)
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

    print("✅ Model trained and saved to:", ART.resolve())
    print("📊 Metrics:", metrics)

if __name__ == "__main__":
    main()