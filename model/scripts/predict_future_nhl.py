#!/usr/bin/env python3
"""
Generate future NHL win-probability predictions.

Reads:
- model/data/processed/nhl/nhl_model_games.parquet
- model/artifacts/nhl/nhl_model.joblib
- model/artifacts/nhl/feature_columns.json

Writes:
- model/data/processed/nhl/nhl_predictions_future.parquet
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import numpy as np
import pandas as pd
from joblib import load

ROOT_DIR = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT_DIR / "model" / "data" / "processed" / "nhl"
ARTIFACTS_DIR = ROOT_DIR / "model" / "artifacts" / "nhl"
OUTPUT_PATH = PROCESSED_DIR / "nhl_predictions_future.parquet"
SCHEDULE_PATH = PROCESSED_DIR / "nhl_games_for_app.parquet"

# Canonical mapping from abbreviations/variants to full team names.
TEAM_NAME_MAP = {
    "ANA": "Anaheim Ducks",
    "ANAHEIMDUCKS": "Anaheim Ducks",
    "ARI": "Arizona Coyotes",
    "ARIZONACOYOTES": "Arizona Coyotes",
    "ATL": "Atlanta Thrashers",
    "ATLANTATHRASHERS": "Atlanta Thrashers",
    "BOS": "Boston Bruins",
    "BOSTONBRUINS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "BUFFALOSABRES": "Buffalo Sabres",
    "CAR": "Carolina Hurricanes",
    "CAROLINAHURRICANES": "Carolina Hurricanes",
    "CBJ": "Columbus Blue Jackets",
    "COLUMBUSBLUEJACKETS": "Columbus Blue Jackets",
    "CGY": "Calgary Flames",
    "CALGARYFLAMES": "Calgary Flames",
    "CHI": "Chicago Blackhawks",
    "CHICAGOBLACKHAWKS": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",
    "COLORADOAVALANCHE": "Colorado Avalanche",
    "DAL": "Dallas Stars",
    "DALLASSTARS": "Dallas Stars",
    "DET": "Detroit Red Wings",
    "DETROITREDWINGS": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "EDMONTONOILERS": "Edmonton Oilers",
    "FLA": "Florida Panthers",
    "FLORIDAPANTHERS": "Florida Panthers",
    "LA": "Los Angeles Kings",
    "LAK": "Los Angeles Kings",
    "LOSANGELESKINGS": "Los Angeles Kings",
    "LAKINGS": "Los Angeles Kings",
    "MIN": "Minnesota Wild",
    "MINNESOTAWILD": "Minnesota Wild",
    "MTL": "Montreal Canadiens",
    "MONTREALCANADIENS": "Montreal Canadiens",
    "NJ": "New Jersey Devils",
    "NJD": "New Jersey Devils",
    "NEWJERSEYDEVILS": "New Jersey Devils",
    "NSH": "Nashville Predators",
    "NASHVILLEPREDATORS": "Nashville Predators",
    "NYI": "New York Islanders",
    "NEWYORKISLANDERS": "New York Islanders",
    "NYR": "New York Rangers",
    "NEWYORKRANGERS": "New York Rangers",
    "OTT": "Ottawa Senators",
    "OTTAWASENATORS": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PHILADELPHIAFLYERS": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "PITTSBURGHPENGUINS": "Pittsburgh Penguins",
    "SJ": "San Jose Sharks",
    "SJS": "San Jose Sharks",
    "SANJOSESHARKS": "San Jose Sharks",
    "SEA": "Seattle Kraken",
    "SEATTLEKRAKEN": "Seattle Kraken",
    "STL": "St. Louis Blues",
    "STLOUISBLUES": "St. Louis Blues",
    "TB": "Tampa Bay Lightning",
    "TBL": "Tampa Bay Lightning",
    "TAMPA BAY LIGHTNING": "Tampa Bay Lightning",
    "TAMPABAYLIGHTNING": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "TORONTOMAPLELEAFS": "Toronto Maple Leafs",
    "UTA": "Utah Hockey Club",
    "UTAH": "Utah Hockey Club",
    "UTAHHOCKEYCLUB": "Utah Hockey Club",
    "VAN": "Vancouver Canucks",
    "VANCOUVERCANUCKS": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights",
    "VEGASGOLDENKNIGHTS": "Vegas Golden Knights",
    "WPG": "Winnipeg Jets",
    "WINNIPEGJETS": "Winnipeg Jets",
    "WSH": "Washington Capitals",
    "WASHINGTONCAPITALS": "Washington Capitals",
}


def canonical_team_name(raw: str | None) -> str | None:
    if raw is None or (isinstance(raw, float) and np.isnan(raw)):
        return None
    token = (
        str(raw)
        .upper()
        .replace(" ", "")
        .replace(".", "")
        .replace("-", "")
        .replace("_", "")
    )
    return TEAM_NAME_MAP.get(token, str(raw))


def load_artifacts():
    model_path = ARTIFACTS_DIR / "nhl_model.joblib"
    feats_path = ARTIFACTS_DIR / "feature_columns.json"

    if not model_path.exists() or not feats_path.exists():
        raise FileNotFoundError(
            "Missing NHL artifacts. Run train_nhl_baseline.py first."
        )

    model = load(model_path)
    feature_cols = json.loads(feats_path.read_text())["features"]
    return model, feature_cols


def load_model_games() -> pd.DataFrame:
    path = PROCESSED_DIR / "nhl_model_games.parquet"
    if not path.exists():
        raise FileNotFoundError(
            "nhl_model_games.parquet not found. Run build_nhl_model_games.py first."
        )
    df = pd.read_parquet(path)
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def main() -> None:
    model, feature_cols = load_artifacts()
    model_games = load_model_games()
    model_games["home_team_canon"] = model_games["home_team"].apply(canonical_team_name)
    model_games["away_team_canon"] = model_games["away_team"].apply(canonical_team_name)

    # Latest feature snapshot per team from scored games.
    # IMPORTANT: Use the correct home/away orientation for that team.
    scored = model_games.dropna(subset=["home_pts", "away_pts"]).sort_values("date")
    feature_bases = {
        col[len("home_") :] if col.startswith("home_") else col[len("away_") :]
        for col in feature_cols
    }

    team_latest: dict[str, dict[str, float]] = {}

    def default_for_base(base: str) -> float:
        return 0.5 if "pct" in base else 0.0

    for team in pd.unique(scored["home_team_canon"].tolist() + scored["away_team_canon"].tolist()):
        if pd.isna(team):
            continue

        tg = scored[(scored["home_team_canon"] == team) | (scored["away_team_canon"] == team)]
        if tg.empty:
            continue

        last_row = tg.iloc[-1]
        feats: dict[str, float] = {}

        for base in feature_bases:
            home_col = f"home_{base}"
            away_col = f"away_{base}"

            if last_row.get("home_team_canon") == team and home_col in last_row:
                val = last_row.get(home_col)
            elif last_row.get("away_team_canon") == team and away_col in last_row:
                val = last_row.get(away_col)
            else:
                val = np.nan

            if pd.isna(val):
                val = default_for_base(base)
            feats[base] = float(val)

        team_latest[str(team)] = feats

    # Helper to build nhl_game_id_str
    def build_id(row: pd.Series) -> str | None:
        dt = pd.to_datetime(row.get("date"), errors="coerce")
        if pd.isna(dt):
            return None

        def clean(val):
            if val is None or pd.isna(val):
                return None
            return str(val).upper().replace(" ", "").replace(".", "").replace("-", "").replace("_", "")

        home_canon = canonical_team_name(row.get("home_team")) or row.get("home_team_abbrev")
        away_canon = canonical_team_name(row.get("away_team")) or row.get("away_team_abbrev")

        home = clean(home_canon)
        away = clean(away_canon)
        if home is None or away is None:
            return None
        return f"{dt.strftime('%Y_%m_%d')}_{home}_{away}"

    # Load schedule/app data for upcoming rows without scores
    if not SCHEDULE_PATH.exists():
        print("No NHL schedule found; skipping predictions.")
        OUTPUT_PATH.unlink(missing_ok=True)
        return

    sched = pd.read_parquet(SCHEDULE_PATH).copy()
    sched = sched.rename(
        columns={
            "game_datetime": "date",
            "home_team_name": "home_team",
            "away_team_name": "away_team",
            "home_score": "home_pts",
            "away_score": "away_pts",
        }
    )
    for col in ["home_pts", "away_pts"]:
        if col not in sched.columns:
            sched[col] = None
    sched["date"] = pd.to_datetime(sched["date"], errors="coerce")

    no_scores = sched["home_pts"].isna() & sched["away_pts"].isna()
    upcoming = sched[no_scores].copy()
    if upcoming.empty:
        print("No upcoming NHL games to predict.")
        OUTPUT_PATH.unlink(missing_ok=True)
        return

    upcoming["home_team_canon"] = upcoming["home_team"].apply(canonical_team_name)
    upcoming["away_team_canon"] = upcoming["away_team"].apply(canonical_team_name)
    upcoming["nhl_game_id_str"] = upcoming.apply(build_id, axis=1)

    # Fill features from latest per-team stats
    for prefix in ("home", "away"):
        team_col = f"{prefix}_team_canon"
        for feat_base in feature_bases:
            col_name = f"{prefix}_{feat_base}"

            def mapper(team):
                feats = team_latest.get(str(team), {})
                val = feats.get(feat_base)
                if val is None:
                    return default_for_base(feat_base)
                return val

            upcoming[col_name] = upcoming[team_col].apply(mapper)

    X = upcoming[feature_cols].fillna(0.0)
    pct_cols = [c for c in feature_cols if "win_pct" in c]
    for col in pct_cols:
        if col in X.columns:
            X[col] = X[col].fillna(0.5)

    proba = model.predict_proba(X.to_numpy())[:, 1]
    extreme_mask = (proba < 0.1) | (proba > 0.9)
    extreme_frac = float(extreme_mask.mean()) if len(proba) else 0.0
    if extreme_frac > 0.5:
        print(
            f"⚠️  Warning: {extreme_frac:.0%} of NHL predictions are extreme (<0.1 or >0.9). "
            "Check feature pipeline for leakage or data quality issues."
        )
    else:
        print(
            f"NHL prediction distribution — min={proba.min():.3f}, max={proba.max():.3f}, "
            f"mean={proba.mean():.3f}, extreme_frac={extreme_frac:.3f}"
        )
    upcoming["p_home_win"] = proba
    upcoming["p_away_win"] = 1 - proba
    upcoming["source"] = "nhl_logreg_v1"
    upcoming["generated_at"] = datetime.utcnow()

    out_cols = [
        "nhl_game_id_str",
        "date",
        "home_team",
        "away_team",
        "p_home_win",
        "p_away_win",
        "source",
        "generated_at",
    ]
    upcoming[out_cols].to_parquet(OUTPUT_PATH, index=False)
    print(f"✅ Wrote {len(upcoming)} NHL predictions to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
