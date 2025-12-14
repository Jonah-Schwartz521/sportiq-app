#!/usr/bin/env python3
"""
Generate predictions for upcoming NFL games.

This script:
1. Loads upcoming NFL schedule from the games database
2. Computes required rolling features based on recent team performance
3. Generates win probability predictions using the trained model
4. Saves predictions to parquet for API consumption
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import pickle

# Add project root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.paths import PROCESSED_DIR, ARTIFACTS_DIR

NFL_TEAM_ABBR_ALIASES = {
    "ARZ": "ARI",
    "GNB": "GB",
    "KAN": "KC",
    "JAC": "JAX",
    "LA": "LAR",
    "LVR": "LV",
    "OAK": "LV",
    "SD": "LAC",
    "STL": "LAR",
    "TAM": "TB",
    "WSH": "WAS",
    "WFT": "WAS",
}


def canonical_abbr(abbr: str) -> str:
    if abbr is None or (isinstance(abbr, float) and np.isnan(abbr)):
        return abbr
    s = str(abbr).strip().upper()
    return NFL_TEAM_ABBR_ALIASES.get(s, s)

def load_historical_games():
    """Load historical NFL games with scores."""
    hist_path = PROCESSED_DIR / "nfl" / "nfl_games_with_scores.parquet"
    print(f"Loading historical NFL games from {hist_path}...")

    if not hist_path.exists():
        raise FileNotFoundError(f"Historical games file not found: {hist_path}")

    df = pd.read_parquet(hist_path)

    # Filter to games with scores only
    scored = df[df['home_score'].notna()].copy()
    print(f"Loaded {len(scored)} games with scores (from {len(df)} total)")
    print(f"Date range: {scored['date'].min()} to {scored['date'].max()}")

    return scored

def load_future_games():
    """Load future NFL games schedule."""
    future_path = PROCESSED_DIR / "nfl" / "nfl_future_games.parquet"
    print(f"\nLoading future NFL games from {future_path}...")

    if not future_path.exists():
        raise FileNotFoundError(f"Future games file not found: {future_path}")

    df = pd.read_parquet(future_path)
    print(f"Loaded {len(df)} future games")
    print(f"Date range: {df['date'].min()} to {df['date'].max()}")

    return df

def compute_rolling_features(historical_df, as_of_date=None):
    """
    Compute rolling features for each team based on historical games.

    Returns dict mapping team_abbr -> feature dict
    """
    print("\nComputing rolling features from historical games...")

    if as_of_date:
        historical_df = historical_df[historical_df['date'] < as_of_date].copy()

    # Sort by date
    historical_df = historical_df.sort_values('date').copy()

    # Compute home win
    historical_df['home_win'] = (historical_df['home_score'] > historical_df['away_score']).astype(int)

    team_features = {}

    # Get all unique teams
    all_teams = set(historical_df['home_team'].unique()) | set(historical_df['away_team'].unique())

    for team in all_teams:
        if pd.isna(team):
            continue

        # Get all games for this team (as home or away)
        team_games = historical_df[
            (historical_df['home_team'] == team) |
            (historical_df['away_team'] == team)
        ].copy()

        if len(team_games) == 0:
            continue

        # Compute points for (PF) and points against (PA) for each game
        pf = []
        pa = []
        wins = []

        for _, game in team_games.iterrows():
            if game['home_team'] == team:
                pf.append(game['home_score'])
                pa.append(game['away_score'])
                wins.append(1 if game['home_score'] > game['away_score'] else 0)
            else:  # away team
                pf.append(game['away_score'])
                pa.append(game['home_score'])
                wins.append(1 if game['away_score'] > game['home_score'] else 0)

        # Convert to series for rolling calculations
        pf_series = pd.Series(pf)
        pa_series = pd.Series(pa)
        wins_series = pd.Series(wins)

        # Compute rolling stats (3-game and 5-game windows)
        # Use the most recent values
        if len(pf_series) >= 3:
            pf_roll_3 = pf_series.rolling(window=3, min_periods=1).mean().iloc[-1]
            pa_roll_3 = pa_series.rolling(window=3, min_periods=1).mean().iloc[-1]
            win_rate_3 = wins_series.rolling(window=3, min_periods=1).mean().iloc[-1]
        else:
            pf_roll_3 = pf_series.mean() if len(pf_series) > 0 else 20.0
            pa_roll_3 = pa_series.mean() if len(pa_series) > 0 else 20.0
            win_rate_3 = wins_series.mean() if len(wins_series) > 0 else 0.5

        if len(pf_series) >= 5:
            pf_roll_5 = pf_series.rolling(window=5, min_periods=1).mean().iloc[-1]
            pa_roll_5 = pa_series.rolling(window=5, min_periods=1).mean().iloc[-1]
            win_rate_5 = wins_series.rolling(window=5, min_periods=1).mean().iloc[-1]
        else:
            pf_roll_5 = pf_series.mean() if len(pf_series) > 0 else 20.0
            pa_roll_5 = pa_series.mean() if len(pa_series) > 0 else 20.0
            win_rate_5 = wins_series.mean() if len(wins_series) > 0 else 0.5

        # Compute rest days (days since last game)
        if len(team_games) > 0:
            last_game_date = pd.to_datetime(team_games.iloc[-1]['date'])
            # For future predictions, use a default of 7 days
            rest_days = 7
        else:
            rest_days = 7

        team_features[team] = {
            'pf_roll_3': pf_roll_3,
            'pa_roll_3': pa_roll_3,
            'win_rate_3': win_rate_3,
            'pf_roll_5': pf_roll_5,
            'pa_roll_5': pa_roll_5,
            'win_rate_5': win_rate_5,
            'rest_days': rest_days,
        }

    print(f"Computed features for {len(team_features)} teams")
    return team_features

def build_prediction_features(future_df, team_features):
    """
    Build model input features for upcoming games.
    """
    print("\nBuilding prediction features...")

    # Use team_id columns (which are actually abbreviations)
    future_df = future_df.copy()

    # Rename for consistency
    if 'home_team_id' in future_df.columns and 'home_team' not in future_df.columns:
        future_df['home_team'] = future_df['home_team_id']
    if 'away_team_id' in future_df.columns and 'away_team' not in future_df.columns:
        future_df['away_team'] = future_df['away_team_id']

    future_df['home_team'] = future_df['home_team'].apply(canonical_abbr)
    future_df['away_team'] = future_df['away_team'].apply(canonical_abbr)

    # Attach team features
    feature_cols = []

    for prefix, team_col in [('home', 'home_team'), ('away', 'away_team')]:
        for stat in ['pf_roll_3', 'pa_roll_3', 'win_rate_3', 'pf_roll_5', 'pa_roll_5', 'win_rate_5', 'rest_days']:
            col_name = f'{prefix}_{stat}'
            feature_cols.append(col_name)

            future_df[col_name] = future_df[team_col].map(
                lambda t: team_features.get(t, {}).get(stat, 20.0 if 'pf' in stat or 'pa' in stat else (0.5 if 'rate' in stat else 7))
            )

    # Compute differential features (home - away)
    for stat in ['pf_roll_3', 'pa_roll_3', 'win_rate_3', 'pf_roll_5', 'pa_roll_5', 'win_rate_5', 'rest_days']:
        diff_col = f'diff_{stat}'
        feature_cols.append(diff_col)
        future_df[diff_col] = future_df[f'home_{stat}'] - future_df[f'away_{stat}']

    print(f"Built features for {len(future_df)} games")
    print(f"Feature columns: {feature_cols}")

    return future_df, feature_cols

def load_nfl_model():
    """Load trained NFL model, scaler, and features."""
    model_dir = ARTIFACTS_DIR / "nfl"

    model_path = model_dir / "baseline_model.pkl"
    scaler_path = model_dir / "baseline_scaler.pkl"
    features_path = model_dir / "baseline_features.pkl"

    print(f"\nLoading NFL model from {model_dir}...")

    if not model_path.exists():
        raise FileNotFoundError(f"Model file not found: {model_path}")
    if not scaler_path.exists():
        raise FileNotFoundError(f"Scaler file not found: {scaler_path}")
    if not features_path.exists():
        raise FileNotFoundError(f"Features file not found: {features_path}")

    with open(model_path, 'rb') as f:
        model = pickle.load(f)

    with open(scaler_path, 'rb') as f:
        scaler = pickle.load(f)

    with open(features_path, 'rb') as f:
        features = pickle.load(f)

    print(f"Loaded model with {len(features)} features: {features}")

    return {
        'model': model,
        'scaler': scaler,
        'features': features
    }

def generate_predictions(games_df, feature_cols):
    """Generate predictions for games using trained model."""
    print("\nGenerating predictions...")

    artifact = load_nfl_model()
    model = artifact['model']
    scaler = artifact['scaler']
    expected_features = artifact['features']

    # Check all features exist
    missing = [f for f in expected_features if f not in games_df.columns]
    if missing:
        print(f"ERROR: Missing features: {missing}")
        print(f"Available columns: {games_df.columns.tolist()}")
        raise ValueError(f"Missing required features: {missing}")

    # Extract features in correct order
    X = games_df[expected_features].values

    # Scale features
    X_scaled = scaler.transform(X)

    # Generate predictions
    probs = model.predict_proba(X_scaled)[:, 1]  # Probability of home win

    games_df['p_home'] = probs
    games_df['p_away'] = 1 - probs

    print(f"Generated {len(games_df)} predictions")
    print(f"Sample predictions:\n{games_df[['date', 'home_team', 'away_team', 'p_home', 'p_away']].head()}")

    return games_df

def save_predictions(predictions_df):
    """Save predictions to parquet file."""
    out_path = PROCESSED_DIR / "nfl" / "nfl_predictions_future.parquet"

    # Create output dataframe with standardized columns
    output_df = predictions_df.copy()

    # Convert date to datetime then to string format (YYYY-MM-DD)
    output_df['game_date'] = pd.to_datetime(output_df['date']).dt.strftime('%Y-%m-%d')

    # Create game_id from date and teams: YYYY_MM_DD_HOME_AWAY
    output_df['game_id'] = (
        pd.to_datetime(output_df['date']).dt.strftime('%Y_%m_%d') + '_' +
        output_df['home_team'].astype(str) + '_' +
        output_df['away_team'].astype(str)
    )

    # Rename probability columns to match historical format
    output_df['p_home_win'] = output_df['p_home']
    output_df['source'] = "nfl_future"

    # Select columns to match historical predictions format
    final_cols = ['game_id', 'game_date', 'home_team', 'away_team', 'p_home_win', 'source']
    output_df = output_df[final_cols].copy()

    print(f"\nSaving predictions to {out_path}...")
    print(f"Output columns: {output_df.columns.tolist()}")
    print(f"Sample:\n{output_df.head(3)}")
    output_df.to_parquet(out_path, index=False)
    print(f"✓ Saved {len(output_df)} predictions")

    return out_path

def main():
    """Main execution."""
    print("=" * 70)
    print("NFL Future Game Predictions Generator")
    print("=" * 70)
    print()

    # Load data
    historical = load_historical_games()
    future = load_future_games()

    # Compute team features from historical data
    team_features = compute_rolling_features(historical)

    if not team_features:
        print("ERROR: No team features could be computed")
        sys.exit(1)

    # Build features for upcoming games
    prediction_input, feature_cols = build_prediction_features(future, team_features)

    if len(prediction_input) == 0:
        print("No games to predict")
        sys.exit(0)

    # Generate predictions
    predictions = generate_predictions(prediction_input, feature_cols)

    # Save predictions
    output_path = save_predictions(predictions)

    # Summary
    print()
    print("=" * 70)
    print("SUMMARY")
    print("=" * 70)
    print(f"Predictions generated: {len(predictions)}")
    print(f"Output file: {output_path}")
    print(f"Date range: {predictions['date'].min()} to {predictions['date'].max()}")
    print()
    print("✓ Prediction generation complete")
    print()
    print("Next steps:")
    print("1. Update model_api/main.py to load nfl_predictions_future.parquet")
    print("2. Restart the API server")

if __name__ == "__main__":
    main()
