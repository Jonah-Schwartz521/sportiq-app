#!/usr/bin/env python3
"""
Generate predictions for upcoming NBA games.

This script:
1. Loads upcoming NBA schedule from the games database
2. Computes required features based on recent team performance
3. Generates win probability predictions
4. Saves predictions to parquet for API consumption
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

# Add project root to path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from src.paths import PROCESSED_DIR, MODEL_DIR
from src.nba_inference import load_nba_model

def load_schedule():
    """Load full NBA schedule including future games."""
    schedule_path = PROCESSED_DIR / "games_with_scores_and_future.parquet"
    print(f"Loading schedule from {schedule_path}...")

    if not schedule_path.exists():
        raise FileNotFoundError(f"Schedule file not found: {schedule_path}")

    df = pd.read_parquet(schedule_path)

    # Filter to NBA games only
    nba = df[df['sport'] == 'NBA'].copy()
    print(f"Loaded {len(nba)} NBA games")

    return nba

def load_historical_results():
    """Load historical NBA results with scores for feature computation."""
    results_path = PROCESSED_DIR / "nba" / "nba_games_with_scores.parquet"
    print(f"Loading historical results from {results_path}...")

    if not results_path.exists():
        print(f"WARNING: Historical results not found at {results_path}")
        print("Using schedule data only - features will be limited")
        return load_schedule()

    df = pd.read_parquet(results_path)
    print(f"Loaded {len(df)} historical games")

    return df

def compute_team_features(historical_df, as_of_date=None):
    """
    Compute team-level features from historical games.

    Returns dict mapping team_id -> feature dict
    """
    # Determine date column name
    date_col = 'date_utc' if 'date_utc' in historical_df.columns else 'date'

    if as_of_date:
        # Only use games before the prediction date
        historical_df = historical_df[historical_df[date_col] < as_of_date].copy()

    print(f"Computing features from {len(historical_df)} historical games...")

    # Ensure we have required columns
    if 'home_score' not in historical_df.columns or 'away_score' not in historical_df.columns:
        print("WARNING: No score data available - using default features")
        return {}

    # Remove games without scores
    scored = historical_df.dropna(subset=['home_score', 'away_score']).copy()
    print(f"Using {len(scored)} games with scores for feature computation")

    if len(scored) == 0:
        return {}

    # Compute win and point differential
    scored['home_win'] = (scored['home_score'] > scored['away_score']).astype(int)
    scored['home_pd'] = scored['home_score'] - scored['away_score']
    scored['away_pd'] = scored['away_score'] - scored['home_score']

    # Sort by date
    scored = scored.sort_values(date_col)

    # Compute features per team
    team_features = {}

    for team_id_col, team_name_col in [('home_team_id', 'home_team_name'), ('away_team_id', 'away_team_name')]:
        is_home = (team_id_col == 'home_team_id')

        for team_id in scored[team_id_col].unique():
            if pd.isna(team_id):
                continue

            team_id = int(team_id)

            # Get all games for this team (as home or away)
            team_games = scored[
                (scored['home_team_id'] == team_id) |
                (scored['away_team_id'] == team_id)
            ].copy()

            if len(team_games) == 0:
                continue

            # Most recent games
            recent_10 = team_games.tail(10)
            recent_20 = team_games.tail(20)

            # Compute win percentage and point differential
            def get_team_stats(games_df, tid):
                home_games = games_df[games_df['home_team_id'] == tid]
                away_games = games_df[games_df['away_team_id'] == tid]

                home_wins = home_games['home_win'].sum() if len(home_games) > 0 else 0
                away_wins = len(away_games) - away_games['home_win'].sum() if len(away_games) > 0 else 0
                total_wins = home_wins + away_wins
                total_games = len(home_games) + len(away_games)
                win_pct = total_wins / total_games if total_games > 0 else 0.5

                home_pd = home_games['home_pd'].mean() if len(home_games) > 0 else 0
                away_pd = away_games['away_pd'].mean() if len(away_games) > 0 else 0
                avg_pd = (home_pd * len(home_games) + away_pd * len(away_games)) / total_games if total_games > 0 else 0

                return win_pct, avg_pd

            # Win pct and PD over last 10
            win_pct_10, avg_pd_10 = get_team_stats(recent_10, team_id)

            # Win pct over last 20
            win_pct_20, _ = get_team_stats(recent_20, team_id)

            # Season stats
            season_win_pct, _ = get_team_stats(team_games, team_id)

            # Last game stats
            last_game = team_games.iloc[-1] if len(team_games) > 0 else None
            if last_game is not None:
                last_pd = last_game['home_pd'] if last_game['home_team_id'] == team_id else last_game['away_pd']
            else:
                last_pd = 0

            team_features[team_id] = {
                'win_pct_10': win_pct_10,
                'avg_pd_10': avg_pd_10,
                'season_win_pct': season_win_pct,
                'recent_win_pct_20g': win_pct_20,
                'last_pd': last_pd,
            }

    print(f"Computed features for {len(team_features)} teams")
    return team_features

def build_prediction_features(schedule_df, team_features):
    """
    Build model input features for upcoming games.
    """
    print("Building prediction features...")

    # Filter to upcoming games (no scores yet)
    upcoming = schedule_df[schedule_df['home_score'].isna()].copy()
    print(f"Found {len(upcoming)} upcoming games")

    if len(upcoming) == 0:
        print("No upcoming games to predict")
        return pd.DataFrame()

    # Compute days rest and B2B (simplified - would need full schedule logic)
    # For now, use defaults
    upcoming['home_days_rest'] = 1
    upcoming['away_days_rest'] = 1
    upcoming['home_b2b'] = 0
    upcoming['away_b2b'] = 0

    # Attach team features
    for prefix, id_col in [('home', 'home_team_id'), ('away', 'away_team_id')]:
        for suffix in ['win_pct_10', 'avg_pd_10', 'season_win_pct', 'recent_win_pct_20g', 'last_pd']:
            upcoming[f'{prefix}_{suffix}'] = upcoming[id_col].map(
                lambda tid: team_features.get(int(tid), {}).get(suffix, 0.5 if 'pct' in suffix else 0)
                if not pd.isna(tid) else (0.5 if 'pct' in suffix else 0)
            )

    return upcoming

def generate_predictions(games_df):
    """Generate predictions for games using trained model."""
    print("Generating predictions...")

    artifact = load_nba_model()
    model = artifact['model']
    features = artifact['features']

    # Check all features exist
    missing = [f for f in features if f not in games_df.columns]
    if missing:
        print(f"WARNING: Missing features: {missing}")
        print("Available columns:", games_df.columns.tolist())
        return games_df

    # Generate predictions
    X = games_df[features]
    probs = model.predict_proba(X)[:, 1]  # Probability of home win

    games_df['p_home'] = probs
    games_df['p_away'] = 1 - probs

    print(f"Generated {len(games_df)} predictions")
    return games_df

def save_predictions(predictions_df):
    """Save predictions to parquet file."""
    out_path = PROCESSED_DIR / "nba_predictions_future.parquet"

    # Keep only essential columns
    essential_cols = ['date', 'p_home', 'p_away']

    # Try to get team names from various possible column names
    home_team_col = None
    away_team_col = None

    for col in ['home_team', 'home_team_name', 'home']:
        if col in predictions_df.columns:
            home_team_col = col
            break

    for col in ['away_team', 'away_team_name', 'away', 'visitor_team', 'visitor_team_name']:
        if col in predictions_df.columns:
            away_team_col = col
            break

    if not home_team_col or not away_team_col:
        print(f"WARNING: Could not find team name columns")
        print(f"Available columns: {predictions_df.columns.tolist()}")
        # Use the first available or create empty
        if 'home_team_id' in predictions_df.columns:
            predictions_df['home_team'] = predictions_df['home_team_id'].astype(str)
        if 'away_team_id' in predictions_df.columns:
            predictions_df['away_team'] = predictions_df['away_team_id'].astype(str)
    else:
        # Standardize column names
        predictions_df['home_team'] = predictions_df[home_team_col]
        predictions_df['away_team'] = predictions_df[away_team_col]

    essential_cols.extend(['home_team', 'away_team'])

    # Use available columns
    available = [c for c in essential_cols if c in predictions_df.columns]
    output_df = predictions_df[available].copy()

    print(f"Saving predictions to {out_path}...")
    print(f"Output columns: {output_df.columns.tolist()}")
    print(f"Sample: {output_df.head(2).to_dict('records')}")
    output_df.to_parquet(out_path, index=False)
    print(f"✓ Saved {len(output_df)} predictions")

    return out_path

def main():
    """Main execution."""
    print("=" * 70)
    print("NBA Future Game Predictions Generator")
    print("=" * 70)
    print()

    # Load data
    schedule = load_schedule()
    historical = load_historical_results()

    # Compute team features from historical data
    team_features = compute_team_features(historical)

    if not team_features:
        print("ERROR: No team features could be computed")
        print("Please ensure historical NBA data with scores is available")
        sys.exit(1)

    # Build features for upcoming games
    prediction_input = build_prediction_features(schedule, team_features)

    if len(prediction_input) == 0:
        print("No games to predict")
        sys.exit(0)

    # Generate predictions
    predictions = generate_predictions(prediction_input)

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
    print("To use these predictions in the API:")
    print("1. Update model_api/main.py to load nba_predictions_future.parquet")
    print("2. Restart the API server")

if __name__ == "__main__":
    main()
