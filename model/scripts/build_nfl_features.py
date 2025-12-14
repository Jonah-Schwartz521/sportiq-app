"""
Build NFL modeling features with zero data leakage.

Creates rolling statistics and differential features for NFL game prediction.
All rolling features use .shift(1) to ensure the current game is never included.
"""

import pandas as pd
import numpy as np
from pathlib import Path


def load_games(path: str) -> pd.DataFrame:
    """Load NFL model games parquet."""
    print(f"Loading games from {path}...")
    df = pd.read_parquet(path)
    print(f"  Loaded {len(df)} games from seasons {df['season'].min()}-{df['season'].max()}")
    print(f"  Future games: {df['is_future'].sum()}")
    print(f"  Historical games with scores: {(~df['is_future']).sum()}")
    return df


def create_team_game_table(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create a team-game table with one row per team per game.

    This "unpivots" the game table so each team's performance is a separate row,
    making it easy to compute rolling statistics per team.
    """
    print("\nCreating team-game table...")

    # Ensure game_date is datetime
    if df['game_date'].dtype == 'object':
        df['game_date'] = pd.to_datetime(df['game_date'])
    elif df['date'].dtype != 'object':
        # Use the datetime 'date' column if game_date is not datetime
        df['game_date'] = df['date']

    # Home team rows
    home = pd.DataFrame({
        'game_id': df['game_id'],
        'season': df['season'],
        'week': df['week'],
        'game_date': df['game_date'],
        'team': df['home_team'],
        'opponent': df['away_team'],
        'is_home': True,
        'points_for': df['home_score'],
        'points_against': df['away_score'],
        'win': df['home_win'],
        'is_future': df['is_future']
    })

    # Away team rows
    away = pd.DataFrame({
        'game_id': df['game_id'],
        'season': df['season'],
        'week': df['week'],
        'game_date': df['game_date'],
        'team': df['away_team'],
        'opponent': df['home_team'],
        'is_home': False,
        'points_for': df['away_score'],
        'points_against': df['home_score'],
        'win': df['home_win'].apply(lambda x: 1 - x if pd.notna(x) else np.nan),
        'is_future': df['is_future']
    })

    # Combine and sort by team then game_date
    team_games = pd.concat([home, away], ignore_index=True)
    team_games = team_games.sort_values(['team', 'game_date']).reset_index(drop=True)

    print(f"  Created {len(team_games)} team-game rows ({len(team_games) / 2:.0f} games * 2 teams)")
    print(f"  Teams: {team_games['team'].nunique()}")

    return team_games


def compute_rolling_features(team_games: pd.DataFrame) -> pd.DataFrame:
    """
    Compute rolling features per team using .shift(1) to avoid data leakage.

    Rolling features are computed over the previous N games (not including current game).
    """
    print("\nComputing rolling features...")

    # Group by team to compute rolling stats
    grouped = team_games.groupby('team', group_keys=False)

    # Shift(1) ensures the current game is not included in the rolling window
    # Rolling 3-game averages
    team_games['pf_roll_3'] = grouped['points_for'].apply(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    team_games['pa_roll_3'] = grouped['points_against'].apply(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )
    team_games['win_rate_3'] = grouped['win'].apply(
        lambda x: x.shift(1).rolling(window=3, min_periods=1).mean()
    )

    # Rolling 5-game averages
    team_games['pf_roll_5'] = grouped['points_for'].apply(
        lambda x: x.shift(1).rolling(window=5, min_periods=1).mean()
    )
    team_games['pa_roll_5'] = grouped['points_against'].apply(
        lambda x: x.shift(1).rolling(window=5, min_periods=1).mean()
    )
    team_games['win_rate_5'] = grouped['win'].apply(
        lambda x: x.shift(1).rolling(window=5, min_periods=1).mean()
    )

    # Rest days: difference in days from previous game
    team_games['prev_game_date'] = grouped['game_date'].shift(1)
    team_games['rest_days'] = (team_games['game_date'] - team_games['prev_game_date']).dt.days

    # For first game of each team, rest_days will be NaN - fill with a reasonable default (e.g., 7)
    team_games['rest_days'] = team_games['rest_days'].fillna(7)

    # Drop temporary column
    team_games = team_games.drop(columns=['prev_game_date'])

    # Sanity check: print missingness
    print("  Rolling feature missingness:")
    for col in ['pf_roll_3', 'pa_roll_3', 'win_rate_3', 'pf_roll_5', 'pa_roll_5', 'win_rate_5', 'rest_days']:
        null_count = team_games[col].isnull().sum()
        print(f"    {col}: {null_count} nulls ({100 * null_count / len(team_games):.1f}%)")

    return team_games


def build_modeling_table(team_games: pd.DataFrame, original_games: pd.DataFrame) -> pd.DataFrame:
    """
    Join features back to game-level data.

    Creates a modeling table with home_* and away_* features, plus differential features.
    """
    print("\nBuilding game-level modeling table...")

    # Select features to join
    feature_cols = ['game_id', 'team', 'is_home',
                    'pf_roll_3', 'pa_roll_3', 'win_rate_3',
                    'pf_roll_5', 'pa_roll_5', 'win_rate_5', 'rest_days']

    features = team_games[feature_cols].copy()

    # Split into home and away
    home_features = features[features['is_home'] == True].drop(columns=['is_home'])
    away_features = features[features['is_home'] == False].drop(columns=['is_home'])

    # Rename columns with home_ and away_ prefixes
    home_features = home_features.rename(columns={
        'team': 'home_team',
        'pf_roll_3': 'home_pf_roll_3',
        'pa_roll_3': 'home_pa_roll_3',
        'win_rate_3': 'home_win_rate_3',
        'pf_roll_5': 'home_pf_roll_5',
        'pa_roll_5': 'home_pa_roll_5',
        'win_rate_5': 'home_win_rate_5',
        'rest_days': 'home_rest_days'
    })

    away_features = away_features.rename(columns={
        'team': 'away_team',
        'pf_roll_3': 'away_pf_roll_3',
        'pa_roll_3': 'away_pa_roll_3',
        'win_rate_3': 'away_win_rate_3',
        'pf_roll_5': 'away_pf_roll_5',
        'pa_roll_5': 'away_pa_roll_5',
        'win_rate_5': 'away_win_rate_5',
        'rest_days': 'away_rest_days'
    })

    # Start with core game info
    modeling_df = original_games[['game_id', 'season', 'week', 'game_date',
                                   'home_team', 'away_team', 'home_score', 'away_score',
                                   'home_win', 'is_future']].copy()

    # Ensure game_date is datetime
    if modeling_df['game_date'].dtype == 'object':
        modeling_df['game_date'] = pd.to_datetime(modeling_df['game_date'])

    # Join home features
    modeling_df = modeling_df.merge(home_features, on=['game_id', 'home_team'], how='left')

    # Join away features
    modeling_df = modeling_df.merge(away_features, on=['game_id', 'away_team'], how='left')

    # Create differential features
    print("  Creating differential features...")
    modeling_df['diff_pf_roll_5'] = modeling_df['home_pf_roll_5'] - modeling_df['away_pf_roll_5']
    modeling_df['diff_pa_roll_5'] = modeling_df['home_pa_roll_5'] - modeling_df['away_pa_roll_5']
    modeling_df['diff_win_rate_5'] = modeling_df['home_win_rate_5'] - modeling_df['away_win_rate_5']
    modeling_df['diff_pf_roll_3'] = modeling_df['home_pf_roll_3'] - modeling_df['away_pf_roll_3']
    modeling_df['diff_pa_roll_3'] = modeling_df['home_pa_roll_3'] - modeling_df['away_pa_roll_3']
    modeling_df['diff_win_rate_3'] = modeling_df['home_win_rate_3'] - modeling_df['away_win_rate_3']
    modeling_df['diff_rest_days'] = modeling_df['home_rest_days'] - modeling_df['away_rest_days']

    print(f"  Final modeling table: {len(modeling_df)} games, {len(modeling_df.columns)} columns")
    print(f"  Target (home_win) missingness: {modeling_df['home_win'].isnull().sum()} nulls")

    return modeling_df


def main():
    """Main execution function."""
    print("="*80)
    print("NFL FEATURE ENGINEERING")
    print("="*80)

    # Paths
    input_path = Path("model/data/processed/nfl/nfl_model_games.parquet")
    output_path = Path("model/data/processed/nfl/nfl_model_features.parquet")

    # Ensure output directory exists
    output_path.parent.mkdir(parents=True, exist_ok=True)

    # Load games
    games_df = load_games(input_path)

    # Create team-game table
    team_games = create_team_game_table(games_df)

    # Compute rolling features
    team_games = compute_rolling_features(team_games)

    # Build modeling table
    modeling_df = build_modeling_table(team_games, games_df)

    # Save to parquet
    print(f"\nSaving features to {output_path}...")
    modeling_df.to_parquet(output_path, index=False)
    print(f"  Saved {len(modeling_df)} rows")

    # Final summary
    print("\n" + "="*80)
    print("FEATURE ENGINEERING COMPLETE")
    print("="*80)
    print(f"Output: {output_path}")
    print(f"Rows: {len(modeling_df)}")
    print(f"Columns: {len(modeling_df.columns)}")
    print(f"\nColumn list:")
    for i, col in enumerate(modeling_df.columns, 1):
        print(f"  {i:2d}. {col}")

    # Show sample of features
    print("\nSample of features (first historical game with complete data):")
    sample = modeling_df[~modeling_df['is_future'] & modeling_df['home_win'].notna()].head(1)
    if len(sample) > 0:
        for col in sample.columns:
            print(f"  {col}: {sample[col].iloc[0]}")


if __name__ == "__main__":
    main()
