"""
Train baseline NFL prediction model using logistic regression.

Uses time-based split to avoid data leakage and evaluates model performance
with accuracy, log loss, and calibration metrics.
"""

import pandas as pd
import numpy as np
from pathlib import Path
import pickle
from sklearn.linear_model import LogisticRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import accuracy_score, log_loss
import warnings

warnings.filterwarnings('ignore')


def load_features(path: str) -> pd.DataFrame:
    """Load feature table."""
    print(f"Loading features from {path}...")
    df = pd.read_parquet(path)
    print(f"  Loaded {len(df)} games")
    print(f"  Columns: {len(df.columns)}")
    return df


def prepare_data(df: pd.DataFrame, test_season: int = 2024):
    """
    Prepare train/test split and feature matrices.

    Args:
        df: Feature dataframe
        test_season: Season to use as test set (>= this season)

    Returns:
        X_train, X_test, y_train, y_test, train_df, test_df, feature_names
    """
    print("\nPreparing data...")

    # Filter to games with valid target
    df = df[df['home_win'].notna()].copy()
    print(f"  Games with valid target: {len(df)}")
    print(f"  Season range: {df['season'].min()}-{df['season'].max()}")

    # Time-based split
    train_mask = df['season'] < test_season
    test_mask = df['season'] >= test_season

    train_df = df[train_mask].copy()
    test_df = df[test_mask].copy()

    print(f"\n  Train set: {len(train_df)} games (seasons < {test_season})")
    print(f"  Test set:  {len(test_df)} games (seasons >= {test_season})")

    if len(test_df) == 0:
        print(f"\n  WARNING: No test data for season >= {test_season}")
        print(f"  Using previous season as test...")
        test_season = df['season'].max()
        train_mask = df['season'] < test_season
        test_mask = df['season'] >= test_season
        train_df = df[train_mask].copy()
        test_df = df[test_mask].copy()
        print(f"  New split - Train: {len(train_df)}, Test: {len(test_df)}")

    # Select feature columns (rolling stats and differentials)
    feature_cols = [
        'home_pf_roll_3', 'home_pa_roll_3', 'home_win_rate_3',
        'home_pf_roll_5', 'home_pa_roll_5', 'home_win_rate_5',
        'home_rest_days',
        'away_pf_roll_3', 'away_pa_roll_3', 'away_win_rate_3',
        'away_pf_roll_5', 'away_pa_roll_5', 'away_win_rate_5',
        'away_rest_days',
        'diff_pf_roll_5', 'diff_pa_roll_5', 'diff_win_rate_5',
        'diff_pf_roll_3', 'diff_pa_roll_3', 'diff_win_rate_3',
        'diff_rest_days'
    ]

    # Check for missing features
    missing_cols = [col for col in feature_cols if col not in df.columns]
    if missing_cols:
        print(f"\n  WARNING: Missing feature columns: {missing_cols}")
        feature_cols = [col for col in feature_cols if col in df.columns]

    print(f"\n  Using {len(feature_cols)} features:")
    for col in feature_cols:
        print(f"    - {col}")

    # Extract features and target
    X_train = train_df[feature_cols].values
    X_test = test_df[feature_cols].values
    y_train = train_df['home_win'].values
    y_test = test_df['home_win'].values

    # Check for missing values
    train_missing = np.isnan(X_train).sum()
    test_missing = np.isnan(X_test).sum()

    if train_missing > 0 or test_missing > 0:
        print(f"\n  WARNING: Missing values detected")
        print(f"    Train: {train_missing} missing values")
        print(f"    Test: {test_missing} missing values")
        print(f"  Filling missing values with 0...")
        X_train = np.nan_to_num(X_train, 0)
        X_test = np.nan_to_num(X_test, 0)

    return X_train, X_test, y_train, y_test, train_df, test_df, feature_cols


def train_model(X_train, y_train):
    """
    Train logistic regression model with standardization.

    Returns:
        model: Trained sklearn model
        scaler: Fitted StandardScaler
    """
    print("\nTraining baseline model...")

    # Standardize features
    scaler = StandardScaler()
    X_train_scaled = scaler.fit_transform(X_train)

    # Train logistic regression
    model = LogisticRegression(
        random_state=42,
        max_iter=1000,
        solver='lbfgs'
    )
    model.fit(X_train_scaled, y_train)

    print(f"  Model trained successfully")
    print(f"  Intercept: {model.intercept_[0]:.4f}")

    return model, scaler


def evaluate_model(model, scaler, X_train, y_train, X_test, y_test, split_name="Test"):
    """
    Evaluate model performance.

    Prints accuracy, log loss, and calibration metrics.
    Returns predictions and probabilities.
    """
    print(f"\n{'='*60}")
    print(f"EVALUATION: {split_name} Set")
    print('='*60)

    # Scale features
    X_train_scaled = scaler.transform(X_train)
    X_test_scaled = scaler.transform(X_test)

    # Predictions
    y_pred_train = model.predict(X_train_scaled)
    y_pred_test = model.predict(X_test_scaled)

    # Probabilities
    y_prob_train = model.predict_proba(X_train_scaled)[:, 1]
    y_prob_test = model.predict_proba(X_test_scaled)[:, 1]

    # Accuracy
    train_acc = accuracy_score(y_train, y_pred_train)
    test_acc = accuracy_score(y_test, y_pred_test)

    print(f"\nAccuracy:")
    print(f"  Train: {train_acc:.4f} ({train_acc*100:.2f}%)")
    print(f"  {split_name}: {test_acc:.4f} ({test_acc*100:.2f}%)")

    # Log Loss
    train_logloss = log_loss(y_train, y_prob_train)
    test_logloss = log_loss(y_test, y_prob_test)

    print(f"\nLog Loss:")
    print(f"  Train: {train_logloss:.4f}")
    print(f"  {split_name}: {test_logloss:.4f}")

    # Calibration curve (binned)
    print(f"\nCalibration (10 bins):")
    print(f"  {'Bin':>3} | {'Pred Prob':>9} | {'Actual':>9} | {'Count':>6} | {'Diff':>9}")
    print(f"  {'-'*3}-+-{'-'*9}-+-{'-'*9}-+-{'-'*6}-+-{'-'*9}")

    # Create 10 bins based on predicted probabilities
    bins = np.linspace(0, 1, 11)
    bin_indices = np.digitize(y_prob_test, bins) - 1
    bin_indices = np.clip(bin_indices, 0, 9)  # Ensure we have exactly 10 bins

    for i in range(10):
        mask = bin_indices == i
        if mask.sum() > 0:
            avg_pred = y_prob_test[mask].mean()
            avg_actual = y_test[mask].mean()
            count = mask.sum()
            diff = avg_pred - avg_actual
            print(f"  {i+1:3d} | {avg_pred:9.4f} | {avg_actual:9.4f} | {count:6d} | {diff:+9.4f}")
        else:
            print(f"  {i+1:3d} | {'---':>9} | {'---':>9} | {'0':>6} | {'---':>9}")

    # Overall calibration
    overall_diff = y_prob_test.mean() - y_test.mean()
    print(f"\n  Overall: Pred={y_prob_test.mean():.4f}, Actual={y_test.mean():.4f}, Diff={overall_diff:+.4f}")

    return y_prob_test


def save_artifacts(model, scaler, feature_names, output_dir: Path):
    """Save model artifacts."""
    print(f"\nSaving model artifacts to {output_dir}...")

    output_dir.mkdir(parents=True, exist_ok=True)

    # Save model
    model_path = output_dir / "baseline_model.pkl"
    with open(model_path, 'wb') as f:
        pickle.dump(model, f)
    print(f"  Saved model: {model_path}")

    # Save scaler
    scaler_path = output_dir / "baseline_scaler.pkl"
    with open(scaler_path, 'wb') as f:
        pickle.dump(scaler, f)
    print(f"  Saved scaler: {scaler_path}")

    # Save feature names
    features_path = output_dir / "baseline_features.pkl"
    with open(features_path, 'wb') as f:
        pickle.dump(feature_names, f)
    print(f"  Saved features: {features_path}")


def save_predictions(train_df, test_df, y_prob_train, y_prob_test, output_path: Path):
    """Save predictions to parquet."""
    print(f"\nSaving predictions to {output_path}...")

    # Prepare train predictions
    train_preds = pd.DataFrame({
        'game_id': train_df['game_id'].values,
        'game_date': train_df['game_date'].values,
        'season': train_df['season'].values,
        'week': train_df['week'].values,
        'home_team': train_df['home_team'].values,
        'away_team': train_df['away_team'].values,
        'p_home_win': y_prob_train,
        'actual_home_win': train_df['home_win'].values,
        'split': 'train'
    })

    # Prepare test predictions
    test_preds = pd.DataFrame({
        'game_id': test_df['game_id'].values,
        'game_date': test_df['game_date'].values,
        'season': test_df['season'].values,
        'week': test_df['week'].values,
        'home_team': test_df['home_team'].values,
        'away_team': test_df['away_team'].values,
        'p_home_win': y_prob_test,
        'actual_home_win': test_df['home_win'].values,
        'split': 'test'
    })

    # Combine
    all_preds = pd.concat([train_preds, test_preds], ignore_index=True)

    # Save
    all_preds.to_parquet(output_path, index=False)
    print(f"  Saved {len(all_preds)} predictions ({len(train_preds)} train, {len(test_preds)} test)")


def main():
    """Main execution function."""
    print("="*80)
    print("NFL BASELINE MODEL TRAINING")
    print("="*80)

    # Paths
    features_path = Path("model/data/processed/nfl/nfl_model_features.parquet")
    artifacts_dir = Path("model/artifacts/nfl")
    predictions_path = artifacts_dir / "nfl_predictions.parquet"

    # Load features
    df = load_features(features_path)

    # Prepare data (train/test split)
    X_train, X_test, y_train, y_test, train_df, test_df, feature_names = prepare_data(df)

    # Train model
    model, scaler = train_model(X_train, y_train)

    # Generate predictions for evaluation
    X_train_scaled = scaler.transform(X_train)
    y_prob_train = model.predict_proba(X_train_scaled)[:, 1]

    # Evaluate
    y_prob_test = evaluate_model(model, scaler, X_train, y_train, X_test, y_test)

    # Save artifacts
    save_artifacts(model, scaler, feature_names, artifacts_dir)

    # Save predictions
    save_predictions(train_df, test_df, y_prob_train, y_prob_test, predictions_path)

    print("\n" + "="*80)
    print("TRAINING COMPLETE")
    print("="*80)
    print(f"Model artifacts: {artifacts_dir}")
    print(f"Predictions: {predictions_path}")


if __name__ == "__main__":
    main()
