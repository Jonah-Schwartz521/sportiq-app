#!/usr/bin/env python3
"""Quick test to verify parquet loading works"""
import sys
from pathlib import Path

# Add model to path
sys.path.insert(0, str(Path(__file__).parent / "model"))

from model_api.main import load_games_table

try:
    games = load_games_table()
    print(f"✓ Successfully loaded games table with {len(games)} rows")
    print(f"✓ Sports included: {games['sport'].unique().tolist()}")
    sys.exit(0)
except Exception as e:
    print(f"✗ Failed to load games table: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)
