import pandas as pd
from pathlib import Path

# Base dir: /Volumes/easystore/Projects/sportiq-app/model
BASE_DIR = Path(__file__).resolve().parents[1]

# Existing combined games file your pipeline already uses
BASE_GAMES_PATH = BASE_DIR / "data" / "processed" / "processed_games_with_scores.parquet"

# New NHL file we just built
NHL_GAMES_PATH = BASE_DIR / "data" / "processed" / "nhl_games_with_scores.parquet"


def append_nhl_to_base() -> None:
    print(f"Loading base games from: {BASE_GAMES_PATH}")
    base = pd.read_parquet(BASE_GAMES_PATH)
    print("Base shape:", base.shape)
    print("Base columns:", list(base.columns))

    # Ensure base has a 'league' column
    if "league" not in base.columns:
        if "season_label" in base.columns:
            base = base.copy()
            base["league"] = base["season_label"].astype(str).str.split("_").str[-1]
            print("Added 'league' column to base from 'season_label'")
        else:
            base = base.copy()
            base["league"] = "NBA"  # Fallback default if we cannot infer
            print("Added 'league' column to base with default 'NBA'")

    print("Base leagues:", base["league"].value_counts(dropna=False))

    print(f"\nLoading NHL games from: {NHL_GAMES_PATH}")
    nhl = pd.read_parquet(NHL_GAMES_PATH)
    print("NHL shape:", nhl.shape)
    print("NHL leagues:", nhl["league"].value_counts(dropna=False))

    # Ensure NHL also has a 'league' column, just in case
    if "league" not in nhl.columns:
        nhl = nhl.copy()
        nhl["league"] = "NHL"

    # Decide which keys to use for de-duplication
    if "league" in base.columns and "league" in nhl.columns:
        dedupe_keys = ["league", "game_id"]
    else:
        dedupe_keys = ["game_id"]

    # Concatenate and drop any accidental duplicates on the chosen keys
    combined = (
        pd.concat([base, nhl], ignore_index=True)
        .drop_duplicates(subset=dedupe_keys)
        .reset_index(drop=True)
    )

    # Normalize dtypes for parquet (avoid mixed-type object columns)
    combined = combined.copy()

    if "season" in combined.columns:
        combined["season"] = combined["season"].astype(str)

    # Make sure league is a plain string
    combined["league"] = combined["league"].astype(str)

    print("\nCombined shape:", combined.shape)
    print("Combined leagues:", combined["league"].value_counts(dropna=False))
    print("\nCombined dtypes:")
    print(combined.dtypes)

    # Overwrite the existing base file using pyarrow (more robust with strings)
    combined.to_parquet(BASE_GAMES_PATH, index=False, engine="pyarrow")
    print(f"\nSaved updated games base to: {BASE_GAMES_PATH}")
    print("Done ✅")


if __name__ == "__main__":
    append_nhl_to_base()