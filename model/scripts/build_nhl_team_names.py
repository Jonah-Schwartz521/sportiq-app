import pandas as pd
from pathlib import Path


def main() -> None:
    """Attach human-readable NHL team names to the games parquet using nhl_team_lookup.csv.

    This script is intentionally **NHL-only** and does **not** touch NBA/MLB/NFL
    mappings or any database tables. It just updates the NHL parquet so your
    API can display proper team names instead of numeric ids.
    """

    # Resolve project and NHL data paths based on this file location
    project_root = Path(__file__).resolve().parents[2]
    nhl_root = project_root / "model" / "data" / "processed" / "nhl"

    games_path = nhl_root / "nhl_games_with_scores.parquet"
    lookup_path = nhl_root / "nhl_team_lookup.csv"

    print(f"Loading NHL games from {games_path}")
    if not games_path.exists():
        raise FileNotFoundError(f"NHL games parquet not found at {games_path}")

    games = pd.read_parquet(games_path)

    # Sanity check for id columns
    required_cols = {"home_team_id", "away_team_id"}
    missing = required_cols.difference(games.columns)
    if missing:
        raise RuntimeError(
            f"NHL games parquet is missing required columns: {sorted(missing)}"
        )

    print(f"Loading team lookup from {lookup_path}")
    if not lookup_path.exists():
        raise FileNotFoundError(
            f"nhl_team_lookup.csv not found at {lookup_path}. "
            "Make sure you've filled that file before running this script."
        )

    lookup_df = pd.read_csv(lookup_path)

    # Expect exactly team_id and team_name
    expected_lookup_cols = {"team_id", "team_name"}
    missing_lookup = expected_lookup_cols.difference(lookup_df.columns)
    if missing_lookup:
        raise RuntimeError(
            "nhl_team_lookup.csv must contain columns 'team_id' and 'team_name'. "
            f"Missing: {sorted(missing_lookup)}"
        )

    # Build mapping dict: id -> name
    id_to_name = dict(zip(lookup_df["team_id"], lookup_df["team_name"]))

    # Show a quick preview so you can verify
    print("Sample of NHL team lookup mapping (team_id -> team_name):")
    for k in sorted(list(id_to_name.keys()))[:10]:
        print(f"  {k}: {id_to_name[k]}")

    # Apply mapping to games
    games["home_team"] = games["home_team_id"].map(id_to_name)
    games["away_team"] = games["away_team_id"].map(id_to_name)

    # If any ids did not map, surface them clearly
    bad_home = games[games["home_team"].isna()]["home_team_id"].unique()
    bad_away = games[games["away_team"].isna()]["away_team_id"].unique()

    bad_ids = sorted(set(bad_home).union(set(bad_away)))
    if bad_ids:
        print("\nWARNING: Some team_ids do not have names in nhl_team_lookup.csv:")
        print("  Unmapped team_ids:", bad_ids)
        print("These games will still show numeric ids until you add names for them.")
    else:
        print("All NHL team_ids successfully mapped to names.")

    # Overwrite the same parquet so the API will pick up the new columns
    games.to_parquet(games_path, index=False)
    print(f"\nWrote updated NHL games with team names back to {games_path}")


if __name__ == "__main__":
    main()