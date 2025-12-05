# model/scripts/build_mlb_teams.py

from __future__ import annotations
import sys
from pathlib import Path
import pandas as pd

ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import MLB_PROCESSED_DIR

LAHMAN_TEAMS = ROOT_DIR / "model/data/raw/mlb/lahman_1871_2024u/Teams.csv"
OUTPUT = MLB_PROCESSED_DIR / "mlb_teams.parquet"

# Manual 2025 mapping for Retrosheet-style team IDs.
# This lets us show proper team names for 2025 games even though Lahman stops at 2024.
TEAM_NAME_MAP_2025 = {
    "ANA": ("Los Angeles Angels", "AL"),
    "ARI": ("Arizona Diamondbacks", "NL"),
    "ATL": ("Atlanta Braves", "NL"),
    "BAL": ("Baltimore Orioles", "AL"),
    "BOS": ("Boston Red Sox", "AL"),
    "CHA": ("Chicago White Sox", "AL"),
    "CHN": ("Chicago Cubs", "NL"),
    "CIN": ("Cincinnati Reds", "NL"),
    "CLE": ("Cleveland Guardians", "AL"),
    "COL": ("Colorado Rockies", "NL"),
    "DET": ("Detroit Tigers", "AL"),
    "HOU": ("Houston Astros", "AL"),
    "KCA": ("Kansas City Royals", "AL"),
    "LAN": ("Los Angeles Dodgers", "NL"),
    "MIA": ("Miami Marlins", "NL"),
    "MIL": ("Milwaukee Brewers", "NL"),
    "MIN": ("Minnesota Twins", "AL"),
    "NYA": ("New York Yankees", "AL"),
    "NYN": ("New York Mets", "NL"),
    # Retrosheet 2025 uses "ATH" for Oakland; keep both mappings for safety
    "ATH": ("Oakland Athletics", "AL"),
    "OAK": ("Oakland Athletics", "AL"),
    "PHI": ("Philadelphia Phillies", "NL"),
    "PIT": ("Pittsburgh Pirates", "NL"),
    "SDN": ("San Diego Padres", "NL"),
    "SEA": ("Seattle Mariners", "AL"),
    "SFN": ("San Francisco Giants", "NL"),
    "SLN": ("St. Louis Cardinals", "NL"),
    "TBA": ("Tampa Bay Rays", "AL"),
    "TEX": ("Texas Rangers", "AL"),
    "TOR": ("Toronto Blue Jays", "AL"),
    "WAS": ("Washington Nationals", "NL"),
}

def build_mlb_teams():
    print("=== Building MLB teams from Lahman Teams.csv + 2025 override ===")
    if not LAHMAN_TEAMS.exists():
        raise RuntimeError(f"Lahman Teams.csv not found at {LAHMAN_TEAMS}")

    df = pd.read_csv(LAHMAN_TEAMS)

    # Keep only 2015–2024 from Lahman (Lahman does not have 2025 yet)
    df = df[(df["yearID"] >= 2015) & (df["yearID"] <= 2024)].copy()

    df = df.rename(
        columns={
            "yearID": "season",
            "teamID": "team_id",
            "name": "team_name",
            "lgID": "league",
        }
    )

    teams = df[["season", "team_id", "team_name", "league"]].drop_duplicates()

    # --- Inject 2025 mapping so Retrosheet 2025 logs have names ---
    extra_2025_rows = [
        {
            "season": 2025,
            "team_id": team_id,
            "team_name": team_name,
            "league": league,
        }
        for team_id, (team_name, league) in TEAM_NAME_MAP_2025.items()
    ]
    extra_2025_df = pd.DataFrame(extra_2025_rows)

    teams = pd.concat([teams, extra_2025_df], ignore_index=True)
    teams = teams.drop_duplicates(subset=["season", "team_id"]).reset_index(drop=True)

    print("2025 teams only:")
    teams_2025 = (
    teams[teams["season"] == 2025]
    .sort_values("team_id")
    .reset_index(drop=True)
    )
    print(teams_2025)

    print("\nCheck Oakland explicitly:")
    print(
        teams[
        (teams["season"] == 2025)
        & (teams["team_id"] == "OAK")
    ]
)


    

    MLB_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    teams.to_parquet(OUTPUT, index=False)

    print(f"\nWrote mlb_teams.parquet to:\n  {OUTPUT}")

if __name__ == "__main__":
    build_mlb_teams()