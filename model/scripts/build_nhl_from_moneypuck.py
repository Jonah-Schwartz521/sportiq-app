"""
Authoritative NHL backfill from MoneyPuck.

Reads every raw MoneyPuck NHL file under model/data/raw/nhl (csv/csv.gz/parquet),
rebuilds 2025+ games from scratch (home rows only), and merges them with untouched
pre-2025 history before writing:
  model/data/processed/nhl/nhl_games_for_app.parquet
"""

from __future__ import annotations

import sys
from pathlib import Path
from typing import List

import pandas as pd

# --- Paths / config ---------------------------------------------------------
SPORT_ID_NHL = 4
CUTOFF_2025 = pd.Timestamp("2025-01-01")

ROOT_DIR = Path(__file__).resolve().parents[1]  # .../model
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from src.paths import RAW_DIR, NHL_PROCESSED_DIR  # type: ignore  # noqa: E402
except Exception:
    RAW_DIR = ROOT_DIR / "data" / "raw"
    NHL_PROCESSED_DIR = ROOT_DIR / "data" / "processed" / "nhl"

RAW_NHL_DIR = RAW_DIR / "nhl"

# Canonical names + preferred abbreviations
TEAM_NAME_MAP: dict[str, str] = {
    # Core franchises
    "ANA": "Anaheim Ducks",
    "ANAHEIM": "Anaheim Ducks",
    "ANAHEIMDUCKS": "Anaheim Ducks",
    "ARI": "Arizona Coyotes",
    "ARIZONA": "Arizona Coyotes",
    "ARIZONACOYOTES": "Arizona Coyotes",
    "ATL": "Atlanta Thrashers",
    "ATLANTA": "Atlanta Thrashers",
    "ATLANTATHRASHERS": "Atlanta Thrashers",
    "BOS": "Boston Bruins",
    "BOSTON": "Boston Bruins",
    "BOSTONBRUINS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "BUFFALO": "Buffalo Sabres",
    "BUFFALOSABRES": "Buffalo Sabres",
    "CAR": "Carolina Hurricanes",
    "CAROLINA": "Carolina Hurricanes",
    "CAROLINAHURRICANES": "Carolina Hurricanes",
    "CBJ": "Columbus Blue Jackets",
    "COLUMBUS": "Columbus Blue Jackets",
    "COLUMBUSBLUEJACKETS": "Columbus Blue Jackets",
    "CGY": "Calgary Flames",
    "CALGARY": "Calgary Flames",
    "CALGARYFLAMES": "Calgary Flames",
    "CHI": "Chicago Blackhawks",
    "CHICAGO": "Chicago Blackhawks",
    "CHICAGOBLACKHAWKS": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",
    "COLORADO": "Colorado Avalanche",
    "COLORADOAVALANCHE": "Colorado Avalanche",
    "DAL": "Dallas Stars",
    "DALLAS": "Dallas Stars",
    "DALLASSTARS": "Dallas Stars",
    "DET": "Detroit Red Wings",
    "DETROIT": "Detroit Red Wings",
    "DETROITREDWINGS": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "EDMONTON": "Edmonton Oilers",
    "EDMONTONOILERS": "Edmonton Oilers",
    "FLA": "Florida Panthers",
    "FLORIDA": "Florida Panthers",
    "FLORIDAPANTHERS": "Florida Panthers",
    "LA": "Los Angeles Kings",
    "LAK": "Los Angeles Kings",
    "LOSANGELES": "Los Angeles Kings",
    "LOSANGELESKINGS": "Los Angeles Kings",
    "MIN": "Minnesota Wild",
    "MINNESOTA": "Minnesota Wild",
    "MINNESOTAWILD": "Minnesota Wild",
    "MTL": "Montreal Canadiens",
    "MONTREAL": "Montreal Canadiens",
    "MONTREALCANADIENS": "Montreal Canadiens",
    "NJ": "New Jersey Devils",
    "NJD": "New Jersey Devils",
    "NEWJERSEY": "New Jersey Devils",
    "NEWJERSEYDEVILS": "New Jersey Devils",
    "NSH": "Nashville Predators",
    "NASHVILLE": "Nashville Predators",
    "NASHVILLEPREDATORS": "Nashville Predators",
    "NYI": "New York Islanders",
    "NYISLANDERS": "New York Islanders",
    "NEWYORKISLANDERS": "New York Islanders",
    "NYR": "New York Rangers",
    "NYRANGERS": "New York Rangers",
    "NEWYORKRANGERS": "New York Rangers",
    "OTT": "Ottawa Senators",
    "OTTAWA": "Ottawa Senators",
    "OTTAWASENATORS": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PHILADELPHIA": "Philadelphia Flyers",
    "PHILADELPHIAFLYERS": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "PITTSBURGH": "Pittsburgh Penguins",
    "PITTSBURGHPENGUINS": "Pittsburgh Penguins",
    "SJ": "San Jose Sharks",
    "SJS": "San Jose Sharks",
    "SANJOSE": "San Jose Sharks",
    "SANJOSESHARKS": "San Jose Sharks",
    "SEA": "Seattle Kraken",
    "SEATTLE": "Seattle Kraken",
    "SEATTLEKRAKEN": "Seattle Kraken",
    "STL": "St. Louis Blues",
    "STLOUIS": "St. Louis Blues",
    "STLOUISBLUES": "St. Louis Blues",
    "TB": "Tampa Bay Lightning",
    "TBL": "Tampa Bay Lightning",
    "TAMPABAY": "Tampa Bay Lightning",
    "TAMPABAYLIGHTNING": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "TORONTO": "Toronto Maple Leafs",
    "TORONTOMAPLELEAFS": "Toronto Maple Leafs",
    "UTA": "Utah Hockey Club",
    "UTAH": "Utah Hockey Club",
    "VAN": "Vancouver Canucks",
    "VANCOUVER": "Vancouver Canucks",
    "VANCOUVERCANUCKS": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights",
    "VEGAS": "Vegas Golden Knights",
    "VEGASGOLDENKNIGHTS": "Vegas Golden Knights",
    "WPG": "Winnipeg Jets",
    "WINNIPEG": "Winnipeg Jets",
    "WINNIPEGJETS": "Winnipeg Jets",
    "WSH": "Washington Capitals",
    "WAS": "Washington Capitals",
    "WASHINGTON": "Washington Capitals",
    "WASHINGTONCAPITALS": "Washington Capitals",
    # Legacy / aliases
    "PHX": "Phoenix Coyotes",
    "PHOENIX": "Phoenix Coyotes",
    "PHOENIXCOYOTES": "Phoenix Coyotes",
}

CANONICAL_ABBREV_BY_NAME: dict[str, str] = {
    "Anaheim Ducks": "ANA",
    "Arizona Coyotes": "ARI",
    "Atlanta Thrashers": "ATL",
    "Boston Bruins": "BOS",
    "Buffalo Sabres": "BUF",
    "Carolina Hurricanes": "CAR",
    "Columbus Blue Jackets": "CBJ",
    "Calgary Flames": "CGY",
    "Chicago Blackhawks": "CHI",
    "Colorado Avalanche": "COL",
    "Dallas Stars": "DAL",
    "Detroit Red Wings": "DET",
    "Edmonton Oilers": "EDM",
    "Florida Panthers": "FLA",
    "Los Angeles Kings": "LAK",
    "Minnesota Wild": "MIN",
    "Montreal Canadiens": "MTL",
    "New Jersey Devils": "NJD",
    "Nashville Predators": "NSH",
    "New York Islanders": "NYI",
    "New York Rangers": "NYR",
    "Ottawa Senators": "OTT",
    "Philadelphia Flyers": "PHI",
    "Pittsburgh Penguins": "PIT",
    "San Jose Sharks": "SJS",
    "Seattle Kraken": "SEA",
    "St. Louis Blues": "STL",
    "Tampa Bay Lightning": "TBL",
    "Toronto Maple Leafs": "TOR",
    "Utah Hockey Club": "UTA",
    "Vancouver Canucks": "VAN",
    "Vegas Golden Knights": "VGK",
    "Winnipeg Jets": "WPG",
    "Washington Capitals": "WSH",
    "Phoenix Coyotes": "PHX",
}


# --- Helpers ---------------------------------------------------------------
def find_raw_files() -> List[Path]:
    """Recursively locate raw MoneyPuck files under RAW_NHL_DIR."""
    patterns = ["*.parquet", "*.csv", "*.csv.gz", "*.tsv", "*.tsv.gz"]
    files: List[Path] = []
    for pat in patterns:
        files.extend(sorted(RAW_NHL_DIR.rglob(pat)))
    return [f for f in files if f.is_file()]


def normalize_label(label: object) -> str | None:
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    cleaned = (
        str(label)
        .strip()
        .upper()
        .replace(".", "")
        .replace(" ", "")
        .replace("-", "")
        .replace("_", "")
    )
    return cleaned if cleaned else None


def to_full_name(label: object) -> str:
    key = normalize_label(label)
    if key is None:
        raise SystemExit(f"Missing NHL team label: {label}")
    if key in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[key]
    raise SystemExit(f"Unknown NHL team label from MoneyPuck: '{label}' (normalized='{key}')")


def format_season(start_year: int) -> str:
    return f"{start_year}-{(start_year + 1) % 100:02d}"


def read_raw_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    sep = "\t" if path.suffix.lower().endswith("tsv") or path.name.endswith(".tsv.gz") else ","
    return pd.read_csv(path, sep=sep)


def build_nhl_from_moneypuck() -> None:
    NHL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    raw_files = find_raw_files()
    if not raw_files:
        raise SystemExit(f"No MoneyPuck NHL files found under {RAW_NHL_DIR}")

    frames: list[pd.DataFrame] = []
    for path in raw_files:
        print(f"Reading MoneyPuck NHL file: {path}")
        frames.append(read_raw_frame(path))

    raw = pd.concat(frames, ignore_index=True)
    print(f"Loaded {len(raw):,} raw rows from {len(raw_files)} MoneyPuck file(s)")

    required_cols = [
        "gameId",
        "gameDate",
        "playerTeam",
        "opposingTeam",
        "home_or_away",
        "goalsFor",
        "goalsAgainst",
        "season",
        "position",
        "situation",
    ]
    missing = [c for c in required_cols if c not in raw.columns]
    if missing:
        raise SystemExit(f"Missing required MoneyPuck columns: {missing}")

    # Team-level rows contain both sides; keep the home row only.
    df = raw[(raw["position"] == "Team Level") & (raw["situation"] == "all")].copy()
    if df.empty:
        raise SystemExit("MoneyPuck extract is empty after filtering to Team Level + situation=all")

    df["date"] = pd.to_datetime(df["gameDate"].astype(str), format="%Y%m%d", errors="coerce")
    df["date"] = df["date"].dt.tz_localize("UTC").dt.tz_convert(None)
    if df["date"].isna().any():
        bad = df[df["date"].isna()].head()
        raise SystemExit(f"Failed to parse some gameDate values:\n{bad[['gameDate']].head()}")

    df["season_start_year"] = df["season"].astype(int)
    df["season"] = df["season_start_year"].apply(format_season)

    side = df["home_or_away"].astype(str).str.upper()
    df = df[side == "HOME"].copy()

    # Resolve teams + scores
    df["home_team_abbrev"] = df["playerTeam"].apply(normalize_label)
    df["away_team_abbrev"] = df["opposingTeam"].apply(normalize_label)
    df["home_team"] = df["home_team_abbrev"].apply(to_full_name)
    df["away_team"] = df["away_team_abbrev"].apply(to_full_name)

    df["home_pts"] = pd.to_numeric(df["goalsFor"], errors="coerce")
    df["away_pts"] = pd.to_numeric(df["goalsAgainst"], errors="coerce")

    df["event_id"] = df["gameId"].astype(str)
    df["game_id"] = pd.to_numeric(df["gameId"], errors="coerce").astype("Int64")
    df["league"] = "NHL"
    df["sport"] = "NHL"
    df["sport_id"] = SPORT_ID_NHL

    has_scores = df["home_pts"].notna() & df["away_pts"].notna()
    df["status"] = has_scores.map({True: "FINAL", False: "UPCOMING"})

    # Canonical abbreviations for ID strings
    df["home_team_abbrev"] = df["home_team"].map(CANONICAL_ABBREV_BY_NAME)
    df["away_team_abbrev"] = df["away_team"].map(CANONICAL_ABBREV_BY_NAME)
    df["nhl_game_id_str"] = (
        df["date"].dt.strftime("%Y_%m_%d")
        + "_"
        + df["home_team_abbrev"].fillna("").astype(str)
        + "_"
        + df["away_team_abbrev"].fillna("").astype(str)
    )

    # Only rebuild/post 2025 data from MoneyPuck
    df_2025_plus = df[df["date"] >= CUTOFF_2025].copy()
    if df_2025_plus.empty:
        raise SystemExit("No NHL games on or after 2025-01-01 were found in MoneyPuck data.")

    # Deduplicate new games strictly on event_id (MoneyPuck file overlaps)
    before_dedupe = len(df_2025_plus)
    df_2025_plus = df_2025_plus.sort_values("date").drop_duplicates(subset=["event_id"], keep="last")
    removed = before_dedupe - len(df_2025_plus)
    if removed > 0:
        print(f"Removed {removed} duplicate 2025+ rows based on event_id.")

    games_path = NHL_PROCESSED_DIR / "nhl_games_for_app.parquet"
    existing_cols: list[str] | None = None
    preserved_pre2025: pd.DataFrame | None = None

    if games_path.exists():
        existing = pd.read_parquet(games_path)
        existing["date"] = pd.to_datetime(existing.get("date"), errors="coerce")
        preserved_pre2025 = existing[existing["date"] < CUTOFF_2025].copy()
        existing_cols = list(existing.columns)
        print(f"Preserved {len(preserved_pre2025):,} pre-2025 rows from existing parquet.")

    # Align columns so pre-2025 rows stay intact
    if preserved_pre2025 is not None:
        all_cols = sorted(set(existing_cols or []).union(df_2025_plus.columns))
        preserved_pre2025 = preserved_pre2025.reindex(columns=all_cols)
        df_2025_plus = df_2025_plus.reindex(columns=all_cols)
        combined = pd.concat([preserved_pre2025, df_2025_plus], ignore_index=True)
    else:
        combined = df_2025_plus.copy()

    if "event_id" not in combined.columns:
        raise SystemExit("event_id column is missing; cannot deduplicate.")

    combined = combined.sort_values("date").reset_index(drop=True)

    # Mandatory validation prints
    combined_dates = pd.to_datetime(combined["date"], errors="coerce")
    min_date = combined_dates.min()
    max_date = combined_dates.max()
    total_rows = len(combined)
    print(f"Total NHL rows: {total_rows:,}")
    print(f"Date range: {min_date.date()} -> {max_date.date()}")

    years = [2022, 2023, 2024, 2025, 2026]
    year_counts = {yr: int((combined_dates.dt.year == yr).sum()) for yr in years}
    print("Count by calendar year (2022-2026):")
    for yr in years:
        print(f"  {yr}: {year_counts[yr]:,}")

    games_2025 = combined[combined_dates.dt.year == 2025].copy()
    games_2025 = games_2025.sort_values("date")
    print(f"Games in 2025: {len(games_2025):,}")
    display_cols = [c for c in ["date", "season", "home_team", "away_team", "home_pts", "away_pts", "status", "event_id"] if c in combined.columns]

    if len(games_2025) < 1_200:
        raise SystemExit(f"NHL 2025 calendar year count too low: {len(games_2025)} (< 1,200)")

    print("First 5 games in 2025:")
    print(games_2025.head(5)[display_cols].to_string(index=False))

    print("Last 5 games (future-most):")
    print(combined.sort_values("date").tail(5)[display_cols].to_string(index=False))

    combined.to_parquet(games_path, index=False)
    print(f"✅ Wrote NHL games parquet to {games_path}")


if __name__ == "__main__":
    build_nhl_from_moneypuck()
