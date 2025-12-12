#!/usr/bin/env python
"""
Build a single NHL parquet for the app by merging:
  - Historic games with scores (MoneyPuck processed)
  - Future 2025-26 schedule games after today (no scores)

Outputs: model/data/processed/nhl/nhl_games_for_app.parquet
"""

from __future__ import annotations

import sys
from datetime import datetime, date
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

# Set up imports from model/src
MODEL_ROOT = Path(__file__).resolve().parents[1]
if str(MODEL_ROOT) not in sys.path:
    sys.path.append(str(MODEL_ROOT))

from src.paths import NHL_PROCESSED_DIR  # noqa: E402

# Raw schedule candidates (use the first that exists)
SCHEDULE_CSV_CANDIDATES = [
    MODEL_ROOT
    / "data"
    / "raw"
    / "nhl_schedule"
    / "2025-26_NHL"
    / "nhl_schedule_2025_26.csv",
    MODEL_ROOT
    / "data"
    / "raw"
    / "nhl_schedule"
    / "nhl_2025_26_schedule_parsed.csv",
]

LOOKUP_PATH = NHL_PROCESSED_DIR / "nhl_team_lookup.csv"

# Canonical NHL team full names mapping
# Maps all known variations (abbreviations, city names, dotted forms) to full team names
NHL_FULL_NAMES = {
    # Anaheim Ducks
    "ANA": "Anaheim Ducks",
    "ANAHEIM": "Anaheim Ducks",
    "ANAHEIMDUCKS": "Anaheim Ducks",
    # Boston Bruins
    "BOS": "Boston Bruins",
    "BOSTON": "Boston Bruins",
    "BOSTONBRUINS": "Boston Bruins",
    # Buffalo Sabres
    "BUF": "Buffalo Sabres",
    "BUFFALO": "Buffalo Sabres",
    "BUFFALOSABRES": "Buffalo Sabres",
    # Calgary Flames
    "CGY": "Calgary Flames",
    "CALGARY": "Calgary Flames",
    "CALGARYFLAMES": "Calgary Flames",
    # Carolina Hurricanes
    "CAR": "Carolina Hurricanes",
    "CAROLINA": "Carolina Hurricanes",
    "CAROLINAHURRICANES": "Carolina Hurricanes",
    # Chicago Blackhawks
    "CHI": "Chicago Blackhawks",
    "CHICAGO": "Chicago Blackhawks",
    "CHICAGOBLACKHAWKS": "Chicago Blackhawks",
    # Colorado Avalanche
    "COL": "Colorado Avalanche",
    "COLORADO": "Colorado Avalanche",
    "COLORADOAVALANCHE": "Colorado Avalanche",
    # Columbus Blue Jackets
    "CBJ": "Columbus Blue Jackets",
    "COLUMBUS": "Columbus Blue Jackets",
    "COLUMBUSBLUEJACKETS": "Columbus Blue Jackets",
    # Dallas Stars
    "DAL": "Dallas Stars",
    "DALLAS": "Dallas Stars",
    "DALLASSTARS": "Dallas Stars",
    # Detroit Red Wings
    "DET": "Detroit Red Wings",
    "DETROIT": "Detroit Red Wings",
    "DETROITREDWINGS": "Detroit Red Wings",
    # Edmonton Oilers
    "EDM": "Edmonton Oilers",
    "EDMONTON": "Edmonton Oilers",
    "EDMONTONOILERS": "Edmonton Oilers",
    # Florida Panthers
    "FLA": "Florida Panthers",
    "FLORIDA": "Florida Panthers",
    "FLORIDAPANTHERS": "Florida Panthers",
    # Los Angeles Kings
    "LAK": "Los Angeles Kings",
    "LA": "Los Angeles Kings",
    "L.A": "Los Angeles Kings",
    "LOSANGELES": "Los Angeles Kings",
    "LOSANGELESKINGS": "Los Angeles Kings",
    # Minnesota Wild
    "MIN": "Minnesota Wild",
    "MINNESOTA": "Minnesota Wild",
    "MINNESOTAWILD": "Minnesota Wild",
    # Montreal Canadiens
    "MTL": "Montreal Canadiens",
    "MONTREAL": "Montreal Canadiens",
    "MONTREALCANADIENS": "Montreal Canadiens",
    # Nashville Predators
    "NSH": "Nashville Predators",
    "NASHVILLE": "Nashville Predators",
    "NASHVILLEPREDATORS": "Nashville Predators",
    # New Jersey Devils
    "NJD": "New Jersey Devils",
    "NJ": "New Jersey Devils",
    "N.J": "New Jersey Devils",
    "NEWJERSEY": "New Jersey Devils",
    "NEWJERSEYDEVILS": "New Jersey Devils",
    # New York Islanders
    "NYI": "New York Islanders",
    "NYISLANDERS": "New York Islanders",
    "NEWYORKISLANDERS": "New York Islanders",
    # New York Rangers
    "NYR": "New York Rangers",
    "NYRANGERS": "New York Rangers",
    "NEWYORKRANGERS": "New York Rangers",
    # Ottawa Senators
    "OTT": "Ottawa Senators",
    "OTTAWA": "Ottawa Senators",
    "OTTAWASENATORS": "Ottawa Senators",
    # Philadelphia Flyers
    "PHI": "Philadelphia Flyers",
    "PHILADELPHIA": "Philadelphia Flyers",
    "PHILADELPHIAFLYERS": "Philadelphia Flyers",
    # Pittsburgh Penguins
    "PIT": "Pittsburgh Penguins",
    "PITTSBURGH": "Pittsburgh Penguins",
    "PITTSBURGHPENGUINS": "Pittsburgh Penguins",
    # San Jose Sharks
    "SJS": "San Jose Sharks",
    "SJ": "San Jose Sharks",
    "S.J": "San Jose Sharks",
    "SANJOSE": "San Jose Sharks",
    "SANJOSESHARKS": "San Jose Sharks",
    # Seattle Kraken
    "SEA": "Seattle Kraken",
    "SEATTLE": "Seattle Kraken",
    "SEATTLEKRAKEN": "Seattle Kraken",
    # St. Louis Blues
    "STL": "St. Louis Blues",
    "STLOUIS": "St. Louis Blues",
    "ST.LOUIS": "St. Louis Blues",
    "STLOUISBLUES": "St. Louis Blues",
    # Tampa Bay Lightning
    "TBL": "Tampa Bay Lightning",
    "TB": "Tampa Bay Lightning",
    "T.B": "Tampa Bay Lightning",
    "TAMPABAY": "Tampa Bay Lightning",
    "TAMPABAYLIGHTNING": "Tampa Bay Lightning",
    # Toronto Maple Leafs
    "TOR": "Toronto Maple Leafs",
    "TORONTO": "Toronto Maple Leafs",
    "TORONTOMAPLELEAFS": "Toronto Maple Leafs",
    # Utah Hockey Club (formerly Arizona Coyotes)
    "UTA": "Utah Hockey Club",
    "UTAH": "Utah Hockey Club",
    "UTAHAMMOTH": "Utah Hockey Club",  # Old name variant
    "UTAHMAMMOTH": "Utah Hockey Club",  # Another variant
    # Vancouver Canucks
    "VAN": "Vancouver Canucks",
    "VANCOUVER": "Vancouver Canucks",
    "VANCOUVERCANUCKS": "Vancouver Canucks",
    # Vegas Golden Knights
    "VGK": "Vegas Golden Knights",
    "VEGAS": "Vegas Golden Knights",
    "VEGASGOLDENKNIGHTS": "Vegas Golden Knights",
    # Washington Capitals
    "WSH": "Washington Capitals",
    "WASHINGTON": "Washington Capitals",
    "WASHINGTONCAPITALS": "Washington Capitals",
    # Winnipeg Jets
    "WPG": "Winnipeg Jets",
    "WINNIPEG": "Winnipeg Jets",
    "WINNIPEGJETS": "Winnipeg Jets",
    # Historic teams
    "ATL": "Atlanta Thrashers",  # Relocated to Winnipeg in 2011
    "ATLANTA": "Atlanta Thrashers",
    "ARI": "Arizona Coyotes",  # Moved to Utah in 2024
    "ARIZONA": "Arizona Coyotes",
    "ARIZONACOYOTES": "Arizona Coyotes",
}


def parse_start_et(raw: str | float | None) -> str:
    """Convert raw times like '7:00 PM' to '19:00' 24-hour ET; default 19:00."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "19:00"

    raw_str = str(raw).strip()
    if not raw_str:
        return "19:00"

    cleaned = raw_str.replace(" ", "").upper().replace("ET", "")
    cleaned = cleaned.replace("P.M.", "PM").replace("A.M.", "AM")
    # Handle trailing 'P' or 'A' without M
    if cleaned.endswith(("P", "A")):
        cleaned = cleaned + "M"

    # Try common patterns
    for fmt in ("%I:%M%p", "%I:%M %p", "%H:%M"):
        try:
            dt = datetime.strptime(raw_str, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            try:
                dt = datetime.strptime(cleaned, fmt)
                return dt.strftime("%H:%M")
            except ValueError:
                continue

    # Last resort: drop spaces and retry without colon (e.g., 700PM)
    compact = "".join(ch for ch in cleaned if ch.isalnum())
    for fmt in ("%I%M%p", "%I%p"):
        try:
            dt = datetime.strptime(compact, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            continue

    return "19:00"


def load_lookup_mapping() -> tuple[dict[str, str], str]:
    """Return mapping from abbrev variants -> canonical team name."""
    if not LOOKUP_PATH.exists():
        raise SystemExit(f"NHL team lookup not found at {LOOKUP_PATH}")

    lookup = pd.read_csv(LOOKUP_PATH)
    name_cols = [
        "team_full_name",
        "team_name",
        "name",
        "full_name",
        "team",
    ]
    abbrev_cols = ["team_abbrev", "team_code", "team", "abbrev", "abbr"]

    name_col = next((c for c in name_cols if c in lookup.columns), None)
    abbrev_col = next((c for c in abbrev_cols if c in lookup.columns), None)

    if name_col is None or abbrev_col is None:
        raise SystemExit(
            "nhl_team_lookup.csv must include an abbrev column "
            "(team_abbrev/team_code/abbrev) and a name column "
            "(team_full_name/team_name/name)."
        )

    lookup = lookup.copy()
    lookup[name_col] = lookup[name_col].astype(str).str.strip()
    lookup[abbrev_col] = lookup[abbrev_col].astype(str).str.strip()

    def base_key(label: str) -> str:
        return label.upper().replace(".", "").replace(" ", "")

    mapping: dict[str, str] = {}
    for _, row in lookup.iterrows():
        name = row[name_col]
        abbrev = row[abbrev_col]
        if not abbrev:
            continue
        keys = {
            abbrev.upper(),
            base_key(abbrev),
        }
        for key in keys:
            mapping.setdefault(key, name)

    # Manual aliases for dotted codes
    alias_sources = {
        "LAK": mapping.get("LA") or mapping.get("L.A"),
        "NJD": mapping.get("NJ") or mapping.get("N.J"),
        "SJS": mapping.get("SJ") or mapping.get("S.J"),
        "TBL": mapping.get("TB") or mapping.get("T.B"),
    }
    for alias, value in alias_sources.items():
        if value:
            mapping.setdefault(alias, value)

    return mapping, name_col


def normalize_team(label: str | float | None, mapping: dict[str, str]) -> str | None:
    """Map any team label to canonical full name; always returns full team names."""
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    raw = str(label).strip()
    if not raw:
        return None

    # First, try the comprehensive NHL_FULL_NAMES mapping
    # Generate normalized keys: uppercase, remove dots/spaces/special chars
    normalized_key = raw.replace(".", "").replace(" ", "").replace("-", "").upper()

    if normalized_key in NHL_FULL_NAMES:
        return NHL_FULL_NAMES[normalized_key]

    # Also try the raw uppercase version
    if raw.upper() in NHL_FULL_NAMES:
        return NHL_FULL_NAMES[raw.upper()]

    # Fallback to old CSV lookup mapping (should rarely be needed now)
    candidates = [raw.upper(), normalized_key]
    for key in candidates:
        if key in mapping:
            return mapping[key]

    # Last resort: if the raw string is already a full name, return it
    # This handles cases where the data already has full names like "Seattle Kraken"
    if normalized_key in [k.replace(".", "").replace(" ", "").replace("-", "").upper()
                           for k in NHL_FULL_NAMES.values()]:
        return raw

    # If we still can't map it, warn and return None to avoid bad data
    print(f"⚠️  Warning: Could not map team '{raw}' to full name")
    return None


def pick_historic_parquet() -> Path:
    """Select best historic NHL parquet that already has scores."""
    required = {"game_datetime", "home_team_name", "away_team_name", "home_score", "away_score"}
    candidates: list[tuple[Path, int, bool]] = []
    for path in sorted(NHL_PROCESSED_DIR.glob("*.parquet")):
        try:
            df = pd.read_parquet(path)
        except Exception as exc:  # pragma: no cover - defensive
            print(f"Skipping {path}: {exc}")
            continue
        if required.issubset(df.columns):
            prefers_scores = "with_scores" in path.name
            candidates.append((path, len(df), prefers_scores))
    if not candidates:
        raise SystemExit("No historic NHL parquet with scores found in processed dir.")

    # Pick the one with the most rows; prefer explicit *_with_scores files
    candidates.sort(key=lambda x: (-x[1], -int(x[2]), x[0].name))
    best_path = candidates[0][0]
    print(f"Using historic NHL parquet: {best_path}")
    return best_path


def normalize_hist_df(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    """Ensure historic data uses unified columns/types."""
    out = df.copy()
    out["game_datetime"] = pd.to_datetime(out["game_datetime"], utc=True, errors="coerce").dt.tz_localize(None)
    out = out.dropna(subset=["game_datetime"])

    out["home_team_name"] = out["home_team_name"].apply(lambda x: normalize_team(x, mapping))
    out["away_team_name"] = out["away_team_name"].apply(lambda x: normalize_team(x, mapping))

    # Drop rows where we couldn't map team names
    before_count = len(out)
    out = out.dropna(subset=["home_team_name", "away_team_name"])
    dropped = before_count - len(out)
    if dropped > 0:
        print(f"⚠️  Dropped {dropped} historic rows with unmappable team names")

    out["home_score"] = pd.to_numeric(out["home_score"], errors="coerce")
    out["away_score"] = pd.to_numeric(out["away_score"], errors="coerce")

    if "start_et" not in out.columns:
        if "start_time_et" in out.columns:
            out["start_et"] = out["start_time_et"]
        else:
            out["start_et"] = None

    out["league"] = out.get("league", "NHL")
    out["sport"] = out.get("sport", "NHL")

    keep_cols = [
        "game_datetime",
        "home_team_name",
        "away_team_name",
        "home_score",
        "away_score",
        "start_et",
        "league",
        "sport",
    ]
    # Preserve any extra metadata columns that already exist
    for extra in ["season", "season_label", "game_id"]:
        if extra in out.columns:
            keep_cols.append(extra)

    return out[keep_cols]


def build_future_schedule(mapping: dict[str, str]) -> pd.DataFrame:
    """Normalize the 2025-26 schedule into app-ready rows (scores empty)."""
    schedule_path = next((p for p in SCHEDULE_CSV_CANDIDATES if p.exists()), None)
    if schedule_path is None:
        raise SystemExit(
            f"NHL schedule CSV not found. Checked: {', '.join(str(p) for p in SCHEDULE_CSV_CANDIDATES)}"
        )

    print(f"Reading NHL schedule from: {schedule_path}")
    sched = pd.read_csv(schedule_path)
    required_cols = {"Date", "Visitor", "Home"}
    if missing := required_cols - set(sched.columns):
        raise SystemExit(f"Schedule CSV missing required columns: {missing}")

    sched = sched.copy()
    sched["date"] = pd.to_datetime(sched["Date"], errors="coerce").dt.date
    sched = sched.dropna(subset=["date"])

    time_col = None
    for cand in ("Start (ET)", "Start (Local)"):
        if cand in sched.columns:
            time_col = cand
            break
    if time_col:
        sched["start_et"] = sched[time_col].apply(parse_start_et)
    else:
        sched["start_et"] = "19:00"

    sched["raw_away_team_abbrev"] = sched["Visitor"].astype(str).str.strip()
    sched["raw_home_team_abbrev"] = sched["Home"].astype(str).str.strip()

    sched["away_team_name"] = sched["raw_away_team_abbrev"].apply(lambda x: normalize_team(x, mapping))
    sched["home_team_name"] = sched["raw_home_team_abbrev"].apply(lambda x: normalize_team(x, mapping))

    # Drop rows where we couldn't map team names
    before_count = len(sched)
    sched = sched.dropna(subset=["home_team_name", "away_team_name"])
    dropped = before_count - len(sched)
    if dropped > 0:
        print(f"⚠️  Dropped {dropped} schedule rows with unmappable team names")

    sched["game_datetime"] = pd.to_datetime(
        sched["date"].astype(str) + " " + sched["start_et"], errors="coerce"
    )

    sched["home_score"] = pd.NA
    sched["away_score"] = pd.NA
    sched["league"] = "NHL"
    sched["sport"] = "NHL"

    keep_cols = [
        "game_datetime",
        "home_team_name",
        "away_team_name",
        "home_score",
        "away_score",
        "start_et",
        "league",
        "sport",
        "raw_home_team_abbrev",
        "raw_away_team_abbrev",
    ]

    return sched[keep_cols]


def main() -> None:
    mapping, name_col = load_lookup_mapping()
    hist_path = pick_historic_parquet()
    hist_df = pd.read_parquet(hist_path)
    hist_norm = normalize_hist_df(hist_df, mapping)

    future_df = build_future_schedule(mapping)

    frames = [hist_norm]
    if not future_df.empty:
        frames.append(future_df)

    combined = pd.concat(frames, ignore_index=True)
    combined = combined.drop_duplicates(subset=["game_datetime", "home_team_name", "away_team_name"])
    combined = combined.sort_values("game_datetime").reset_index(drop=True)

    NHL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = NHL_PROCESSED_DIR / "nhl_games_for_app.parquet"
    combined.to_parquet(out_path, index=False)

    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    with_scores = combined["home_score"].notna() & combined["away_score"].notna()
    future_mask = combined["game_datetime"].dt.date > today_et

    print(f"✅ Wrote combined NHL parquet to {out_path}")
    print(f"Total rows: {len(combined):,}")
    print(
        f"Date range: {combined['game_datetime'].min().date()} → "
        f"{combined['game_datetime'].max().date()}"
    )
    print(f"Rows with scores: {with_scores.sum():,}")
    print(f"Rows without scores: {len(combined) - with_scores.sum():,}")
    print(f"Future games after {today_et}: {future_mask.sum():,}")


if __name__ == "__main__":
    main()
