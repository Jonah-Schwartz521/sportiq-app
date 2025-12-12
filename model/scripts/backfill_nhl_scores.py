#!/usr/bin/env python
"""
Backfill missing NHL scores in the combined parquet.

Steps:
- Load combined NHL parquet (games_for_app).
- Identify past games (date < today ET) with missing scores.
- Load best historic NHL parquet with scores.
- Map team abbreviations to canonical names using nhl_team_lookup.csv.
- Join and fill missing scores when a historic match exists.
- Write the updated combined parquet back to NHL_PROCESSED_DIR.
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

LOOKUP_PATH = NHL_PROCESSED_DIR / "nhl_team_lookup.csv"
COMBINED_PATH = NHL_PROCESSED_DIR / "nhl_games_for_app.parquet"

# NHL abbrev → full name (mirrors web/lib/teams.ts)
ABBR_TO_FULL = {
    "ANA": "Anaheim Ducks",
    "ARI": "Arizona Coyotes",
    "BOS": "Boston Bruins",
    "BUF": "Buffalo Sabres",
    "CAR": "Carolina Hurricanes",
    "CBJ": "Columbus Blue Jackets",
    "CGY": "Calgary Flames",
    "CHI": "Chicago Blackhawks",
    "COL": "Colorado Avalanche",
    "DAL": "Dallas Stars",
    "DET": "Detroit Red Wings",
    "EDM": "Edmonton Oilers",
    "FLA": "Florida Panthers",
    "LAK": "Los Angeles Kings",
    "MIN": "Minnesota Wild",
    "MTL": "Montreal Canadiens",
    "NJD": "New Jersey Devils",
    "NSH": "Nashville Predators",
    "NYI": "New York Islanders",
    "NYR": "New York Rangers",
    "OTT": "Ottawa Senators",
    "PHI": "Philadelphia Flyers",
    "PIT": "Pittsburgh Penguins",
    "SEA": "Seattle Kraken",
    "SJS": "San Jose Sharks",
    "STL": "St. Louis Blues",
    "TBL": "Tampa Bay Lightning",
    "TOR": "Toronto Maple Leafs",
    "UTA": "Utah Hockey Club",
    "VAN": "Vancouver Canucks",
    "VGK": "Vegas Golden Knights",
    "WPG": "Winnipeg Jets",
    "WSH": "Washington Capitals",
    "L.A": "Los Angeles Kings",
    "LA": "Los Angeles Kings",
    "N.J": "New Jersey Devils",
    "NJ": "New Jersey Devils",
    "S.J": "San Jose Sharks",
    "SJ": "San Jose Sharks",
    "T.B": "Tampa Bay Lightning",
    "TB": "Tampa Bay Lightning",
    "ATL": "Atlanta Thrashers",
    "PHX": "Phoenix Coyotes",
    "WAS": "Washington Capitals",
}

FULL_TO_ABBR: dict[str, str] = {}
for abbr, full in ABBR_TO_FULL.items():
    key = full.upper().replace(".", "").replace(" ", "")
    FULL_TO_ABBR.setdefault(key, abbr[:3].upper())

# Add city/short aliases derived from full names to help map schedule labels like "Chicago"
ALIAS_TO_ABBR: dict[str, str] = {}
for abbr, full in ABBR_TO_FULL.items():
    cleaned = full.upper().replace(".", "")
    parts = cleaned.split()
    if parts:
        city = "".join(parts[:1])
        ALIAS_TO_ABBR.setdefault(city, abbr[:3].upper())
        if len(parts) >= 2:
            city_two = "".join(parts[:2])
            ALIAS_TO_ABBR.setdefault(city_two, abbr[:3].upper())
    compact = "".join(parts)
    if compact:
        ALIAS_TO_ABBR.setdefault(compact, abbr[:3].upper())
ALIAS_TO_ABBR.setdefault("UTAHMAMMOTH", "UTA")  # observed in schedule

def load_lookup_mapping() -> dict[str, str]:
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
            "nhl_team_lookup.csv must include both a name column "
            "and an abbreviation column."
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

    return mapping


def normalize_team(label: str | float | None, mapping: dict[str, str]) -> str | None:
    """Map raw abbrev to canonical name using lookup; fallback to raw string."""
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    raw = str(label).strip()
    if not raw:
        return None
    candidates = [raw.upper(), raw.replace(".", "").replace(" ", "").upper()]
    for key in candidates:
        if key in mapping:
            return mapping[key]
    return raw


def canonical_abbrev(label: str | float | None) -> str | None:
    """Convert a team label (abbr or full name) to a canonical abbreviation."""
    if label is None or (isinstance(label, float) and pd.isna(label)):
        return None
    raw = str(label).strip()
    if not raw:
        return None

    def clean(s: str) -> str:
        return s.upper().replace(".", "").replace(" ", "")

    # 1) Direct match in ABBR_TO_FULL keys (including variants)
    for key in ABBR_TO_FULL.keys():
        if clean(key) == clean(raw):
            # Return a stable 3-letter-ish abbrev (use cleaned key trimmed to 3 if needed)
            base = key.replace(".", "").upper()
            return base if len(base) <= 3 else base[:3]

    # 2) Full-name match
    full_key = clean(raw)
    if full_key in FULL_TO_ABBR:
        return FULL_TO_ABBR[full_key]

    # 3) City/alias match
    if full_key in ALIAS_TO_ABBR:
        return ALIAS_TO_ABBR[full_key]

    return clean(raw)


def pick_historic_parquet() -> Path:
    """Select best historic NHL parquet that already has scores."""
    required = {"game_datetime", "home_team_name", "away_team_name", "home_score", "away_score"}
    candidates: list[tuple[Path, int, bool]] = []
    for path in sorted(NHL_PROCESSED_DIR.glob("*.parquet")):
        if path.name == "nhl_games_for_app.parquet":
            continue  # avoid using the combined file as the source of truth
        try:
            df = pd.read_parquet(path)
        except Exception:
            continue
        if required.issubset(df.columns):
            prefers_scores = "with_scores" in path.name
            candidates.append((path, len(df), prefers_scores))
    if not candidates:
        raise SystemExit("No historic NHL parquet with scores found in processed dir.")

    candidates.sort(key=lambda x: (-x[1], -int(x[2]), x[0].name))
    return candidates[0][0]


def normalize_hist_df(df: pd.DataFrame, mapping: dict[str, str]) -> pd.DataFrame:
    out = df.copy()
    out["game_datetime"] = pd.to_datetime(out["game_datetime"], utc=True, errors="coerce").dt.tz_localize(None)
    out = out.dropna(subset=["game_datetime"])

    out["home_team_name"] = out["home_team_name"].apply(lambda x: normalize_team(x, mapping))
    out["away_team_name"] = out["away_team_name"].apply(lambda x: normalize_team(x, mapping))

    out["home_abbr"] = out["home_team_name"].apply(canonical_abbrev)
    out["away_abbr"] = out["away_team_name"].apply(canonical_abbrev)
    out["game_date"] = out["game_datetime"].dt.date

    out["home_score"] = pd.to_numeric(out["home_score"], errors="coerce")
    out["away_score"] = pd.to_numeric(out["away_score"], errors="coerce")

    return out[
        [
            "game_datetime",
            "game_date",
            "home_team_name",
            "home_abbr",
            "away_team_name",
            "away_abbr",
            "home_score",
            "away_score",
        ]
    ]


def main() -> None:
    if not COMBINED_PATH.exists():
        raise SystemExit(f"Combined NHL parquet not found at {COMBINED_PATH}")

    mapping = load_lookup_mapping()

    combined = pd.read_parquet(COMBINED_PATH)
    combined["game_datetime"] = pd.to_datetime(combined["game_datetime"], errors="coerce").dt.tz_localize(None)
    combined["game_date"] = combined["game_datetime"].dt.date
    combined["home_abbr"] = combined["home_team_name"].apply(canonical_abbrev)
    combined["away_abbr"] = combined["away_team_name"].apply(canonical_abbrev)

    today_et = datetime.now(ZoneInfo("America/New_York")).date()
    missing_mask = (
        combined["game_date"] < today_et
    ) & (
        combined["home_score"].isna() | combined["away_score"].isna()
    )

    missing_count = int(missing_mask.sum())
    print(f"Past games with missing scores: {missing_count}")
    if missing_count == 0:
        print("No backfill needed.")
        return

    hist_path = pick_historic_parquet()
    print(f"Loading historic scores from: {hist_path}")
    hist_df = pd.read_parquet(hist_path)
    hist_norm = normalize_hist_df(hist_df, mapping)

    # Build maps keyed by (datetime, home, away)
    def build_keyed_map(series_df: pd.DataFrame, col: str) -> dict[tuple, float]:
        keyed = series_df.set_index(
            ["game_date", "home_abbr", "away_abbr"]
        )[col]
        return keyed.dropna().to_dict()

    home_score_map = build_keyed_map(hist_norm, "home_score")
    away_score_map = build_keyed_map(hist_norm, "away_score")

    combined["__key"] = list(
        zip(
            combined["game_date"],
            combined["home_abbr"],
            combined["away_abbr"],
        )
    )

    # Apply maps to missing rows
    combined.loc[missing_mask, "home_score"] = combined.loc[missing_mask, "__key"].map(home_score_map).combine_first(
        combined.loc[missing_mask, "home_score"]
    )
    combined.loc[missing_mask, "away_score"] = combined.loc[missing_mask, "__key"].map(away_score_map).combine_first(
        combined.loc[missing_mask, "away_score"]
    )

    combined = combined.drop(columns=["__key", "game_date", "home_abbr", "away_abbr"])

    # Recompute missing after backfill
    missing_after = combined.loc[
        (pd.to_datetime(combined["game_datetime"], errors="coerce").dt.date < today_et)
        & (combined["home_score"].isna() | combined["away_score"].isna())
    ]

    filled = missing_count - len(missing_after)
    print(f"Backfilled scores for {filled} games; remaining without scores: {len(missing_after)}")

    combined.to_parquet(COMBINED_PATH, index=False)
    print(f"✅ Updated parquet written to {COMBINED_PATH}")


if __name__ == "__main__":
    main()
