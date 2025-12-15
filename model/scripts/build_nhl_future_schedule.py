"""
Build a schedule-only NHL parquet of upcoming games without touching history.

Output: model/data/processed/nhl/nhl_future_schedule_for_app.parquet
Schema aligns with /events: sport, sport_id, league, date, season, home/away team,
scores None, status UPCOMING, event_id/game_id prefixed with NHL_SCHED.
"""

from __future__ import annotations

import importlib.util
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List

import pandas as pd

# Paths
ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

try:
    from src.paths import RAW_DIR, NHL_PROCESSED_DIR  # type: ignore  # noqa: E402
except Exception:
    RAW_DIR = ROOT_DIR / "data" / "raw"
    NHL_PROCESSED_DIR = ROOT_DIR / "data" / "processed" / "nhl"

# Load canonical team mappings from build_nhl_from_moneypuck.py without modifying it.
TEAM_NAME_MAP: Dict[str, str]
CANONICAL_ABBREV_BY_NAME: Dict[str, str]

def _load_team_mappings() -> None:
    global TEAM_NAME_MAP, CANONICAL_ABBREV_BY_NAME
    if "scripts.build_nhl_from_moneypuck" in sys.modules:
        mod = sys.modules["scripts.build_nhl_from_moneypuck"]
    else:
        script_path = ROOT_DIR / "scripts" / "build_nhl_from_moneypuck.py"
        spec = importlib.util.spec_from_file_location("scripts.build_nhl_from_moneypuck", script_path)
        if spec is None or spec.loader is None:
            raise SystemExit(f"Unable to load build_nhl_from_moneypuck.py from {script_path}")
        mod = importlib.util.module_from_spec(spec)
        sys.modules["scripts.build_nhl_from_moneypuck"] = mod
        spec.loader.exec_module(mod)  # type: ignore[arg-type]

    try:
        TEAM_NAME_MAP = mod.TEAM_NAME_MAP  # type: ignore[attr-defined]
        CANONICAL_ABBREV_BY_NAME = mod.CANONICAL_ABBREV_BY_NAME  # type: ignore[attr-defined]
    except AttributeError as exc:  # pragma: no cover - defensive
        raise SystemExit(
            "build_nhl_from_moneypuck.py is missing TEAM_NAME_MAP or CANONICAL_ABBREV_BY_NAME."
        ) from exc


_load_team_mappings()
# Allow extra schedule-only aliases without touching the source mappings
TEAM_NAME_MAP.update(
    {
        "UTAHMAMMOTH": "Utah Hockey Club",
        "UTAHAMMOTH": "Utah Hockey Club",
    }
)

SPORT_ID_NHL = 4
RAW_SCHEDULE_CSV = RAW_DIR / "nhl_schedule" / "nhl_2025_26_schedule_parsed.csv"
OUTPUT_PATH = NHL_PROCESSED_DIR / "nhl_future_schedule_for_app.parquet"


def normalize_label(label: object) -> str | None:
    """Uppercase and strip punctuation/spaces for matching."""
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


def canonical_team_name(raw: object) -> str:
    key = normalize_label(raw)
    if key is None:
        raise SystemExit(f"Missing NHL team label in schedule: {raw}")
    if key in TEAM_NAME_MAP:
        return TEAM_NAME_MAP[key]
    # Allow raw full-name matches even if not in TEAM_NAME_MAP keys
    for name in CANONICAL_ABBREV_BY_NAME.keys():
        cmp_key = normalize_label(name)
        if cmp_key == key:
            return name
    raise SystemExit(f"Unknown NHL team label in schedule: '{raw}' (normalized='{key}')")


def canonical_abbrev(full_name: str) -> str:
    if full_name not in CANONICAL_ABBREV_BY_NAME:
        raise SystemExit(f"Missing canonical abbrev for team: {full_name}")
    return CANONICAL_ABBREV_BY_NAME[full_name]


def season_from_date(dt: datetime) -> str:
    """NHL season starts in early fall; use August 1 as cutoff to capture preseason."""
    start_year = dt.year if dt.month >= 8 else dt.year - 1
    return f"{start_year}-{(start_year + 1) % 100:02d}"


@dataclass
class ScheduleRow:
    date: datetime
    away_team: str
    home_team: str


def load_schedule_rows() -> List[ScheduleRow]:
    if RAW_SCHEDULE_CSV.exists():
        df = pd.read_csv(RAW_SCHEDULE_CSV)
        if not {"Date", "Visitor", "Home"}.issubset(df.columns):
            raise SystemExit(f"Schedule CSV missing required columns: {RAW_SCHEDULE_CSV}")
        df["Date"] = pd.to_datetime(df["Date"], errors="coerce")
        df = df.dropna(subset=["Date"])
        rows = []
        for _, row in df.iterrows():
            rows.append(
                ScheduleRow(
                    date=row["Date"].to_pydatetime(),
                    away_team=str(row["Visitor"]),
                    home_team=str(row["Home"]),
                )
            )
        return rows

    raise SystemExit(f"No NHL schedule source found. Expected: {RAW_SCHEDULE_CSV}")


def build_nhl_future_schedule() -> None:
    NHL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    now_utc = pd.Timestamp.utcnow().tz_localize(None)
    today_floor = pd.Timestamp.utcnow().normalize().tz_localize(None)

    raw_rows = load_schedule_rows()
    normalized_rows: list[ScheduleRow] = []
    for r in raw_rows:
        dt = pd.to_datetime(r.date, utc=True, errors="coerce").tz_localize(None)
        normalized_rows.append(ScheduleRow(date=dt, away_team=r.away_team, home_team=r.home_team))

    schedule_rows = [r for r in normalized_rows if r.date >= today_floor]

    data: list[dict[str, object]] = []
    for row in schedule_rows:
        home_name = canonical_team_name(row.home_team)
        away_name = canonical_team_name(row.away_team)
        home_abbr = canonical_abbrev(home_name)
        away_abbr = canonical_abbrev(away_name)

        event_id = f"NHL_SCHED|{row.date.date()}|{away_abbr}|{home_abbr}"

        game_dt = row.date.replace(tzinfo=None)
        status = "UPCOMING"
        if game_dt < now_utc:
            status = "IN_PROGRESS"

        data.append(
            {
                "sport": "NHL",
                "sport_id": SPORT_ID_NHL,
                "league": "NHL",
                "date": game_dt,
                "season": season_from_date(row.date),
                "home_team": home_name,
                "away_team": away_name,
                "home_pts": None,
                "away_pts": None,
                "status": status,
                "event_id": event_id,
                "game_id": event_id,
                "nhl_game_id_str": f"{game_dt.strftime('%Y_%m_%d')}_{home_abbr}_{away_abbr}",
            }
        )

    df = pd.DataFrame(data)

    # Hard validation
    if df.empty:
        raise SystemExit("No future NHL schedule rows found (rowcount=0).")

    df = df.sort_values("date").reset_index(drop=True)
    df = df.drop_duplicates(subset=["event_id"], keep="first")

    today_mask = df["date"].dt.normalize() == today_floor

    print(f"now (UTC-naive): {now_utc}")
    print(f"today_floor: {today_floor}")
    print(f"NHL future schedule rows: {len(df)}")
    print(f"Date range: {df['date'].min().date()} → {df['date'].max().date()}")
    print(f"Rows on today (UTC date): {int(today_mask.sum())}")
    print("Sample rows:")
    print(
        df[["date", "away_team", "home_team", "event_id", "status", "nhl_game_id_str"]]
        .head(5)
        .to_string(index=False)
    )

    df.to_parquet(OUTPUT_PATH, index=False)
    print(f"✅ Wrote NHL future schedule to {OUTPUT_PATH}")


if __name__ == "__main__":
    build_nhl_future_schedule()
