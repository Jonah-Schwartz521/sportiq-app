#!/usr/bin/env python
"""
Build future NHL games parquet for SportIQ.

- Reads raw NHL schedule CSVs from:
    model/data/raw/nhl_schedule/2025-26_NHL/*.csv
- Normalizes dates + ET start times.
- Maps teams to IDs/names using nhl_team_lookup.csv.
- Writes:
    model/data/processed/nhl/nhl_games_for_app.parquet

You can then have your API load this file (you already
have NHL loader wiring in model_api/main.py).
"""

from __future__ import annotations

import sys
from pathlib import Path
from datetime import datetime, date

import pandas as pd

# ---------- Paths ----------

# This script lives in model/scripts/, so:
ROOT_DIR = Path(__file__).resolve().parents[2]
if str(ROOT_DIR) not in sys.path:
    sys.path.append(str(ROOT_DIR))

from model.src.paths import NHL_PROCESSED_DIR  # type: ignore  # noqa

# Raw NHL schedule location (you just created this)
RAW_NHL_SCHEDULE_DIR = ROOT_DIR / "model" / "data" / "raw" / "nhl_schedule" / "2025-26_NHL"


# ---------- Helpers ----------

def parse_nhl_start_time(raw: str | float | None) -> str:
    """
    Parse raw start times like:
      "7:00 PM", "7:30p", "19:00", "7:00 p", etc.
    Return a clean 24-hour string "HH:MM" (e.g., "19:00").
    If missing or weird, default to 19:00 (7 PM).
    """
    if raw is None:
        return "19:00"
    if isinstance(raw, float):
        if pd.isna(raw):
            return "19:00"
        raw = str(raw)

    raw = str(raw).strip()
    if not raw:
        return "19:00"

    # Normalize little variations
    raw = raw.lower().replace("et", "").replace(" ", "")
    if raw.endswith("p"):
        raw = raw[:-1]  # "7:30p" -> "7:30"

    # Try several formats
    for fmt in ("%I:%M%p", "%I:%M", "%H:%M"):
        try:
            dt = datetime.strptime(raw, fmt)
            return dt.strftime("%H:%M")
        except ValueError:
            continue

    # Fallback
    return "19:00"


# ---------- Main builder ----------

def main() -> None:
    # 1. Read and combine all raw schedule CSVs
    if not RAW_NHL_SCHEDULE_DIR.exists():
        raise SystemExit(f"Raw NHL schedule dir not found: {RAW_NHL_SCHEDULE_DIR}")

    frames: list[pd.DataFrame] = []
    for csv_path in sorted(RAW_NHL_SCHEDULE_DIR.glob("*.csv")):
        print(f"Reading NHL schedule file: {csv_path}")
        df = pd.read_csv(csv_path)

        # Figure out which columns mean "away" and "home"
        if "Date" not in df.columns:
            raise SystemExit(f"{csv_path} has no 'Date' column; adjust script or CSV headers.")

        if "Visitor/Neutral" in df.columns:
            away_col = "Visitor/Neutral"
        elif "Visitor" in df.columns:
            away_col = "Visitor"
        else:
            raise SystemExit(f"{csv_path} is missing Visitor/Neutral or Visitor column")

        if "Home/Neutral" in df.columns:
            home_col = "Home/Neutral"
        elif "Home" in df.columns:
            home_col = "Home"
        else:
            raise SystemExit(f"{csv_path} is missing Home/Neutral or Home column")

        time_col = "Start (ET)" if "Start (ET)" in df.columns else None

        # Keep only what we need and rename to normalized names
        df = df[["Date", away_col, home_col] + ([time_col] if time_col else [])].copy()
        df.rename(
            columns={
                "Date": "raw_date",
                away_col: "raw_away_team",
                home_col: "raw_home_team",
                time_col: "raw_start_et" if time_col else None,
            },
            inplace=True,
        )
        frames.append(df)

    if not frames:
        raise SystemExit(f"No CSV files found under {RAW_NHL_SCHEDULE_DIR}")

    sched = pd.concat(frames, ignore_index=True)
    print(f"Combined NHL schedule rows: {len(sched)}")

    # 2. Normalize date + time
    sched["raw_date"] = pd.to_datetime(sched["raw_date"], errors="coerce")
    sched = sched.dropna(subset=["raw_date"]).copy()
    sched["date"] = sched["raw_date"].dt.date

    if "raw_start_et" in sched.columns:
        sched["start_time_et"] = sched["raw_start_et"].apply(parse_nhl_start_time)
    else:
        # If your CSV has no times at all, default 7 PM
        sched["start_time_et"] = "19:00"

    sched["season"] = "2025-26_NHL"
    sched["league"] = "NHL"
    sched["sport"] = "NHL"

    # 3. Map teams to IDs and canonical names using nhl_team_lookup.csv
    lookup_path = NHL_PROCESSED_DIR / "nhl_team_lookup.csv"
    print(f"Reading NHL team lookup from: {lookup_path}")
    teams = pd.read_csv(lookup_path)

    # Be flexible about column names
    col_map = {c.lower(): c for c in teams.columns}

    if "team_id" in col_map:
        id_col = col_map["team_id"]
    elif "id" in col_map:
        id_col = col_map["id"]
    else:
        raise SystemExit("nhl_team_lookup must have a 'team_id' or 'id' column")

    name_col = None
    for candidate in ("full_name", "name", "team_name"):
        if candidate in col_map:
            name_col = col_map[candidate]
            break

    abbrev_col = None
    for candidate in ("abbrev", "abbr", "code"):
        if candidate in col_map:
            abbrev_col = col_map[candidate]
            break

    if name_col is None and abbrev_col is None:
        raise SystemExit("nhl_team_lookup must have either full_name/name or abbrev/abbr column")

    # Normalize the lookup table
    keep_cols = [id_col]
    if name_col:
        keep_cols.append(name_col)
    if abbrev_col:
        keep_cols.append(abbrev_col)
    teams_norm = teams[keep_cols].copy()

    teams_norm[id_col] = teams_norm[id_col].astype(int)
    if name_col:
        teams_norm[name_col] = teams_norm[name_col].astype(str).str.strip()
    if abbrev_col:
        teams_norm[abbrev_col] = (
            teams_norm[abbrev_col].astype(str).str.strip().str.upper()
        )

    # Build a mapping from label → id
    id_by_label: dict[str, int] = {}
    if name_col:
        id_by_label.update(
            {n: i for i, n in zip(teams_norm[id_col], teams_norm[name_col])}
        )
    if abbrev_col:
        id_by_label.update(
            {a: i for i, a in zip(teams_norm[id_col], teams_norm[abbrev_col])}
        )

    def normalize_label(s):
        if pd.isna(s):
            return None
        s = str(s).strip()
        if not s:
            return None
        # We'll upper() so "Tor" / "tor" / "TOR" all match ABBR in lookup
        return s.upper()

    sched["away_label"] = sched["raw_away_team"].apply(normalize_label)
    sched["home_label"] = sched["raw_home_team"].apply(normalize_label)

    sched["away_team_id"] = sched["away_label"].map(id_by_label)
    sched["home_team_id"] = sched["home_label"].map(id_by_label)

    # Warn if anything didn't match
    missing = sched[sched["away_team_id"].isna() | sched["home_team_id"].isna()]
    if not missing.empty:
        print("WARNING: Some NHL schedule rows did not match nhl_team_lookup:")
        print(missing[["raw_date", "raw_away_team", "raw_home_team"]].head(20))
        print("→ Fix spelling/abbrevs in either the schedule CSV or nhl_team_lookup.csv")

    # For now, use the label as the display name. If your lookup has nice
    # full names, you can join them in instead.
    sched["away_team_name"] = sched["away_label"]
    sched["home_team_name"] = sched["home_label"]

    # 4. Keep only FUTURE games (after today)
    today = date.today()
    sched = sched[sched["date"] >= today].copy()
    sched = sched.reset_index(drop=True)
    print(f"Future NHL games from {today}: {len(sched)}")

    # 5. Generate unique game_ids for NHL future games
    # Pattern: 4 + YYYYMMDD + two-digit index for that day
    # (4 = sport_id for NHL in your app; adjust if you use a different id)
    sched["game_id"] = [
        int(f"4{d.strftime('%Y%m%d')}{i:02d}")
        for i, d in enumerate(pd.to_datetime(sched["date"]), start=1)
    ]

    # 6. Select columns used by your API layer
    out_cols = [
        "game_id",
        "date",
        "start_time_et",
        "season",
        "league",
        "sport",
        "home_team_id",
        "away_team_id",
        "home_team_name",
        "away_team_name",
    ]
    out = sched[out_cols].copy()

    NHL_PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    out_path = NHL_PROCESSED_DIR / "nhl_games_for_app.parquet"
    out.to_parquet(out_path, index=False)
    print(f"✅ Wrote {len(out)} future NHL games to {out_path}")


if __name__ == "__main__":
    main()