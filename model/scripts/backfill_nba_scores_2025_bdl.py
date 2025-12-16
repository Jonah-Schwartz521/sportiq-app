#!/usr/bin/env python3
"""
Backfill NBA scores for the 2025 season from BallDontLie.

Behaviors:
- Fetches 2025 games in chunks (monthly, paginated) to avoid rate limits.
- Always refreshes the most recent 14 days.
- Builds canonical columns: date (naive UTC), home_team, away_team, home_pts, away_pts, status, sport.
- Writes to model/data/processed/nba/nba_games_with_scores.parquet
- Stores a lightweight cache of chunk hashes in model/data/processed/nba/nba_backfill_state.json
- Deterministic/idempotent: sorting + dedupe on (date, home_team, away_team).

Environment:
- BALLDONTLIE_API_KEY optional (passed as Bearer if provided).
"""

from __future__ import annotations

import hashlib
import json
import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import pandas as pd
import requests

ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = ROOT / "model" / "data" / "processed" / "nba"
OUTPUT_PATH = PROCESSED_DIR / "nba_games_with_scores.parquet"
STATE_PATH = PROCESSED_DIR / "nba_backfill_state.json"

YEAR = 2025
CHUNK_DAYS = 31  # monthly-ish
RECENT_DAYS = 30  # always refresh last 30 days
BDL_BASE = "https://api.balldontlie.io/v1/games"
PER_PAGE = 100

DATE_ALIASES = [
    "date",
    "game_date",
    "start_date",
    "start_time",
    "startTime",
    "datetime",
    "scheduled",
    "commence_time",
    "tipoff_time",
    "time",
    "utc_start",
    "start_at",
]


def ensure_date_column(df: pd.DataFrame) -> pd.DataFrame:
    """Ensure df has a `date` column parsed as datetime. Renames common alternates when present."""
    if df is None:
        df = pd.DataFrame()
    if df.empty:
        # still guarantee the column exists
        if "date" not in df.columns:
            df = df.copy()
            df["date"] = pd.to_datetime(pd.Series([], dtype="datetime64[ns]"), errors="coerce")
        else:
            df = df.copy()
            df["date"] = pd.to_datetime(df["date"], errors="coerce")
        return df

    df = df.copy()

    if "date" not in df.columns:
        for alt in DATE_ALIASES[1:]:  # skip 'date'
            if alt in df.columns:
                df = df.rename(columns={alt: "date"})
                break

    if "date" not in df.columns:
        df["date"] = pd.NaT

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    return df


def load_state() -> Dict[str, str]:
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except Exception:
            return {}
    return {}


def save_state(state: Dict[str, str]) -> None:
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    STATE_PATH.write_text(json.dumps(state, indent=2))


def chunk_ranges(year: int) -> List[Tuple[datetime, datetime, str]]:
    ranges: List[Tuple[datetime, datetime, str]] = []
    start = datetime(year, 1, 1, tzinfo=timezone.utc)
    end_of_year = datetime(year + 1, 1, 1, tzinfo=timezone.utc) - timedelta(seconds=1)

    cur = start
    idx = 1
    while cur <= end_of_year:
        nxt = min(cur + timedelta(days=CHUNK_DAYS - 1), end_of_year)
        label = f"{year}-{idx:02d}"
        ranges.append((cur, nxt, label))
        cur = nxt + timedelta(days=1)
        idx += 1
    return ranges


def fetch_chunk(start_dt: datetime, end_dt: datetime, headers: Dict[str, str]) -> List[Dict]:
    records: List[Dict] = []
    page = 1
    max_retries = 5
    while True:
        params = {
            "start_date": start_dt.strftime("%Y-%m-%d"),
            "end_date": end_dt.strftime("%Y-%m-%d"),
            "per_page": PER_PAGE,
            "page": page,
        }

        for attempt in range(1, max_retries + 1):
            resp = requests.get(BDL_BASE, params=params, headers=headers, timeout=30)
            if resp.status_code == 429:
                retry_after = resp.headers.get("Retry-After")
                sleep_s = int(retry_after) if retry_after and retry_after.isdigit() else 5 * attempt
                print(f"⚠️  429 rate limit for {params['start_date']}..{params['end_date']} page={page}, sleeping {sleep_s}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_s)
                continue
            if resp.status_code >= 500:
                sleep_s = 5 * attempt
                print(f"⚠️  {resp.status_code} from BDL for page={page}, sleeping {sleep_s}s (attempt {attempt}/{max_retries})")
                time.sleep(sleep_s)
                continue
            resp.raise_for_status()
            break
        else:
            # Exhausted retries
            resp.raise_for_status()

        data = resp.json()
        page_data = data.get("data", [])
        records.extend(page_data)
        # Small pause to reduce likelihood of 429
        time.sleep(0.3)
        meta = data.get("meta", {}) or {}
        if not page_data or page >= meta.get("total_pages", 0):
            break
        page += 1
    return records


def normalize_status(raw_status: str) -> str:
    if not raw_status:
        return "SCHEDULED"
    s = raw_status.lower()
    if "final" in s:
        return "FINAL"
    if "in progress" in s or "live" in s:
        return "IN_PROGRESS"
    return "SCHEDULED"


def normalize_df(records: List[Dict]) -> pd.DataFrame:
    rows = []
    for r in records:
        date_str = r.get("date")
        dt = pd.to_datetime(date_str, utc=True, errors="coerce")
        if pd.isna(dt):
            continue
        dt_naive = dt.tz_localize(None)
        home = r.get("home_team", {}) or {}
        away = r.get("visitor_team", {}) or {}
        status = normalize_status(r.get("status"))
        home_pts = r.get("home_team_score")
        away_pts = r.get("visitor_team_score")
        # For scheduled/pre games, keep scores as None to avoid 0/0 finals.
        if status in ("SCHEDULED", "PRE"):
            home_pts = None
            away_pts = None

        rows.append(
            {
                "date": dt_naive,
                "home_team": home.get("full_name") or home.get("name"),
                "away_team": away.get("full_name") or away.get("name"),
                "home_pts": home_pts,
                "away_pts": away_pts,
                "status": status,
                "sport": "NBA",
                "season": r.get("season"),
                "id": r.get("id"),
            }
        )
    df = pd.DataFrame(rows)
    if df.empty:
        return df

    # Dedupe: prefer rows with scores then latest date string
    df["has_scores"] = df["home_pts"].notna() & df["away_pts"].notna()
    df = df.sort_values(["date", "has_scores"], ascending=[True, False])
    df = df.drop_duplicates(subset=["date", "home_team", "away_team"], keep="first")
    df = df.drop(columns=["has_scores"])
    return df


def compute_hash(df: pd.DataFrame) -> str:
    if df.empty:
        return "empty"
    subset = df[["date", "home_team", "away_team", "home_pts", "away_pts", "status"]].copy()
    subset["date"] = subset["date"].astype(str)
    data_bytes = subset.to_json(orient="records", date_format="iso", date_unit="s").encode("utf-8")
    return hashlib.md5(data_bytes).hexdigest()


def load_existing() -> pd.DataFrame:
    if OUTPUT_PATH.exists():
        return pd.read_parquet(OUTPUT_PATH)
    return pd.DataFrame()


def main() -> None:
    api_key = os.getenv("BALLDONTLIE_API_KEY")
    headers = {}
    if api_key:
        headers["Authorization"] = f"Bearer {api_key}"

    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    state = load_state()

    all_chunks: List[pd.DataFrame] = []
    chunk_labels: List[str] = []

    ranges = chunk_ranges(YEAR)
    now_utc = datetime.now(tz=timezone.utc)
    recent_start = now_utc - timedelta(days=RECENT_DAYS)

    for start_dt, end_dt, label in ranges:
        force_refresh = start_dt >= recent_start
        records = fetch_chunk(start_dt, end_dt, headers=headers)
        df_chunk = normalize_df(records)
        h = compute_hash(df_chunk)
        prev_hash = state.get(label)

        if not force_refresh and prev_hash and prev_hash == h:
            # Cache hit, but still keep cached hash
            print(f"[SKIP] {label}: hash unchanged ({h}), using existing data if present.")
        else:
            print(f"[FETCH] {label}: rows={len(df_chunk)} hash={h}")
            state[label] = h
        all_chunks.append(df_chunk)
        chunk_labels.append(label)

    combined = pd.concat(all_chunks, ignore_index=True) if all_chunks else pd.DataFrame()
    combined = ensure_date_column(combined)

    # Filter by calendar year window (NBA season spans years; don't rely on season field)
    year_start = datetime(YEAR, 1, 1)
    today = datetime.now()
    year_end = datetime(YEAR, 12, 31, 23, 59, 59)
    target_end = min(year_end, today + timedelta(days=1))

    # Only filter if the `date` column actually contains values
    if "date" in combined.columns:
        combined = combined[(combined["date"] >= year_start) & (combined["date"] <= target_end)]

    # Merge with existing to preserve other seasons/years (season != YEAR stays)
    existing = load_existing()
    existing = ensure_date_column(existing)

    if not existing.empty:
        existing["season"] = pd.to_numeric(existing.get("season"), errors="coerce")
        print("Existing parquet seasons (before merge):")
        print(existing["season"].value_counts(dropna=False).head(20))
        others = existing[existing["season"] != YEAR]
        existing_year = existing[existing["season"] == YEAR]
    else:
        others = pd.DataFrame()
        existing_year = pd.DataFrame()

    # Build combined 2025 slice with existing 2025 rows + freshly fetched
    combined_2025 = pd.concat([existing_year, combined], ignore_index=True)

    # Final dedupe and sort
    for df in (others, combined_2025):
        if not df.empty:
            df = ensure_date_column(df)
            df["season"] = pd.to_numeric(df.get("season"), errors="coerce")

    def quality(row):
        status = str(row.get("status", "")).upper()
        has_scores = pd.notna(row.get("home_pts")) and pd.notna(row.get("away_pts"))
        if status == "FINAL" and has_scores:
            return 3
        if has_scores:
            return 2
        if status in ("IN_PROGRESS", "LIVE"):
            return 1
        return 0

    def dedupe(df: pd.DataFrame) -> pd.DataFrame:
        if df.empty:
            return df
        df = df.copy()
        df["__quality"] = df.apply(quality, axis=1)
        df = df.sort_values(
            ["date", "season", "home_team", "away_team", "__quality"],
            ascending=[True, True, True, True, False],
        )
        df = df.drop_duplicates(subset=["date", "home_team", "away_team", "season"], keep="first")
        return df.drop(columns="__quality")

    combined_2025 = dedupe(combined_2025)
    others = dedupe(others)

    merged = pd.concat([others, combined_2025], ignore_index=True)
    if merged.empty:
        raise RuntimeError("No data collected for NBA backfill; aborting write.")

    merged["season"] = pd.to_numeric(merged.get("season"), errors="coerce").astype("Int64")
    merged = merged.sort_values("date")

    # Safety guards
    if not existing.empty:
        seasons_before = existing["season"].nunique(dropna=True)
        seasons_after = merged["season"].nunique(dropna=True)
        if seasons_after < seasons_before:
            raise RuntimeError(f"Refusing to write: seasons dropped from {seasons_before} to {seasons_after}")
        rows_before = len(existing)
        rows_after = len(merged)
        if rows_after < rows_before * 0.95:
            raise RuntimeError(f"Refusing to write: rows fell from {rows_before} to {rows_after}")

    merged.to_parquet(OUTPUT_PATH, index=False)

    save_state(state)

    num_final = (merged["status"] == "FINAL").sum() if "status" in merged.columns else 0
    min_date = merged["date"].min()
    max_date = merged["date"].max()
    dec_count = merged[merged["date"].dt.month == 12].shape[0]
    recent_cutoff = datetime.now() - timedelta(days=2)
    season_counts = merged["season"].value_counts(dropna=False)
    status_counts = merged["status"].value_counts(dropna=False)
    status_counts_2025 = merged[merged["season"] == YEAR]["status"].value_counts(dropna=False)

    print(f"✅ Wrote {len(merged)} rows to {OUTPUT_PATH}")
    print(f"   FINAL games: {num_final}")
    print(f"   Date range: {min_date} -> {max_date}")
    print(f"   December rows: {dec_count}")
    print("   Rows by season:")
    print(season_counts.head(20))
    print("   Status counts (all):")
    print(status_counts)
    print("   Status counts (2025):")
    print(status_counts_2025)
    print("   Sample:")
    print(merged.tail(5)[["date", "season", "home_team", "away_team", "home_pts", "away_pts", "status"]])

    if pd.notna(max_date) and max_date < recent_cutoff:
        print(f"⚠️  Warning: latest game {max_date} is older than expected (recent_cutoff={recent_cutoff.date()}). Recent coverage may be missing.")


if __name__ == "__main__":
    main()
