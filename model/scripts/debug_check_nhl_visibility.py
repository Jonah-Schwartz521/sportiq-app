#!/usr/bin/env python3
"""
Failing-check script for NHL visibility.
Requires API running locally at http://127.0.0.1:8000.
Exits non-zero if NHL data or games endpoint returns no rows.
"""
from __future__ import annotations

import sys
import datetime as dt
import requests

BASE = "http://127.0.0.1:8000"


def main() -> None:
    # 1) debug/nhl
    resp = requests.get(f"{BASE}/debug/nhl", timeout=10)
    resp.raise_for_status()
    data = resp.json()

    loaded = data.get("loaded_rows", 0)
    if loaded <= 0:
        raise SystemExit("NHL debug shows 0 rows")

    min_date = data.get("min_date")
    if not min_date or str(min_date) > "2008-10-01":
        raise SystemExit(f"Unexpected min_date: {min_date}")

    print("debug/nhl OK:", loaded, "rows; min_date", min_date, "max_date", data.get("max_date"))

    # 2) pick a date within range
    target_date = "2025-12-14"
    resp2 = requests.get(f"{BASE}/events", params={"sport": "NHL", "limit": 20000}, timeout=15)
    resp2.raise_for_status()
    items = resp2.json().get("items", [])
    rows = [r for r in items if r.get("date") == target_date]
    if not rows:
        raise SystemExit(f"/events returned 0 rows for NHL date {target_date}")

    print(f"/events OK: found {len(rows)} rows for {target_date}")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print("FAILED:", exc)
        sys.exit(1)
