# events.py
from typing import Any, Dict, List, Optional, Tuple
from datetime import date as _date
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

def _parse_iso(s: Optional[str]) -> Optional[_date]:
    if not s:
        return None
    y, m, d = map(int, s.split("-"))
    return _date(y, m, d)

def list_events_service(
    sport_id: Optional[int],
    date_from: Optional[str],
    date_to: Optional[str],
    status: Optional[str],
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []

    if sport_id is not None:
        where.append("sport_id = %s")
        params.append(sport_id)
    if status:
        where.append("status = %s")
        params.append(status)

    d_from = _parse_iso(date_from) if date_from else None
    d_to   = _parse_iso(date_to) if date_to else None
    if d_from and d_to:
        where.append('"date" BETWEEN %s AND %s')
        params.extend([d_from, d_to])
    elif d_from:
        where.append('"date" >= %s')
        params.append(d_from)
    elif d_to:
        where.append('"date" <= %s')
        params.append(d_to)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
      SELECT event_id, sport_id, season, "date",
             home_team_id, away_team_id, venue, status, start_time
      FROM core.events
      {where_sql}
      ORDER BY "date" DESC, event_id DESC
      LIMIT %s OFFSET %s
    """

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, [*params, limit, offset])
        rows: List[Tuple] = cur.fetchall()

    items: List[Dict[str, Any]] = []
    for (event_id, sp, season, dt, home_id, away_id, venue, st, start_time) in rows or []:
        items.append({
            "event_id": event_id,
            "sport_id": sp,
            "season": season,
            "date": dt.isoformat(),
            "home_team_id": home_id,
            "away_team_id": away_id,
            "venue": venue,
            "status": st,
            "start_time": start_time.isoformat() if hasattr(start_time, "isoformat") else start_time,
        })
    return items

def get_event_service(event_id: int) -> Optional[Dict[str, Any]]:
    sql = """
      SELECT event_id, sport_id, season, "date",
             home_team_id, away_team_id, venue, status, start_time
      FROM core.events
      WHERE event_id = %s
    """
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, (event_id,))
        row = cur.fetchone()
    if not row:
        return None
    (event_id, sp, season, dt, home_id, away_id, venue, st, start_time) = row
    return {
        "event_id": event_id,
        "sport_id": sp,
        "season": season,
        "date": dt.isoformat(),
        "home_team_id": home_id,
        "away_team_id": away_id,
        "venue": venue,
        "status": st,
        "start_time": start_time.isoformat() if hasattr(start_time, "isoformat") else start_time,
    }