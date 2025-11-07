# apps/api/app/routers/events.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
from datetime import date as _date, datetime as _dt, time as _t
import psycopg

from apps.api.app.core.config import POSTGRES_DSN
from apps.api.app.schemas.events import EventList, Event

router = APIRouter(prefix="/events", tags=["events"])


def _parse_iso(s: Optional[str], field: str) -> Optional[_date]:
    if not s:
        return None
    try:
        y, m, d = map(int, s.split("-"))
        return _date(y, m, d)
    except Exception:
        raise HTTPException(status_code=400, detail=f"Bad {field}: expected YYYY-MM-DD")


@router.get("", summary="List Events", response_model=EventList)
def list_events(
    sport_id: Optional[int] = Query(None, description="Filter by sport_id"),
    date_from: Optional[str] = Query(None, description="Inclusive start date (YYYY-MM-DD)"),
    date_to: Optional[str] = Query(None, description="Inclusive end date (YYYY-MM-DD)"),
    status: Optional[str] = Query(None, description="Filter by status (e.g., scheduled, in_progress, final)"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    where: List[str] = []
    params: List[Any] = []

    if sport_id is not None:
        where.append("sport_id = %s")
        params.append(sport_id)

    if status:
        where.append("status = %s")
        params.append(status)

    d_from = _parse_iso(date_from, "date_from")
    d_to = _parse_iso(date_to, "date_to")

    if d_from is not None and d_to is not None:
        where.append('"date" >= %s AND "date" <= %s')
        params.extend([d_from, d_to])
    elif d_from is not None:
        where.append('"date" >= %s')
        params.append(d_from)
    elif d_to is not None:
        where.append('"date" <= %s')
        params.append(d_to)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
        SELECT event_id, sport_id, season, "date", home_team_id, away_team_id, venue, status, start_time
        FROM core.events
        {where_sql}
        ORDER BY "date" DESC, event_id DESC
        LIMIT %s OFFSET %s
    """

    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(sql, [*params, limit, offset])
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    items: List[Dict[str, Any]] = []
    for (event_id, sp, season, dval, home_id, away_id, venue, st, start_time) in rows or []:
        date_str = dval.isoformat() if hasattr(dval, "isoformat") else dval
        start_time_str = (
            start_time.isoformat()
            if (isinstance(start_time, (_dt, _t)) or hasattr(start_time, "isoformat"))
            else start_time
        )
        items.append(
            {
                "event_id": event_id,
                "sport_id": sp,
                "season": season,
                "date": date_str,
                "home_team_id": home_id,
                "away_team_id": away_id,
                "venue": venue,
                "status": st,
                "start_time": start_time_str,
            }
        )

    return {"items": items, "total_returned": len(items), "limit": limit, "offset": offset}


@router.get("/{event_id}", summary="Get single event", response_model=Event)
def get_event(event_id: int):
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(
                """
                SELECT event_id, sport_id, season, "date", home_team_id, away_team_id, venue, status, start_time
                FROM core.events
                WHERE event_id = %s
                """,
                (event_id,),
            )
            row = cur.fetchone()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    if not row:
        raise HTTPException(status_code=404, detail="Event not found")

    (event_id, sp, season, dval, home_id, away_id, venue, st, start_time) = row
    date_str = dval.isoformat() if hasattr(dval, "isoformat") else dval
    start_time_str = (
        start_time.isoformat()
        if (isinstance(start_time, (_dt, _t)) or hasattr(start_time, "isoformat"))
        else start_time
    )

    return {
        "event_id": event_id,
        "sport_id": sp,
        "season": season,
        "date": date_str,
        "home_team_id": home_id,
        "away_team_id": away_id,
        "venue": venue,
        "status": st,
        "start_time": start_time_str,
    }