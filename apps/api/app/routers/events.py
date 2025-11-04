from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/events", tags=["events"])

@router.get("", summary="List Events")
def list_events(
    sport_id: Optional[int] = Query(None, description="Filter by sport_id"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    where: List[str] = []
    params: List[Any] = []

    if sport_id is not None:
        where.append("sport_id = %s")
        params.append(sport_id)

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
      SELECT event_id, sport_id, season, date, home_team_id, away_team_id, venue, status, start_time
      FROM core.events
      {where_sql}
      ORDER BY event_id DESC
      LIMIT %s OFFSET %s
    """

    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(sql, [*params, limit, offset])
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    items: List[Dict[str, Any]] = []
    for (event_id, sp, season, date, home_id, away_id, venue, status, start_time) in rows or []:
        items.append({
            "event_id": event_id,
            "sport_id": sp,
            "season": season,
            "date": date.isoformat() if date else None,
            "home_team_id": home_id,
            "away_team_id": away_id,
            "venue": venue,
            "status": status,
            "start_time": start_time.isoformat() if start_time else None,
        })

    return {"items": items, "total_returned": len(items), "limit": limit, "offset": offset}