# apps/api/app/routers/teams.py
from fastapi import APIRouter, Query, HTTPException
from typing import Optional, List, Dict, Any
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/teams", tags=["teams"])

@router.get("", summary="List Teams")
def list_teams(
    sport_id: Optional[int] = Query(None, description="Filter by sport_id"),
    q: Optional[str] = Query(None, description="Case-insenitive search over team name"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    where: List[str] = []
    params: List[Any] = []

    if sport_id is not None:
        where.append("sport_id = %s")
        params.append(sport_id)

    if q:
        where.append("name ILIKE %s")
        params.append(f"%{q}")
        
    where_sql = f"WHERE {' AND '.join(where)}" if where else ""

    sql = f"""
      SELECT team_id, sport_id, name
      FROM core.teams
      {where_sql}
      ORDER BY team_id DESC
      LIMIT %s OFFSET %s
    """
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(sql, [*params, limit, offset])
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    items: List[Dict[str, Any]] = [
        {"team_id": team_id, "sport_id": sp, "name": name}
        for (team_id, sp, name) in rows or []
    ]
    return {"items": items, "total_returned": len(items), "limit": limit, "offset": offset}