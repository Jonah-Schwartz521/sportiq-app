# teams.py
from typing import Any, Dict, List, Optional, Tuple
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

def list_teams_service(
    sport_id: Optional[int],
    q: Optional[str],
    limit: int,
    offset: int,
) -> List[Dict[str, Any]]:
    where: List[str] = []
    params: List[Any] = []

    if sport_id is not None:
        where.append("sport_id = %s")
        params.append(sport_id)
    if q:
        where.append("LOWER(name) LIKE %s")
        params.append(f"%{q.lower()}%")

    where_sql = f"WHERE {' AND '.join(where)}" if where else ""
    sql = f"""
      SELECT team_id, sport_id, name
      FROM core.teams
      {where_sql}
      ORDER BY team_id DESC
      LIMIT %s OFFSET %s
    """

    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, [*params, limit, offset])
        rows: List[Tuple] = cur.fetchall()

    return [{"team_id": tid, "sport_id": sp, "name": name} for (tid, sp, name) in rows or []]

def get_team_service(team_id: int) -> Optional[Dict[str, Any]]:
    sql = """SELECT team_id, sport_id, name FROM core.teams WHERE team_id = %s"""
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute(sql, (team_id,))
        row = cur.fetchone()
    if not row:
        return None
    team_id, sp, name = row
    return {"team_id": team_id, "sport_id": sp, "name": name}