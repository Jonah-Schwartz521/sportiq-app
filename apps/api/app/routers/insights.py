# apps/api/app/routers/insights.py
from fastapi import APIRouter, HTTPException, Query
from typing import Literal
import psycopg
from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/insights", tags=["insights"])

@router.get("/{sport}")
def get_insights(
    sport: Literal["nba", "ufc"],
    limit: int = Query(5, ge=1, le=25, description="Max number of insights to return")
):
    """Return latest insights for a sport."""
    with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
        cur.execute("SELECT sport_id FROM core.sports WHERE key=%s", (sport,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Unknown sport")
        sport_id = row[0]

        cur.execute("""
            SELECT text, metric, sample_size, updated_at
            FROM core.insights
            WHERE sport_id=%s
            ORDER BY updated_at DESC, insight_id DESC
            LIMIT %s
        """, (sport_id, limit))
        rows = cur.fetchall()

    return {
        "sport": sport,
        "insights": [
            {"text": t, "metric": m, "sample_size": n, "updated_at": str(u)}
            for (t, m, n, u) in rows
        ]
    }