# Purpose: list saved predictions with optional filtering & pagination

from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

# Router is mounted at /predictions
router = APIRouter(prefix="/predictions", tags=["predictions"])

@router.get("", summary="List Predictions", description="List recent predictions with optional filters + pagination.")
def list_predictions(
    event_id: Optional[int] = Query(None, description="Filter by event_id"),
    model_key: Optional[str] = Query(None, description="Filter by model key (e.g. nba-winprob-0.3.0)"),
    limit: int = Query(50, ge=1, le=200, description="Max rows to return (1–200)"),
    offset: int = Query(0, ge=0, description="Offset for pagination"),
):
    """
    Returns:
        JSON object with: items[], total_returned, limit, offset.
    """
    where_clauses: List[str] = []
    params: List[Any] = []

    if event_id is not None:
        where_clauses.append("event_id = %s")
        params.append(event_id)

    if model_key is not None:
        where_clauses.append("model_key = %s")
        params.append(model_key)  # was wrongly appending event_id

    where_sql = ""
    if where_clauses:
        where_sql = "WHERE " + " AND ".join(where_clauses)

    sql = f"""
        SELECT pred_id, event_id, model_key, home_wp, away_wp, created_at
        FROM core.predictions
        {where_sql}
        ORDER BY pred_id DESC
        LIMIT %s OFFSET %s;
    """

    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(sql, [*params, limit, offset])
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    items: List[Dict[str, Any]] = []
    for r in rows or []:
        pred_id, ev, mk, home_wp, away_wp, created_at = r
        items.append({
            "pred_id": pred_id,
            "event_id": ev,
            "model_key": mk,
            "home_wp": float(home_wp),
            "away_wp": float(away_wp),
            "created_at": created_at.isoformat() if created_at else None,
        })

    return {
        "items": items,
        "total_returned": len(items),
        "limit": limit,
        "offset": offset,
    }