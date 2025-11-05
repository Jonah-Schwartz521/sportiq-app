from fastapi import APIRouter, Query, HTTPException, Request
from typing import List, Dict, Any
import psycopg

from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/predictions", tags=["predictions"])

@router.get("", summary="List predictions")
def list_predictions(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    sql = """
      SELECT event_id, model_key, home_wp, away_wp, created_at
      FROM core.predictions
      ORDER BY created_at DESC
      LIMIT %s OFFSET %s
    """
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            cur.execute(sql, [limit, offset])
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    items: List[Dict[str, Any]] = []
    for event_id, model_key, home_wp, away_wp, created_at in rows or []:
        items.append({
            "event_id": event_id,
            "model_key": model_key,
            "home_wp": float(home_wp) if home_wp is not None else None,
            "away_wp": float(away_wp) if away_wp is not None else None,
            "created_at": created_at.isoformat() if hasattr(created_at, "isoformat") else created_at,
        })

    # Legacy behavior for tests: if no query params, return a raw list
    if not request.query_params:   # i.e., hit with /predictions exactly
        return items

    # Otherwise, return the paginated envelope
    return {"items": items, "total_returned": len(items), "limit": limit, "offset": offset}