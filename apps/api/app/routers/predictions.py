# apps/api/app/routers/predictions.py
from fastapi import APIRouter, HTTPException, Query
from typing import Optional, List, Dict, Any
import psycopg
from apps.api.app.core.config import POSTGRES_DSN

router = APIRouter(prefix="/predictions", tags=["predictions"])

@router.get("")
def list_predictions(event_id: Optional[int] = Query(None, description="Filter by event_id")):
    try:
        with psycopg.connect(POSTGRES_DSN) as conn, conn.cursor() as cur:
            if event_id is None:
                cur.execute("""
                    SELECT pred_id, event_id, model_key, home_wp, away_wp, created_at
                    FROM core.predictions
                    ORDER BY pred_id DESC
                    LIMIT 50;
                """)
            else:
                cur.execute("""
                    SELECT pred_id, event_id, model_key, home_wp, away_wp, created_at
                    FROM core.predictions
                    WHERE event_id = %s
                    ORDER BY pred_id DESC
                    LIMIT 50;
                """, (event_id,))
            rows = cur.fetchall()
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"DB error: {e}")

    out: List[Dict[str, Any]] = []
    for pred_id, ev, mk, home_wp, away_wp, created_at in rows or []:
        out.append({
            "pred_id": pred_id,
            "event_id": ev,
            "model_key": mk,
            "home_wp": float(home_wp),
            "away_wp": float(away_wp),
            "created_at": created_at.isoformat() if created_at else None,
        })
    return out