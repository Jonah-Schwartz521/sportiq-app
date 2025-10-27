# apps/api/app/routers/predict.py
from fastapi import APIRouter, HTTPException
from typing import Optional, Literal, Dict
from pydantic import BaseModel, ConfigDict
from pathlib import Path
from datetime import datetime, timezone

# Optional ML deps; we'll import lazily inside the NBA branch
# so the module can load even if sklearn/numpy aren't installed.

router = APIRouter(prefix="/predict", tags=["predict"])

# ----- Schemas -----
class PredictRequest(BaseModel):
    event_id: Optional[int] = None
    home_team: Optional[str] = None
    away_team: Optional[str] = None
    fighter_a: Optional[str] = None
    fighter_b: Optional[str] = None

class PredictResponse(BaseModel):
    model_config = ConfigDict(protected_namespaces=())  # silence pydantic "model_*" warning
    model_key: str
    win_probabilities: Dict[str, float]
    generated_at: str

# ----- Helpers -----
def _project_root() -> Path:
    # This file lives at: <root>/apps/api/app/routers/predict.py
    # parents[0]=routers, [1]=app, [2]=api, [3]=apps, [4]=<root>
    p = Path(__file__).resolve()
    return p.parents[4] if len(p.parents) >= 5 else p.parent.parent.parent.parent

def _nba_artifacts_dir() -> Path:
    return _project_root() / "models" / "nba" / "artifacts"

def _predict_nba(_: PredictRequest) -> Dict[str, float]:
    art = _nba_artifacts_dir()
    model_path = art / "model.joblib"
    meta_path = art / "feature_meta.json"

    if not model_path.exists() or not meta_path.exists():
        raise HTTPException(
            status_code=500,
            detail="NBA prediction error: NBA model artifact missing. Run `make train-nba` first."
        )

    # Lazy imports so the router loads even if ML deps aren’t installed for other routes
    try:
        from joblib import load
        import numpy as np
        import json
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NBA prediction error: {e}")

    try:
        model = load(model_path)
        meta = json.loads(meta_path.read_text())
        features = meta.get("features", [])
        # Create a neutral feature vector (all zeros) for demo purposes.
        # You can replace this with real features later.
        X = np.zeros((1, len(features)), dtype=float)
        proba_home = float(model.predict_proba(X)[0, 1])
        return {"home": proba_home, "away": 1.0 - proba_home}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"NBA prediction error: {e}")

def _predict_ufc(_: PredictRequest) -> Dict[str, float]:
    # Simple placeholder
    return {"fighter_a": 0.55, "fighter_b": 0.45}

# ----- Route -----
@router.post("/{sport}", response_model=PredictResponse)
def predict(sport: Literal["nba", "ufc"], payload: PredictRequest):
    if sport == "nba":
        from apps.api.app.services.nba_model import predict_winprob
        try: 
            result = predict_winprob(payload.event_id or 1)
            return result 
        except Exception as e:
            raise HTTPException(status_code=500, detail=f"NBA prediction error: {e}")    
        mk = "nba-winprob-0.1.0"
    elif sport == "ufc":
        probs = _predict_ufc(payload)
        mk = "ufc-winprob-0.1.0"
    else:
        raise HTTPException(status_code=400, detail="Unsupported sport")

    return {
        "model_key": mk,
        "win_probabilities": probs,
        "generated_at": datetime.now(timezone.utc).isoformat(),
    }