# apps/api/app/adapters/base.py
from typing import Dict, Any

class SportAdapter:
    sport_code: str  # e.g., "nba", "ufc", "mlb"

    def validate(self, payload: Dict[str, Any]) -> None:
        pass  # raise ValueError on bad inputs

    def predict(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """Return: {"model_key": str, "win_probabilities": {...}, "generated_at": str}"""
        raise NotImplementedError

    def persist(self, event_id: int, result: Dict[str, Any]) -> None:
        pass  # optional: write to DB

    def explain(self, event_id: int) -> Dict[str, Any]:
        raise NotImplementedError  # optional per sport

    def insights(self, limit: int = 5) -> Dict[str, Any]:
        return {"items": []}  # optional