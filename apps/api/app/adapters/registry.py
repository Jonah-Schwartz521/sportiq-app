# apps/api/app/adapters/registry.py
from apps.api.app.adapters.nba import NBAAdapter
from apps.api.app.adapters.ufc import UFCAdapter
from apps.api.app.adapters.mlb import MLBAdapter

ADAPTERS = {
    "nba": NBAAdapter(),
    "ufc": UFCAdapter(),
    "mlb": MLBAdapter(),
}