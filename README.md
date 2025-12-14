# sportiq-app

Backend API for SportIQ — a multi-sport win probability and insights platform.

Implements endpoints for teams, events, predictions, and insights with full automated test coverage.  
FastAPI + PostgreSQL stack.

---

## Setup

```bash
git clone https://github.com/Jonah-Schwartz521/sportiq-app.git
cd sportiq-app
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp .env.example .env


POSTGRES_DSN=postgresql://user:password@localhost:5432/sportiq


make api
# or:
# uvicorn apps.api.app.main:app --reload --port 8000


http://localhost:8000



curl http://localhost:8000/health
# {"status":"ok"}


PYTHONPATH=. pytest -q apps/api/tests


API Reference

/health

GET /health

Response: {"status":"ok"}


/teams

GET /teams

Query:
	•	sport_id (int, optional)
	•	q (string, optional)
	•	limit (int, default 50)
	•	offset (int, default 0)

Response: {
  "items":[{"team_id":7,"sport_id":1,"name":"Los Angeles Lakers"}],
  "total_returned":1,"limit":50,"offset":0
}


GET /teams/{team_id}
Response: {"team_id":7,"sport_id":1,"name":"Los Angeles Lakers"}


GET /events

Query:
	•	sport_id, status
	•	date_from, date_to (YYYY-MM-DD)
	•	limit, offset

Response: {
  "items":[
    {"event_id":1,"sport_id":1,"season":2025,"date":"2025-10-01",
     "home_team_id":7,"away_team_id":8,"venue":"Staples Center",
     "status":"scheduled","start_time":"19:30:00"}
  ],
  "total_returned":1,"limit":50,"offset":0
}
GET /events/{event_id}
Returns single event.
404 if not found.


/predict/{sport}

sport ∈ nba | mlb | nfl | nhl | ufc
POST /predict/nba
{"event_id":123}

{
  "model_key":"nba-winprob-0.1.0",
  "win_probabilities":{"home":0.55,"away":0.45},
  "generated_at":"2025-11-11T06:38:51.668256Z"
}


POST /predict/ufc
{"fighter_a":"A","fighter_b":"B"}


{
  "model_key":"ufc-winprob-0.1.0",
  "win_probabilities":{"fighter_a":0.55,"fighter_b":0.45},
  "generated_at":"2025-11-11T06:40:00Z"
}

## NHL model pipeline (win probability)

Run end-to-end:

```bash
make nhl-all     # build features, train baseline, predict upcoming
```

Individual steps:
```bash
make nhl-build   # builds model/data/processed/nhl/nhl_model_games.parquet
make nhl-train   # trains logistic regression -> model/artifacts/nhl/*
make nhl-predict # writes model/data/processed/nhl/nhl_predictions_future.parquet
```

The FastAPI backend loads `nhl_predictions_future.parquet`, joins by
`nhl_game_id_str = YYYY_MM_DD_HOME_AWAY`, and exposes the AI Win Probability
bar for upcoming NHL games only. Final games show scores; upcoming games show
model_snapshot.
