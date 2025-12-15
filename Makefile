SHELL := /bin/bash
include .env
export $(shell sed 's/=.*//' .env)

# -------- Defaults --------
API_PORT ?= 8000
DB_URL := postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)

.PHONY: up down logs db api lint test psql seed migrate nhl

up:
	docker compose up -d

down:
	docker compose down -v

logs:
	docker compose logs -f

# Run all migrations in order, then seed
migrate:
	# wait for db, run migrations
	sleep 2
	@for f in $$(ls -1 db/migrations/*.sql | sort); do \
		echo "Applying $$f"; \
		psql $(DB_URL) -f $$f; \
	done

seed:
	psql $(DB_URL) -f db/seeds/seed_core.sql

db: migrate seed

psql:
	psql $(DB_URL)

api:
	@lsof -ti:8000 | xargs kill -9 2>/dev/null || true
	uvicorn model.model_api.main:app --reload --port $(API_PORT)
	

lint:
	python -m pip install ruff
	ruff check apps/api

test:
	python -m pytest -q

train-nba:
	python models/nba/train_baseline.py

# --- NHL rebuild (MoneyPuck) ---
nhl:
	python model/scripts/build_nhl_from_moneypuck.py
	@test -f model/data/processed/nhl/nhl_games_for_app.parquet

nhl_future:
	python model/scripts/build_nhl_future_schedule.py
	@test -f model/data/processed/nhl/nhl_future_schedule_for_app.parquet

# --- NHL model pipeline ---
nhl-build:
	python model/scripts/build_nhl_model_games.py

nhl-train: nhl-build
	python model/scripts/train_nhl_baseline.py

nhl-predict: nhl-build
	python model/scripts/predict_future_nhl.py

nhl-all: nhl-build nhl-train nhl-predict
