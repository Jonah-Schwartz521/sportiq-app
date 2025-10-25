SHELL := /bin/bash
include .env
export $(shell sed 's/=.*//' .env)

.PHONY: up down logs db api lint test psql seed

up:
	docker compose up -d

down:
	docker compose down -v

	docker compose logs -f

db:
	# wait for db, run migrations and seed
	sleep 2
	psql postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB) -f db/migrations/001_init.sql
	psql postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB) -f db/seeds/seed_core.sql

psql:
	psql postgresql://$(POSTGRES_USER):$(POSTGRES_PASSWORD)@$(POSTGRES_HOST):$(POSTGRES_PORT)/$(POSTGRES_DB)

api:
	cd apps/api && uvicorn app.main:app --reload --port $${API_PORT:-8000}

lint:
	python -m pip install ruff
	ruff check apps/api

test:
	python -m pytest -q
