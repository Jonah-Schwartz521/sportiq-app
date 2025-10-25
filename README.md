# SportIQ — Starter (M0)

This is a minimal scaffold to get **PostgreSQL + Redis + FastAPI** running for the SportIQ MVP.

## Prereqs
- Docker Desktop (or Docker + Compose)
- Python 3.11+ (for local tooling)

## Quickstart (Terminal)
1. Copy envs
   ```bash
   cp .env.example .env
   ```
2. Start infra (Postgres + Redis)
   ```bash
   make up
   ```
3. Create schema + seed minimal data
   ```bash
   make db
   ```
4. Run the API locally (hot reload)
   ```bash
   make api
   ```
5. Visit docs at http://127.0.0.1:8000/docs

## Repo layout
```
sportiq/
  apps/api/            FastAPI service
  db/migrations/       SQL schema
  db/seeds/            Seed data
  docs/api/            OpenAPI spec
  docker-compose.yml   Postgres + Redis
  Makefile             Common tasks
```
## Next steps
- Replace seed data with real NBA/UFC historicals.
- Add `/explain/{sport}` and `/insights/{sport}` endpoints.
- Wire in model artifacts and SHAP precomputes.
