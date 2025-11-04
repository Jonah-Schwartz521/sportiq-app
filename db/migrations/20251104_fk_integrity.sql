-- 20251104_fk_integrity.sql
-- Purpose: Add FK integrity + helpful indexes for multi-sport scale.

BEGIN;

-- 0) Schemas (idempotent)
CREATE SCHEMA IF NOT EXISTS core;

-- 1) Sports
CREATE TABLE IF NOT EXISTS core.sports (
  sport_id   INT PRIMARY KEY,
  name       TEXT NOT NULL UNIQUE
);

-- Seed base sports if table is empty
INSERT INTO core.sports (sport_id, name)
SELECT * FROM (VALUES
  (1, 'NBA'),
  (2, 'UFC'),
  (3, 'MLB'),
  (4, 'NFL'),
  (5, 'NHL')
) AS v(sport_id, name)
WHERE NOT EXISTS (SELECT 1 FROM core.sports);

-- 2) Teams
CREATE TABLE IF NOT EXISTS core.teams (
  team_id   INT PRIMARY KEY,
  sport_id  INT NOT NULL,
  name      TEXT NOT NULL
);

-- (Optional) Make (team_id, sport_id) unique to enable composite FK patterns
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='core' AND indexname='uq_teams_teamid_sportid'
  ) THEN
    CREATE UNIQUE INDEX uq_teams_teamid_sportid
      ON core.teams (team_id, sport_id);
  END IF;
END$$;

-- 3) Events
CREATE TABLE IF NOT EXISTS core.events (
  event_id      INT PRIMARY KEY,
  sport_id      INT NOT NULL,
  season        INT,
  date          DATE,
  home_team_id  INT NULL,
  away_team_id  INT NULL,
  venue         TEXT,
  status        TEXT,
  start_time    TIMESTAMPTZ
);

-- Helpful index for listings by sport & date
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='core' AND indexname='idx_events_sport_date_desc'
  ) THEN
    CREATE INDEX idx_events_sport_date_desc
      ON core.events (sport_id, date DESC);
  END IF;
END$$;

-- 4) Predictions
CREATE TABLE IF NOT EXISTS core.predictions (
  pred_id     BIGSERIAL PRIMARY KEY,
  event_id    INT NOT NULL,
  model_key   TEXT NOT NULL,
  home_wp     NUMERIC,
  away_wp     NUMERIC,
  created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Helpful indexes for your /predictions filters
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='core' AND indexname='idx_predictions_event_id'
  ) THEN
    CREATE INDEX idx_predictions_event_id
      ON core.predictions (event_id);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='core' AND indexname='idx_predictions_model_key'
  ) THEN
    CREATE INDEX idx_predictions_model_key
      ON core.predictions (model_key);
  END IF;

  IF NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname='core' AND indexname='idx_predictions_created_at_desc'
  ) THEN
    CREATE INDEX idx_predictions_created_at_desc
      ON core.predictions (created_at DESC);
  END IF;
END$$;

-- 5) Foreign keys (idempotent with guards)

-- events.sport_id -> sports.sport_id
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='fk_events_sport'
  ) THEN
    ALTER TABLE core.events
      ADD CONSTRAINT fk_events_sport
      FOREIGN KEY (sport_id)
      REFERENCES core.sports(sport_id)
      ON UPDATE CASCADE ON DELETE RESTRICT;
  END IF;
END$$;

-- events (home_team_id, sport_id) -> teams (team_id, sport_id)
-- NOTE: Composite FK ensures the team's sport matches the event's sport.
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='fk_events_home_team_sport'
  ) THEN
    ALTER TABLE core.events
      ADD CONSTRAINT fk_events_home_team_sport
      FOREIGN KEY (home_team_id, sport_id)
      REFERENCES core.teams(team_id, sport_id)
      ON UPDATE CASCADE ON DELETE SET NULL;
  END IF;
END$$;

-- events (away_team_id, sport_id) -> teams (team_id, sport_id)
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='fk_events_away_team_sport'
  ) THEN
    ALTER TABLE core.events
      ADD CONSTRAINT fk_events_away_team_sport
      FOREIGN KEY (away_team_id, sport_id)
      REFERENCES core.teams(team_id, sport_id)
      ON UPDATE CASCADE ON DELETE SET NULL;
  END IF;
END$$;

-- predictions.event_id -> events.event_id
DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_constraint
    WHERE conname='fk_predictions_event'
  ) THEN
    ALTER TABLE core.predictions
      ADD CONSTRAINT fk_predictions_event
      FOREIGN KEY (event_id)
      REFERENCES core.events(event_id)
      ON UPDATE CASCADE ON DELETE CASCADE;
  END IF;
END$$;

COMMIT;