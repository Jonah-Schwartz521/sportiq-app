-- Core and sport-specific schemas
CREATE SCHEMA IF NOT EXISTS core;
CREATE SCHEMA IF NOT EXISTS nba;
CREATE SCHEMA IF NOT EXISTS ufc;

-- Core tables
CREATE TABLE IF NOT EXISTS core.sports (
  sport_id SERIAL PRIMARY KEY,
  key TEXT UNIQUE NOT NULL,
  name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS core.teams (
  team_id SERIAL PRIMARY KEY,
  sport_id INT REFERENCES core.sports(sport_id),
  ext_ref TEXT,
  name TEXT NOT NULL,
  abbrev TEXT
);

CREATE TABLE IF NOT EXISTS core.players (
  player_id SERIAL PRIMARY KEY,
  team_id INT REFERENCES core.teams(team_id),
  name TEXT NOT NULL,
  position TEXT,
  dob DATE
);

CREATE TABLE IF NOT EXISTS core.events (
  event_id SERIAL PRIMARY KEY,
  sport_id INT REFERENCES core.sports(sport_id),
  season INT,
  date DATE,
  home_team_id INT REFERENCES core.teams(team_id),
  away_team_id INT REFERENCES core.teams(team_id),
  venue TEXT,
  status TEXT
);

CREATE TABLE IF NOT EXISTS core.predictions (
  pred_id SERIAL PRIMARY KEY,
  event_id INT REFERENCES core.events(event_id),
  model_key TEXT NOT NULL,
  home_wp NUMERIC,
  away_wp NUMERIC,
  created_at TIMESTAMP DEFAULT now()
);

CREATE TABLE IF NOT EXISTS core.explanations (
  pred_id INT REFERENCES core.predictions(pred_id),
  rank INT,
  feature_name TEXT,
  shap_value NUMERIC,
  contribution_text TEXT
);

CREATE TABLE IF NOT EXISTS core.user_picks (
  pick_id SERIAL PRIMARY KEY,
  user_id TEXT,
  event_id INT REFERENCES core.events(event_id),
  selection TEXT,
  created_at TIMESTAMP DEFAULT now(),
  outcome TEXT,
  correct BOOLEAN
);

-- NBA specific
CREATE TABLE IF NOT EXISTS nba.boxscores (
  id SERIAL PRIMARY KEY,
  event_id INT REFERENCES core.events(event_id),
  team_id INT REFERENCES core.teams(team_id),
  ortg NUMERIC,
  drtg NUMERIC,
  rebound_margin NUMERIC,
  three_pt_rate NUMERIC,
  tov_rate NUMERIC
);

-- UFC specific
CREATE TABLE IF NOT EXISTS ufc.fighters (
  fighter_id SERIAL PRIMARY KEY,
  name TEXT NOT NULL,
  reach_cm NUMERIC,
  stance TEXT,
  dob DATE
);

CREATE TABLE IF NOT EXISTS ufc.bouts (
  bout_id SERIAL PRIMARY KEY,
  event_id INT REFERENCES core.events(event_id),
  fighter_a_id INT REFERENCES ufc.fighters(fighter_id),
  fighter_b_id INT REFERENCES ufc.fighters(fighter_id),
  result TEXT
);
