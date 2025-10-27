CREATE TABLE IF NOT EXISTS core.insights (
  insight_id   SERIAL PRIMARY KEY,
  sport_id     INT REFERENCES core.sports(sport_id),
  text         TEXT NOT NULL,
  metric       TEXT,
  sample_size  INT,
  updated_at   TIMESTAMP DEFAULT now()
);
