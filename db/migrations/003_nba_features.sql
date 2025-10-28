CREATE SCHEMA IF NOT EXISTS nba;

DROP VIEW IF EXISTS nba.features_v1;

CREATE VIEW nba.features_v1 AS
SELECT
    e.event_id,

    -- Core identifiers
    e.sport_id,
    e.season,
    e.date,
    e.home_team_id,
    e.away_team_id,

    -- ====== MODEL FEATURES (keep order stable!) ======
    1::int                                                AS home_flag,
    0.58::double precision                                AS prior_home_adv,     -- simple prior; replace later

    -- Team strength proxies
    t_home.elo::double precision                          AS home_elo_proxy,
    t_away.elo::double precision                          AS away_elo_proxy,

    -- Rest (days since last game)
    GREATEST(0, (CURRENT_DATE - t_home.last_game_date))::double precision AS home_rest_days,
    GREATEST(0, (CURRENT_DATE - t_away.last_game_date))::double precision AS away_rest_days,

    -- Style stats (seeded earlier)
    t_home.three_pt_rate::double precision                AS home_three_pt_rate,
    t_away.three_pt_rate::double precision                AS away_three_pt_rate,
    t_home.rebound_margin::double precision               AS home_rebound_margin,
    t_away.rebound_margin::double precision               AS away_rebound_margin

FROM core.events e
JOIN core.teams t_home ON t_home.team_id = e.home_team_id
JOIN core.teams t_away ON t_away.team_id = e.away_team_id
WHERE e.sport_id = 1;  -- NBA