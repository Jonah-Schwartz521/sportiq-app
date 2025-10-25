-- Seed minimal reference data
INSERT INTO core.sports (key, name) VALUES
('nba','Basketball (NBA)')
ON CONFLICT DO NOTHING;

INSERT INTO core.sports (key, name) VALUES
('ufc','Mixed Martial Arts (UFC)')
ON CONFLICT DO NOTHING;

-- Minimal NBA demo teams
INSERT INTO core.teams (sport_id, ext_ref, name, abbrev)
SELECT s.sport_id, 'LAL', 'Los Angeles Lakers', 'LAL' FROM core.sports s WHERE s.key='nba'
ON CONFLICT DO NOTHING;

INSERT INTO core.teams (sport_id, ext_ref, name, abbrev)
SELECT s.sport_id, 'BOS', 'Boston Celtics', 'BOS' FROM core.sports s WHERE s.key='nba'
ON CONFLICT DO NOTHING;

-- Minimal demo event
INSERT INTO core.events (sport_id, season, date, home_team_id, away_team_id, venue, status)
SELECT s.sport_id, 2024, CURRENT_DATE, t1.team_id, t2.team_id, 'Demo Arena', 'scheduled'
FROM core.sports s
JOIN core.teams t1 ON t1.sport_id = s.sport_id AND t1.abbrev='LAL'
JOIN core.teams t2 ON t2.sport_id = s.sport_id AND t2.abbrev='BOS'
WHERE s.key='nba'
ON CONFLICT DO NOTHING;

-- Minimal UFC demo fighters
INSERT INTO ufc.fighters (name, reach_cm, stance) VALUES ('Fighter A', 188, 'Orthodox');
INSERT INTO ufc.fighters (name, reach_cm, stance) VALUES ('Fighter B', 183, 'Southpaw');

-- Minimal UFC event (re-using core.events with sport_id = UFC)
INSERT INTO core.events (sport_id, season, date, home_team_id, away_team_id, venue, status)
SELECT s.sport_id, 2024, CURRENT_DATE, NULL, NULL, 'Demo Octagon', 'scheduled'
FROM core.sports s WHERE s.key='ufc';

-- Minimal bout
INSERT INTO ufc.bouts (event_id, fighter_a_id, fighter_b_id, result)
SELECT e.event_id, 1, 2, NULL FROM core.events e
JOIN core.sports s ON e.sport_id = s.sport_id
WHERE s.key='ufc' ORDER BY e.event_id DESC LIMIT 1;
