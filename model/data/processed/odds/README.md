# Odds Data

This directory contains betting odds data fetched from [The Odds API](https://the-odds-api.com/).

## Files

- `nba_odds.parquet` - NBA moneyline and spread odds
- `nfl_odds.parquet` - NFL moneyline and spread odds

## Schema

| Column | Type | Description |
|--------|------|-------------|
| `sport` | string | Sport identifier (nba, nfl) |
| `commence_time_utc` | datetime | Game start time (UTC) |
| `home_team` | string | Home team name |
| `away_team` | string | Away team name |
| `bookmaker` | string | Bookmaker identifier (fanduel, draftkings, etc.) |
| `market` | string | Market type (h2h, spreads) |
| `outcome_name` | string | Team or outcome name |
| `outcome_price` | float | American odds (e.g., -110, +150) |
| `point` | float | Point spread (null for moneyline) |
| `last_update_utc` | datetime | Last update time from bookmaker |
| `source` | string | Data source (the-odds-api) |

## Data Freshness

- **Refresh frequency**: Nightly at 4:00 AM Denver time
- **Time window**: Next 24 hours of upcoming games
- **Markets**: Moneyline (h2h) and Spreads only
- **Bookmakers**: FanDuel, DraftKings, BetMGM, PointsBet, Bovada

## Optimization Notes

To minimize API usage on the free tier (500 requests/month):
- Single bulk endpoint per sport (not per-game)
- Limited to 5 bookmakers
- Only 2 markets (h2h + spreads)
- 24-hour time window
- ~2 API calls per run (1 per sport)
- ~60 API calls per month

## Update Logic

Files are updated **idempotently** using upsert logic:
- **Composite key**: `(commence_time_utc, home_team, away_team, bookmaker, market, outcome_name, point)`
- **Conflict resolution**: Keep row with most recent `last_update_utc`
- **Behavior**: Merges new data with existing, updates changed odds, preserves historical data

## API Key

The odds refresh script requires `ODDS_API_KEY` environment variable:

```bash
# Local testing
export ODDS_API_KEY="your-api-key-here"
python model/scripts/refresh_odds.py
```

In GitHub Actions, the key is stored as a repository secret named `ODDS_API_KEY`.

## Example Queries

```python
import pandas as pd

# Load odds
nba_odds = pd.read_parquet("model/data/processed/odds/nba_odds.parquet")

# Get latest moneyline for a specific game
game_odds = nba_odds[
    (nba_odds["home_team"] == "Los Angeles Lakers") &
    (nba_odds["away_team"] == "Boston Celtics") &
    (nba_odds["market"] == "h2h")
]

# Get average spread across bookmakers
avg_spread = nba_odds[
    (nba_odds["market"] == "spreads") &
    (nba_odds["outcome_name"] == "Los Angeles Lakers")
].groupby("commence_time_utc")["point"].mean()

# Compare odds across bookmakers
fanduel_odds = nba_odds[nba_odds["bookmaker"] == "fanduel"]
draftkings_odds = nba_odds[nba_odds["bookmaker"] == "draftkings"]
```

## Workflow

The nightly refresh is managed by `.github/workflows/refresh_odds.yml`:

1. Fetches latest odds from The Odds API
2. Upserts into existing parquet files
3. Creates a PR on branch `bot/odds-refresh`
4. If no changes detected, no PR is created

To trigger manually:
```bash
gh workflow run "Nightly Odds Refresh"
```
