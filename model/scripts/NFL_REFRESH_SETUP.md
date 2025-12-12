# NFL Scores Nightly Refresh - Setup Guide

## What This Does

Automatically refreshes NFL game scores every night using **nfl_data_py** (sourced from nflverse/nflfastR). The job:
- Runs at **3:30 AM Denver time** (10:30 AM UTC)
- Fetches yesterday + today's games
- Updates scores for games that have finished
- Commits the updated parquet file back to the repo (only if changed)
- **No API keys required** (nfl_data_py is free and open source)

---

## Setup

### 1. Verify Workflow File

The workflow is already in place at:
```
.github/workflows/refresh_nfl_scores.yml
```

It will run automatically every night. No secrets or API keys needed!

### 2. Test Manually in GitHub Actions

1. Go to: **Actions** → **Nightly NFL Scores Refresh**
2. Click **Run workflow** dropdown (top right)
3. Click the green **Run workflow** button
4. Monitor the job logs to verify it completes successfully

Expected output:
- Seasons fetched (current + previous season)
- Games filtered to yesterday/today
- Number of new/updated games
- Parquet file committed if changes detected

---

## Local Testing

### Run the Script Locally

```bash
# Install dependencies
pip install pandas pyarrow nfl-data-py

# Run the refresh script
python model/scripts/refresh_nfl_scores.py
```

Check the output file:
```
model/data/processed/nfl/nfl_games_with_scores.parquet
```

---

## How It Works

### Schedule
- Runs at **3:30 AM Denver time** (10:30 AM UTC)
- Fetches games from **yesterday + today** (to catch late finishes and timezone edge cases)

### Data Source
- Uses `nfl_data_py.import_schedules()` to get NFL schedule data
- Fetches current season + previous season (to handle year boundaries)
- Filters down to only games in the date window
- **No play-by-play data** (keeps it lightweight and fast)

### Data Flow
1. Fetch schedules for current and previous NFL seasons
2. Filter to games scheduled for yesterday or today
3. Normalize into consistent schema
4. Upsert into local parquet file (merges on `game_id`)
5. Commit updated parquet back to repo (only if changed)

### Error Handling
- Retries schedule fetch up to 3 times
- Fails loudly with clear error messages
- Logs detailed output for debugging

---

## Output Schema

The parquet file contains:

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | string | Unique game identifier (from nfl_data_py) |
| `sport` | string | Always "NFL" |
| `season` | int | Season year (e.g., 2024) |
| `week` | int | Week number |
| `game_date` | string | Game date (YYYY-MM-DD) |
| `game_datetime_utc` | string | ISO datetime (currently null, can be enhanced) |
| `home_team` | string | Home team abbreviation (e.g., "DEN") |
| `away_team` | string | Away team abbreviation |
| `home_score` | int | Final home score (null if not finished) |
| `away_score` | int | Final away score (null if not finished) |
| `status` | string | "scheduled" or "final" |
| `neutral_site` | bool | True if played at neutral site |
| `postseason` | bool | True if playoff game |
| `source` | string | Always "nfl_data_py" |

---

## Monitoring

### Check Workflow Runs
Go to **Actions** tab to see:
- Last run status
- Detailed logs
- Any errors

### Expected Behavior
- **Games found**: Workflow commits updated parquet
- **No games found**: Workflow completes with "No changes detected" message
- **Fetch error**: Workflow fails with clear error message after 3 retry attempts

---

## Troubleshooting

### "No games found in date window"
→ Normal during off-season or bye weeks. The workflow will still succeed.

### "Failed to fetch schedules after 3 attempts"
→ Check if nflverse data sources are accessible. This is rare but can happen if upstream sources are down.

### Parquet file not updating
→ Check the workflow logs to see if any games were actually in the date window (yesterday + today in Denver time).

### Local run fails with "nfl_data_py is not installed"
→ Run: `pip install nfl-data-py`

---

## Differences from NBA Refresh

| Feature | NBA | NFL |
|---------|-----|-----|
| **Data source** | BallDontLie API | nfl_data_py package |
| **API key** | Required (GitHub secret) | Not required |
| **Seasons fetched** | Current season | Current + previous season |
| **Date filtering** | API-side | Client-side (after fetch) |
| **Postseason flag** | From API | Derived from `game_type` field |

---

## Next Steps

Once running, you can:
1. Read the parquet file in your app (no live API calls needed)
2. Monitor the Actions tab for nightly run status
3. Manually trigger refreshes anytime via `workflow_dispatch`
4. Extend the date window if needed (edit `refresh_nfl_scores.py` lines with `get_date_window()`)

---

## Data Source Credits

This refresh uses **nfl_data_py**, which sources data from:
- [nflverse](https://github.com/nflverse)
- [nflfastR](https://www.nflfastr.com/)

All NFL data is freely available and does not require API authentication.
