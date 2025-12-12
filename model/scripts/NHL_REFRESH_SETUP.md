# NHL Scores Nightly Refresh - Setup Guide

## What This Does

Automatically refreshes NHL game scores every night using the **NHL Web API** (https://api-web.nhle.com/). The job:
- Runs at **3:30 AM Denver time** (10:30 AM UTC)
- Fetches yesterday + today's games
- Updates scores for games that have finished
- Creates/updates a pull request with the new data
- **No API keys required** (uses public NHL Web API)

---

## Setup

### 1. Verify Workflow File

The workflow is already in place at:
```
.github/workflows/refresh_nhl_scores.yml
```

It will run automatically every night. No secrets or API keys needed!

### 2. Test Manually in GitHub Actions

1. Go to: **Actions** → **Nightly NHL Scores Refresh**
2. Click **Run workflow** dropdown (top right)
3. Click the green **Run workflow** button
4. Monitor the job logs to verify it completes successfully

Expected output:
- Date window (yesterday + today)
- Games fetched per day
- Number of new/updated games
- PR created/updated if changes detected

---

## Local Testing

### Run the Script Locally

```bash
# Install dependencies
pip install pandas pyarrow requests

# Run the refresh script
python model/scripts/refresh_nhl_scores.py
```

Check the output file:
```
model/data/processed/nhl/nhl_games_with_scores.parquet
```

---

## How It Works

### Schedule
- Runs at **3:30 AM Denver time** (10:30 AM UTC)
- Fetches games from **yesterday + today** (to catch late finishes and timezone edge cases)

### Data Source
- Uses NHL Web API: `https://api-web.nhle.com/v1/score/{date}`
- Fetches scores for each date in the window
- Parses game data and normalizes into consistent schema
- **No authentication required** (public API)

### Data Flow
1. Fetch scores for yesterday and today via NHL Web API
2. Normalize into consistent schema
3. Upsert into local parquet file (merges on `game_id`)
4. Create/update PR to `bot/nhl-scores-refresh` branch
5. PR requires manual review and merge to update main

### Error Handling
- Retries API calls up to 3 times with exponential backoff
- Fails loudly with clear error messages
- Logs detailed output for debugging

---

## Output Schema

The parquet file contains:

| Column | Type | Description |
|--------|------|-------------|
| `game_id` | string | Unique game identifier (from NHL API) |
| `sport` | string | Always "NHL" |
| `season` | int | Season year (e.g., 2024 for 2024-25 season) |
| `game_date` | string | Game date (YYYY-MM-DD) |
| `game_datetime_utc` | string | ISO datetime (if available) |
| `home_team` | string | Home team abbreviation (e.g., "COL") |
| `away_team` | string | Away team abbreviation |
| `home_score` | int | Final home score (null if not finished) |
| `away_score` | int | Final away score (null if not finished) |
| `status` | string | "scheduled", "in_progress", or "final" |
| `neutral_site` | bool | True if neutral site (null if unknown) |
| `postseason` | bool | True if playoff game |
| `source` | string | Always "nhl_api_web" |

### Season Logic
- NHL season runs October (year N) → June (year N+1)
- Season year = year the season started
- Example: Oct 2024 → June 2025 = **2024** season

---

## Monitoring

### Check Workflow Runs
Go to **Actions** tab to see:
- Last run status
- Detailed logs
- Any errors

### Expected Behavior
- **Games found**: Workflow creates/updates PR to `bot/nhl-scores-refresh`
- **No games found**: Workflow completes with "No changes detected" message
- **Fetch error**: Workflow fails with clear error message after 3 retry attempts

---

## Troubleshooting

### "No games found in date window"
→ Normal during off-season (July-September). The workflow will still succeed.

### "Failed to fetch scores after 3 attempts"
→ Check if NHL API is accessible:
```bash
curl https://api-web.nhle.com/v1/score/2024-12-11
```
If it fails, the API may be temporarily down.

### Parquet file not updating
→ Check the workflow logs to see if any games were actually in the date window (yesterday + today in Denver time).

### API returns unexpected JSON structure
→ NHL API occasionally changes format. Check the raw API response and update the normalization logic in `refresh_nhl_scores.py` if needed.

### Local run fails with "zoneinfo not found" (Python < 3.9)
→ Upgrade to Python 3.9+ or install `backports.zoneinfo`:
```bash
pip install backports.zoneinfo
```

---

## Differences from NBA/NFL Refresh

| Feature | NBA | NFL | NHL |
|---------|-----|-----|-----|
| **Data source** | BallDontLie API | nfl_data_py | NHL Web API |
| **API key** | Required | Not required | Not required |
| **Season logic** | Current year | Aug-Feb span | Oct-Jun span |
| **Postseason flag** | From API | From game_type | From game_type |
| **Team names** | Abbreviations | Abbreviations | Abbreviations |

---

## Next Steps

Once running, you can:
1. Review PRs in the **Pull requests** tab (look for `bot/nhl-scores-refresh`)
2. Merge PRs to update main with latest NHL scores
3. Monitor the Actions tab for nightly run status
4. Manually trigger refreshes anytime via `workflow_dispatch`
5. Read the parquet file in your app (no live API calls needed)

---

## Data Source Credits

This refresh uses the **NHL Web API**, a public API provided by the National Hockey League:
- Endpoint: https://api-web.nhle.com/
- No authentication required
- Free to use for non-commercial purposes
