# NBA Scores Nightly Refresh - Setup Guide

## How to Enable

### 1. Add GitHub Secret

1. Go to your repository on GitHub
2. Navigate to: **Settings** → **Secrets and variables** → **Actions**
3. Click **New repository secret**
4. Name: `BALLDONTLIE_API_KEY`
5. Value: Your BallDontLie API key
6. Click **Add secret**

### 2. Verify Workflow File

The workflow file is already in place at:
```
.github/workflows/refresh_nba_scores.yml
```

It will run automatically every night at **3:30 AM Denver time** (10:30 AM UTC).

### 3. Test Manually (Recommended)

Before waiting for the nightly run, test the workflow manually:

1. Go to: **Actions** → **Nightly NBA Scores Refresh**
2. Click **Run workflow** dropdown (top right)
3. Click the green **Run workflow** button
4. Monitor the job logs to verify it completes successfully

Expected output:
- ✓ API key loaded
- Games fetched for yesterday and today
- Parquet file updated and committed back to repo

### 4. Test Locally (Optional)

To test the script on your local machine:

```bash
# Set your API key
export BALLDONTLIE_API_KEY="your-api-key-here"

# Install dependencies
pip install pandas pyarrow requests

# Run the script
python model/scripts/refresh_nba_scores.py
```

Check the output file:
```
model/data/processed/nba/nba_games_with_scores.parquet
```

---

## How It Works

### Schedule
- Runs at **3:30 AM Denver time** (10:30 AM UTC)
- Fetches games from **yesterday + today** (to catch late finishes and timezone edge cases)

### Data Flow
1. Fetches games via BallDontLie API
2. Normalizes into consistent schema
3. Upserts into local parquet file (merges on `game_id`)
4. Commits updated parquet back to repo (only if changed)

### Error Handling
- Retries API calls up to 3 times with exponential backoff
- Fails loudly if `BALLDONTLIE_API_KEY` is missing
- Logs detailed output for debugging

### Output Schema
The parquet file contains:
- `game_id` - Unique game identifier
- `date_utc` - Game date (YYYY-MM-DD)
- `home_team_id`, `home_team_name` - Home team info
- `away_team_id`, `away_team_name` - Away team info
- `home_score`, `away_score` - Final scores (null if not finished)
- `status` - Game status
- `season` - Season year
- `postseason` - Boolean flag

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
- **API error**: Workflow fails with clear error message

### Troubleshooting

**"ERROR: BALLDONTLIE_API_KEY environment variable not set"**
→ Add the secret in GitHub settings (see step 1)

**"Failed to fetch games after 3 attempts"**
→ Check BallDontLie API status or rate limits

**No new scores appearing**
→ Check if games were played in the date window (yesterday + today)

---

## Next Steps

Once enabled, you can:
1. Read the parquet file in your app (no live API calls needed)
2. Monitor the Actions tab for nightly run status
3. Manually trigger refreshes anytime via workflow_dispatch
4. Extend the date window if needed (edit `refresh_nba_scores.py` lines 167-170)
