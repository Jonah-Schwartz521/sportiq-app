#!/bin/zsh

# Load your zsh configuration so environment variables (like BALLDONTLIE_API_KEY) are available
source ~/.zshrc

# Navigate to your SportIQ project on the external drive
cd /Volumes/easystore/Projects/sportiq-app || exit 1

# Run the NBA daily update script using the venv's Python
# Output (stdout + stderr) is appended to your log file
./.venv/bin/python model/scripts/update_daily_games.py >> logs/nba_update.log 2>&1