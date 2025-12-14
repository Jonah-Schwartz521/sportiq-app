#!/usr/bin/env python3
"""Debug script to check API responses for model_snapshot and odds."""

import requests
import json

API_BASE = "http://127.0.0.1:8000"

def check_nfl_response():
    """Check NFL API response for model_snapshot."""
    print("="*80)
    print("CHECKING NFL API RESPONSE")
    print("="*80)

    url = f"{API_BASE}/events?sport_id=3&limit=5"
    print(f"\nRequesting: {url}\n")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        print(f"Received {len(items)} NFL games\n")

        for i, event in enumerate(items, 1):
            print(f"Game {i}:")
            print(f"  event_id: {event.get('event_id')}")
            print(f"  sport_id: {event.get('sport_id')}")
            print(f"  teams: {event.get('away_team')} @ {event.get('home_team')}")
            print(f"  date: {event.get('date')}")
            print(f"  model_snapshot: {event.get('model_snapshot')}")

            if event.get('model_snapshot'):
                snapshot = event['model_snapshot']
                print(f"    ✅ Has model_snapshot!")
                print(f"       source: {snapshot.get('source')}")
                print(f"       p_home_win: {snapshot.get('p_home_win')}")
                print(f"       p_away_win: {snapshot.get('p_away_win')}")
            else:
                print(f"    ❌ NO model_snapshot")
            print()

        # Count coverage
        with_snapshot = sum(1 for e in items if e.get('model_snapshot'))
        print(f"Summary: {with_snapshot}/{len(items)} games have model_snapshot")

    except Exception as e:
        print(f"❌ ERROR: {e}")


def check_nba_response():
    """Check NBA API response for odds fields."""
    print("\n" + "="*80)
    print("CHECKING NBA API RESPONSE")
    print("="*80)

    url = f"{API_BASE}/events?sport_id=1&limit=5"
    print(f"\nRequesting: {url}\n")

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()

        items = data.get("items", [])
        print(f"Received {len(items)} NBA games\n")

        for i, event in enumerate(items, 1):
            print(f"Game {i}:")
            print(f"  event_id: {event.get('event_id')}")
            print(f"  sport_id: {event.get('sport_id')}")
            print(f"  teams: {event.get('away_team')} @ {event.get('home_team')}")
            print(f"  date: {event.get('date')}")
            print(f"  model_home_win_prob: {event.get('model_home_win_prob')}")
            print(f"  model_away_win_prob: {event.get('model_away_win_prob')}")
            print(f"  model_home_american_odds: {event.get('model_home_american_odds')}")
            print(f"  model_away_american_odds: {event.get('model_away_american_odds')}")
            print(f"  sportsbook_home_american_odds: {event.get('sportsbook_home_american_odds')}")
            print(f"  sportsbook_away_american_odds: {event.get('sportsbook_away_american_odds')}")
            print(f"  model_snapshot: {event.get('model_snapshot')}")

            has_model_odds = event.get('model_home_american_odds') is not None
            has_sportsbook_odds = event.get('sportsbook_home_american_odds') is not None

            if has_model_odds:
                print(f"    ✅ Has model odds")
            else:
                print(f"    ⚠️  No model odds")

            if has_sportsbook_odds:
                print(f"    ✅ Has sportsbook odds")
            else:
                print(f"    ℹ️  No sportsbook odds (may be expected)")
            print()

        # Count coverage
        with_model = sum(1 for e in items if e.get('model_home_american_odds') is not None)
        print(f"Summary: {with_model}/{len(items)} games have model odds")

    except Exception as e:
        print(f"❌ ERROR: {e}")


if __name__ == "__main__":
    check_nfl_response()
    check_nba_response()
