import pandas as pd

from model_api.main import build_feature_insights, InsightItem


def make_base_row():
    # minimal row with all needed columns
    return pd.Series(
        {
            "home_team": "Home",
            "away_team": "Away",
            "home_season_win_pct": 0.5,
            "away_season_win_pct": 0.5,
            "home_recent_win_pct_20g": 0.5,
            "away_recent_win_pct_20g": 0.5,
            "home_days_rest": 2.0,
            "away_days_rest": 2.0,
            "home_b2b": 0.0,
            "away_b2b": 0.0,
            "home_last_pd": 0.0,
            "away_last_pd": 0.0,
        }
    )


def test_season_strength_home_better():
    row = make_base_row()
    row["home_season_win_pct"] = 0.70
    row["away_season_win_pct"] = 0.50

    insights = build_feature_insights(row)
    types = {i.type for i in insights}

    assert "season_strength" in types
    text = next(i.detail for i in insights if i.type == "season_strength")
    assert "Home" in text
    assert "stronger season" in text


def test_season_strength_away_better():
    row = make_base_row()
    row["home_season_win_pct"] = 0.40
    row["away_season_win_pct"] = 0.65

    insights = build_feature_insights(row)
    types = {i.type for i in insights}

    assert "season_strength" in types
    text = next(i.detail for i in insights if i.type == "season_strength")
    assert "Away" in text


def test_rest_advantage():
    row = make_base_row()
    row["home_days_rest"] = 4.0
    row["away_days_rest"] = 1.0

    insights = build_feature_insights(row)
    types = {i.type for i in insights}

    assert "rest" in types
    text = next(i.detail for i in insights if i.type == "rest")
    assert "more rested (+3" in text  # 3 days


def test_back_to_back_flags():
    row = make_base_row()
    row["home_b2b"] = 1.0
    row["away_b2b"] = 1.0

    insights = build_feature_insights(row)
    types = [i.type for i in insights]

    assert types.count("fatigue") == 2


def test_momentum_home():
    row = make_base_row()
    row["home_last_pd"] = 1.0
    row["away_last_pd"] = 0.0

    insights = build_feature_insights(row)
    types = {i.type for i in insights}

    assert "momentum" in types
    text = next(i.detail for i in insights if i.type == "momentum")
    assert "Home" in text