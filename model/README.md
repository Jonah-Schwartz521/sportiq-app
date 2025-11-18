# SportIQ Modeling

This folder contains the modeling code for SportIQ (win probabilities + insights).

## Goal

Build and evaluate models that predict **home team win probability** for a given game, and expose those predictions to the web app via `/predict` and `/insights`.

## Layout

- `data/`
  - `raw/` — original data dumps (never edited by hand)
  - `interim/` — lightly cleaned / joined tables
  - `processed/` — final modeling tables (one row per game)
- `notebooks/`
  - Exploratory analysis, model experiments, and reports
- `src/`
  - Reusable Python modules (data loading, feature engineering, training, evaluation)