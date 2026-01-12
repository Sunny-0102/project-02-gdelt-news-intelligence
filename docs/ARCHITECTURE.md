# Architecture — Project 02: GDELT News Intelligence

## 1) System overview
This project builds a reproducible analytics + forecasting pipeline on top of the public GDELT 2.1 dataset in BigQuery, and serves Tableau dashboards from curated tables in a personal BigQuery dataset.

**Flow**
1. BigQuery (public): `gdelt-bq.gdeltv2.events_partitioned`
2. Python extract: daily country × event-root aggregates → `data/extracts/*.csv`
3. Python clean/enrich → `data/processed/*.parquet` + QA report
4. Publish curated table to BigQuery → `gdelt_portfolio.events_daily_clean`
5. Derive daily risk signals → `gdelt_portfolio.country_risk_daily`
6. Train + publish next-day forecasts → `gdelt_portfolio.country_risk_forecasts_next_day`
7. Tableau reads curated tables (Live connection) for dashboards

## 2) Data model (BigQuery tables)
### A) events_daily_clean
Grain: **(date, CountryCode, EventRootCode)**  
Key columns:
- EventCount, TotalArticles, TotalSources, TotalMentions
- AvgTone, AvgGoldstein
- EventRootLabel, ToneBucket

### B) country_risk_daily
Grain: **(date, CountryCode)**  
Derived features:
- total_events, conflict_events, conflict_share
- negative_share, weighted_avg_tone
- risk_raw (interpretable composite score)

### C) country_risk_forecasts_next_day
Grain: **(run_date, as_of_date, forecast_date, CountryCode)**  
Key columns:
- risk_as_of (actual on as_of_date)
- pred_risk_next_day (prediction for forecast_date)
- predicted delta = pred - actual
- features snapshot (total_events, conflict_share, negative_share, weighted_avg_tone)

## 3) Forecasting design
Model: RandomForestRegressor trained on lag + rolling features per country:
- lags: 1,2,3,7,14
- roll_mean_7, roll_std_7
- plus current daily features (volume/tone/shares)

Validation: time-ordered backtest MAE on last 14 days.

Output: a “latest snapshot” table overwritten each run so Tableau always reads a clean, current dataset.

## 4) Tableau relationship (critical)
Tables:
- `country_risk_daily`
- `country_risk_forecasts_next_day`

Relationship:
- CountryCode (daily) = Country Code (forecast)
- Date (daily) = As Of Date (forecast)

This ensures the forecast dot sits on a real observed day, while the dot’s value is the next-day prediction.

## 5) Cost control
All queries against the public GDELT table must include partition filters so BigQuery prunes partitions (reduces bytes scanned and cost).
