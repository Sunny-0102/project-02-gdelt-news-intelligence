# Final Report — GDELT News Intelligence

## What this project does
This project builds a reproducible pipeline to query global news-derived event signals from GDELT in BigQuery, generate an analytical dataset, produce Python visualizations, detect anomalies, and deliver an interactive Tableau dashboard.

## Data source
- BigQuery public project: `gdelt-bq`
- Dataset: `gdeltv2`
- Table: `gdelt-bq.gdeltv2.events_partitioned`

GDELT is too large to use as a full local download, so the pipeline extracts an aggregated slice for analysis. :contentReference[oaicite:3]{index=3}

## Cost control
All BigQuery queries filter `_PARTITIONTIME` using constant timestamps to ensure partition pruning and avoid scanning unnecessary partitions. :contentReference[oaicite:4]{index=4}

## Pipeline outputs
- Local extract: `data/extracts/events_daily_20251001_20260101.csv` (ignored by git)
- Clean dataset: `data/processed/events_daily_clean.parquet` (ignored by git)
- Data quality report: `reports/data_quality_events_daily.md`
- Python charts: `reports/figures/`
- Anomaly report: `reports/anomalies/top_50_country_day_anomalies.csv`
- Tableau dashboard:
  - Workbook: `tableau/gdelt_global_pulse.twb`
  - Screenshot: `tableau/screenshots/gdelt_global_pulse.png`

## Key visuals
![Global Event Volume](figures/01_global_event_volume_over_time.png)
![Top Countries](figures/02_top_countries_by_event_count.png)
![Top Categories](figures/04_top_event_root_categories.png)
![Anomalies](figures/07_anomalies_top_country_eventcount.png)

## Modeling
Unsupervised anomaly detection is applied to country-day signals using rolling z-score features and Isolation Forest to flag unusual days. :contentReference[oaicite:5]{index=5}

## Tableau deliverable
The Tableau dashboard connects to `gen-lang-client-0366281238.gdelt_portfolio.events_daily_clean` and supports interactive filtering by date, country, and event category. :contentReference[oaicite:6]{index=6}
