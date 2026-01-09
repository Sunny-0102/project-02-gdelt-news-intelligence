# Project 02 — GDELT News Intelligence (BigQuery + Python + Tableau)

## Goal
Build a large-scale, reproducible pipeline that pulls global news-derived event signals from GDELT in BigQuery, cleans and aggregates them, produces analysis + visualizations, and then publishes a Tableau dashboard.

## Data source
- BigQuery public project: `gdelt-bq`
- Dataset: `gdeltv2`
- Table used: `gdelt-bq.gdeltv2.events_partitioned`

This dataset is designed to be analyzed at scale in BigQuery rather than downloaded in full. Only aggregated extracts are pulled locally for modeling and visualization.

## Cost control rule
All BigQuery queries in this repo filter `_PARTITIONTIME` with constant timestamps so partition pruning works and query cost stays controlled.

## Repo structure
- `src/` scripts (extract, clean, visualize)
- `reports/figures/` saved PNG charts
- `reports/` markdown reports
- `data/extracts/` local BigQuery extracts (ignored by git)
- `data/processed/` cleaned outputs (ignored by git)

## Setup (macOS)
```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
Authentication (BigQuery)
bash
Copy code
gcloud auth login
gcloud auth application-default login
gcloud config set project gen-lang-client-0366281238
gcloud auth application-default set-quota-project gen-lang-client-0366281238
gcloud services enable bigquery.googleapis.com
Pipeline (run in order)
bash
Copy code
python src/bq_smoke_test.py
python src/extract_events_daily.py
python src/clean_events_daily.py
python src/viz_overview.py
Tableau
Tableau deliverables will be stored under tableau/ with connection/refresh notes.

## Visualizations (auto-saved to `reports/figures/`)

### Global Event Volume Over Time
![Global Event Volume](reports/figures/01_global_event_volume_over_time.png)

### Top 15 Countries by Total Event Count
![Top Countries](reports/figures/02_top_countries_by_event_count.png)

### Event-Weighted Tone Distribution
![Tone Distribution](reports/figures/03_weighted_tone_distribution.png)

### Top Event Root Categories
![Top Categories](reports/figures/04_top_event_root_categories.png)

### Monthly Activity Heatmap (Top Categories)
![Monthly Heatmap](reports/figures/05_monthly_root_category_heatmap.png)

### Tone by Event Root Category
![Tone by Category](reports/figures/06_tone_by_root_category_boxplot.png)
