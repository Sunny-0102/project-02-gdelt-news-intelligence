# Tableau (BigQuery) — Project 02: GDELT News Intelligence

This folder contains the Tableau workbook(s), screenshots, and the exact BigQuery tables/relationships used by each dashboard.

---

## 1) What’s in this folder

- **Workbook(s)**: `.twb` or `.twbx`
- **Screenshots**: `tableau/screenshots/*.png` (used in the root README)
- **Tableau notes**: connection, refresh, troubleshooting

Recommended structure:
- `tableau/gdelt_global_pulse.twb` (or `.twbx`)
- `tableau/screenshots/`
  - `gdelt_global_pulse.png`
  - `topic_pulse.png`
  - `risk_monitor_2.png`
  - `forecast_outlook.png`

---

## 2) BigQuery tables used (authoritative)

BigQuery dataset:
- `gen-lang-client-0366281238.gdelt_portfolio`

Tables:
- **V1 (Global Pulse)**: `events_daily_clean`
- **V2 (Topic Pulse)**: `gkg_theme_daily_YYYYMMDD_YYYYMMDD` (example: `gkg_theme_daily_20251001_20260101`)
- **V3 (Risk Monitor)**: `country_risk_daily`
- **V3 (Forecast Outlook)**: `country_risk_forecasts_next_day`

---

## 3) Connection options (Tableau Desktop → BigQuery)

### Option A (recommended for shareable workbooks): Service Account JSON
Use this when you want the workbook to refresh without relying on your personal Google login.

High-level steps:
1) **Connect → Google BigQuery**
2) **Sign In** → choose **Service Account** (JSON)
3) Select:
   - Project: `gen-lang-client-0366281238`
   - Dataset: `gdelt_portfolio`
   - Table(s): see section 2

### Option B (simple for personal use): OAuth sign-in
Use this for your own machine (Tableau will use your Google login).
1) **Connect → Google BigQuery**
2) **Sign In with Google**
3) Select the project/dataset/table(s)

---

## 4) V3 relationship (critical)

When using **country_risk_daily** + **country_risk_forecasts_next_day** together:

Relationship keys:
- `CountryCode` (daily) = `Country Code` (forecast)
- `Date` (daily) = `As Of Date` (forecast)

Why:
- The forecast is for the *next day*, but it is anchored to the last *observed* day so the forecast point always “sits” on a real day in the timeline.

---

## 5) Refresh checklist (do this after you re-run the pipeline)

1) Run the pipeline refresh (see `docs/OPERATIONS.md`)
2) Open Tableau workbook
3) **Data Source → Refresh**
4) If a table is missing:
   - Go to **Data Source** tab
   - Click the **refresh** icon for the BigQuery connection
   - Re-select dataset `gdelt_portfolio`
   - Use search in the table list (type `risk` or `forecast`)

---

## 6) Common Tableau issues (fast fixes)

### A) Forecast table not visible in Tableau
- Confirm it exists in BigQuery: `gdelt_portfolio.country_risk_forecasts_next_day`
- In Tableau Data Source:
  - Refresh connection
  - Re-open dataset dropdown
  - Search “forecast”

### B) “Risk Trend + Forecast” becomes blank on the dashboard
Usually caused by dashboard filters being applied to the wrong sheets/table context.

Fix:
1) Confirm the worksheet renders by itself (open the worksheet tab).
2) In the dashboard, set filters using:
   - **Apply to Worksheets → Selected Worksheets**
3) Apply:
   - **Date** filter → only to sheets built from `country_risk_daily`
   - **Latest Run Date** (forecast) filter → only to sheets using the forecast table

### C) KPI sheets show blank (no number)
Most common causes:
- KPI field not placed on **Marks → Text**
- Date filter excludes the latest day in range
- Wrong aggregation (KPI should typically use `MAX(...)`, not `SUM(...)`)

---

## 7) Notes on cost
This repo is designed so heavy work happens in BigQuery with partition pruning, and Tableau reads from curated tables in `gdelt_portfolio` rather than querying raw GDELT directly.
