# Tableau (BigQuery)

This folder will hold the Tableau workbook(s) and refresh notes for this project.

## Connection option A (recommended for shareable workbooks): Service Account JSON
Tableau Desktop can authenticate to BigQuery using a Service Account JSON file.
Use this when you want the workbook to refresh without relying on your personal Google login. :contentReference[oaicite:0]{index=0}

High-level steps in Tableau Desktop:
1) Connect → Google BigQuery
2) Authentication → Sign in using Service Account (JSON)
3) Select the project + dataset + table/view, then build worksheets/dashboards :contentReference[oaicite:1]{index=1}

## Connection option B (simple for personal use): OAuth sign-in
Tableau Desktop can also connect via OAuth by signing into your Google account. :contentReference[oaicite:2]{index=2}

## What Tableau will use from this repo
Preferred: a BigQuery view/table built from partition-filtered extracts (so queries stay cheap).
We already use partition filters in Python extracts; the same principle applies for Tableau-backed tables/views. :contentReference[oaicite:3]{index=3}

## Files to store here
- Workbook: `.twb` or `.twbx`
- Screenshots: `png` (for README / report)
- Notes: how to refresh and what tables/views the workbook uses
