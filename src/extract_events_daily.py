from pathlib import Path

import pandas as pd
from google.cloud import bigquery



BILLING_PROJECT = "gen-lang-client-0366281238"

# Public GDELT partitioned Events table
TABLE = "gdelt-bq.gdeltv2.events_partitioned"


START = "2025-10-01"
END = "2026-01-01"


def main() -> None:
    
    client = bigquery.Client(project=BILLING_PROJECT)

    # Save extracts locally (ignored by git)
    root = Path(__file__).resolve().parents[1]
    out_dir = root / "data" / "extracts"
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"events_daily_{START.replace('-','')}_{END.replace('-','')}.csv"

    # Aggregate inside BigQuery (massive tables) and download only the result
    # Partition filter uses constant timestamps for pruning
    query = f"""
    SELECT
      SQLDATE,
      ActionGeo_CountryCode AS CountryCode,
      EventRootCode,
      COUNT(1) AS EventCount,
      AVG(AvgTone) AS AvgTone,
      AVG(GoldsteinScale) AS AvgGoldstein,
      SUM(NumMentions) AS TotalMentions,
      SUM(NumArticles) AS TotalArticles,
      SUM(NumSources) AS TotalSources
    FROM `{TABLE}`
    WHERE _PARTITIONTIME >= TIMESTAMP('{START}')
      AND _PARTITIONTIME <  TIMESTAMP('{END}')
      AND ActionGeo_CountryCode IS NOT NULL
    GROUP BY SQLDATE, CountryCode, EventRootCode
    ORDER BY SQLDATE, CountryCode, EventRootCode
    """

    df = client.query(query).to_dataframe()
    df.to_csv(out_path, index=False)

    print(f"Saved: {out_path}")
    print("Rows:", len(df))
    print(df.head())


if __name__ == "__main__":
    main()
