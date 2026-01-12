from google.cloud import bigquery

BILLING_PROJECT = "gen-lang-client-0366281238"

# Public GDELT table we will query
TABLE = "gdelt-bq.gdeltv2.events_partitioned"


def main() -> None:
    # BigQuery client uses Application Default Credentials automatically
    client = bigquery.Client(project=BILLING_PROJECT)

    # Keep the date range small so this stays fast and cheap
    # We filter on _PARTITIONTIME so BigQuery scans only the selected day partition
    query = f"""
    SELECT
      SQLDATE,
      Actor1CountryCode,
      Actor2CountryCode,
      EventCode,
      EventRootCode,
      GoldsteinScale,
      NumMentions,
      NumSources,
      NumArticles,
      AvgTone
    FROM `{TABLE}`
    WHERE _PARTITIONTIME >= TIMESTAMP('2025-12-01')
      AND _PARTITIONTIME <  TIMESTAMP('2025-12-02')
    LIMIT 10
    """

    df = client.query(query).to_dataframe()
    print(df)


if __name__ == "__main__":
    main()
