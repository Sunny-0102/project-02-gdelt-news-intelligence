from google.cloud import bigquery

BILLING_PROJECT = "gen-lang-client-0366281238"
SOURCE = f"{BILLING_PROJECT}.gdelt_portfolio.events_daily_clean"
DEST = f"{BILLING_PROJECT}.gdelt_portfolio.country_risk_daily"


def main() -> None:
    # This builds a stable derived table that Tableau and modeling can reuse without reprocessing raw data.
    client = bigquery.Client(project=BILLING_PROJECT)

    # This risk table is intentionally simple and transparent:
    # - Conflict events = selected CAMEO root codes (protest/military/coerce/assault/fight/mass violence).
    # - Negative tone events = rows with AvgTone <= -2, weighted by EventCount.
    # - risk_raw blends conflict share, negative share, and overall volume (log-scaled).
    query = f"""
    CREATE OR REPLACE TABLE `{DEST}` AS
    WITH base AS (
      SELECT
        DATE(date) AS date,
        CountryCode,
        LPAD(CAST(EventRootCode AS STRING), 2, '0') AS EventRootCode,
        CAST(EventCount AS INT64) AS EventCount,
        CAST(AvgTone AS FLOAT64) AS AvgTone
      FROM `{SOURCE}`
      WHERE CountryCode IS NOT NULL
        AND date IS NOT NULL
    ),
    agg AS (
      SELECT
        date,
        CountryCode,
        SUM(EventCount) AS total_events,
        SUM(IF(EventRootCode IN ('14','15','16','17','18','19','20'), EventCount, 0)) AS conflict_events,
        SUM(IF(AvgTone <= -2, EventCount, 0)) AS negative_tone_events,
        SAFE_DIVIDE(SUM(AvgTone * EventCount), SUM(EventCount)) AS weighted_avg_tone
      FROM base
      GROUP BY date, CountryCode
    )
    SELECT
      date,
      CountryCode,
      total_events,
      conflict_events,
      negative_tone_events,
      weighted_avg_tone,
      SAFE_DIVIDE(conflict_events, total_events) AS conflict_share,
      SAFE_DIVIDE(negative_tone_events, total_events) AS negative_share,
      LOG(1 + total_events) AS log_events,
      (
        1.5 * SAFE_DIVIDE(conflict_events, total_events)
        + 1.0 * SAFE_DIVIDE(negative_tone_events, total_events)
        + 0.5 * LOG(1 + total_events)
      ) AS risk_raw
    FROM agg
    ORDER BY date, CountryCode
    """

    client.query(query).result()
    print(f"Created: {DEST}")


if __name__ == "__main__":
    main()
