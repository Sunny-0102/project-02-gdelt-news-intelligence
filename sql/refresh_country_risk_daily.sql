-- BigQuery Standard SQL
-- Rebuilds the daily country risk table from `events_daily_clean`.
-- If you change the risk formula in Python, update this file to match.

CREATE OR REPLACE TABLE `gen-lang-client-0366281238.gdelt_portfolio.country_risk_daily` AS
WITH base AS (
  SELECT
    date,
    CountryCode,
    SUM(EventCount) AS total_events,
    SUM(IF(EventRootCode IN ('18','19','20'), EventCount, 0)) AS conflict_events,
    SUM(IF(ToneBucket = 'negative', EventCount, 0)) AS negative_events,
    SAFE_DIVIDE(SUM(IF(EventRootCode IN ('18','19','20'), EventCount, 0)), SUM(EventCount)) AS conflict_share,
    SAFE_DIVIDE(SUM(IF(ToneBucket = 'negative', EventCount, 0)), SUM(EventCount)) AS negative_share,
    SAFE_DIVIDE(SUM(AvgTone * EventCount), SUM(EventCount)) AS weighted_avg_tone
  FROM `gen-lang-client-0366281238.gdelt_portfolio.events_daily_clean`
  WHERE date IS NOT NULL AND CountryCode IS NOT NULL
  GROUP BY date, CountryCode
),
scored AS (
  SELECT
    *,
    -- A simple interpretable risk score:
    -- (1) more negative coverage increases risk
    -- (2) more conflict-coded events increases risk
    -- (3) high volume slightly increases risk via a log term
    (10.0 * (0.6 * negative_share + 0.4 * conflict_share)) + (LN(1 + total_events) / 5.5) AS risk_raw
  FROM base
)
SELECT
  date,
  CountryCode,
  total_events,
  conflict_events,
  conflict_share,
  negative_share,
  weighted_avg_tone,
  risk_raw
FROM scored;
