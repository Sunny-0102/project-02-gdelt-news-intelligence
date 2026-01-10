from google.cloud import bigquery


BILLING_PROJECT = "gen-lang-client-0366281238"
SOURCE_TABLE = "gdelt-bq.gdeltv2.gkg_partitioned"
DEST_TABLE = f"{BILLING_PROJECT}.gdelt_portfolio.gkg_theme_daily_20251001_20260101"

START = "2025-10-01"
END = "2026-01-01"
TOP_THEMES = 200


def pick_field(fields: set[str], candidates: list[str]) -> str:
    for name in candidates:
        if name in fields:
            return name
    raise RuntimeError(f"None of these fields exist: {candidates}")


def main() -> None:
    client = bigquery.Client(project=BILLING_PROJECT)

    table = client.get_table(SOURCE_TABLE)
    field_names = {f.name for f in table.schema}

    themes_field = pick_field(field_names, ["V2Themes", "Themes"])
    tone_field = pick_field(field_names, ["V2Tone", "Tone"])

    query = f"""
    CREATE OR REPLACE TABLE `{DEST_TABLE}` AS
    WITH base AS (
      SELECT
        DATE(_PARTITIONTIME) AS date,
        {themes_field} AS ThemesField,
        SAFE_CAST(SPLIT({tone_field}, ',')[OFFSET(0)] AS FLOAT64) AS Tone
      FROM `{SOURCE_TABLE}`
      WHERE _PARTITIONTIME >= TIMESTAMP('{START}')
        AND _PARTITIONTIME <  TIMESTAMP('{END}')
        AND {themes_field} IS NOT NULL
    ),
    exploded AS (
      SELECT
        date,
        theme,
        Tone
      FROM base,
      UNNEST(SPLIT(ThemesField, ';')) AS theme
      WHERE theme IS NOT NULL AND theme != ''
    ),
    top_themes AS (
      SELECT theme
      FROM exploded
      GROUP BY theme
      ORDER BY COUNT(1) DESC
      LIMIT {TOP_THEMES}
    )
    SELECT
      date,
      theme,
      COUNT(1) AS ArticleCount,
      AVG(Tone) AS AvgTone
    FROM exploded
    JOIN top_themes USING(theme)
    GROUP BY date, theme
    ORDER BY date, theme
    """

    job = client.query(query)
    job.result()

    print(f"Created: {DEST_TABLE}")


if __name__ == "__main__":
    main()
