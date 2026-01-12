from pathlib import Path

import pandas as pd
import pandas_gbq

BILLING_PROJECT = "gen-lang-client-0366281238"
DESTINATION = "gdelt_portfolio.events_daily_clean"
LOCATION = "US"


def main() -> None:
    # This pushes the cleaned dataset into BigQuery so Tableau can query it directly.
    root = Path(__file__).resolve().parents[1]

    parquet_path = root / "data" / "processed" / "events_daily_clean.parquet"
    csv_gz_path = root / "data" / "processed" / "events_daily_clean.csv.gz"

    # This reads the fastest local format available.
    if parquet_path.exists():
        df = pd.read_parquet(parquet_path)
    else:
        df = pd.read_csv(csv_gz_path)

    # This keeps dates clean for BigQuery and downstream tools.
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # This overwrites the table each run so Tableau always reads the latest version.
    pandas_gbq.to_gbq(
        df,
        DESTINATION,
        project_id=BILLING_PROJECT,
        if_exists="replace",
        location=LOCATION,
        progress_bar=True,
    )

    print(f"Published table: {BILLING_PROJECT}.{DESTINATION}")


if __name__ == "__main__":
    main()
