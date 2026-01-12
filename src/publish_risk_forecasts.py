from datetime import date

import pandas as pd
import pandas_gbq
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_absolute_error


PROJECT = "gen-lang-client-0366281238"
SOURCE_TABLE = f"{PROJECT}.gdelt_portfolio.country_risk_daily"
DEST_TABLE = "gdelt_portfolio.country_risk_forecasts_next_day"
LOCATION = "US"


def make_features(g: pd.DataFrame) -> pd.DataFrame:
    # Sort by date so “lag_7” really means 7 days earlier.
    g = g.sort_values("date").copy()

    # Lags give the model memory of recent risk levels.
    for k in [1, 2, 3, 7, 14]:
        g[f"lag_{k}"] = g["risk_raw"].shift(k)

    # Rolling stats capture trend and volatility without heavy modeling.
    g["roll_mean_7"] = g["risk_raw"].rolling(7, min_periods=3).mean()
    g["roll_std_7"] = g["risk_raw"].rolling(7, min_periods=3).std()

    # Tomorrow’s risk is the target we’re trying to predict.
    g["target_next_day"] = g["risk_raw"].shift(-1)
    return g


def main() -> None:
    client = bigquery.Client(project=PROJECT)

    # Pull only the columns we need to train + forecast.
    q = f"""
    SELECT
      date,
      CountryCode,
      total_events,
      conflict_share,
      negative_share,
      weighted_avg_tone,
      risk_raw
    FROM `{SOURCE_TABLE}`
    WHERE date IS NOT NULL AND CountryCode IS NOT NULL
    ORDER BY CountryCode, date
    """
    df = client.query(q).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])

    # Make sure numeric columns are truly numeric before feature engineering.
    num_cols = ["total_events", "conflict_share", "negative_share", "weighted_avg_tone", "risk_raw"]
    for col in num_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    # Build features for every country-day (including the latest day per country).
    # include_groups=False is the forward-safe behavior for pandas GroupBy.apply.
    feats_all = (
        df.groupby("CountryCode", group_keys=True)
        .apply(make_features, include_groups=False)
        .reset_index(level=0)  # Bring CountryCode back as a normal column.
    )

    feature_cols = [
        "lag_1", "lag_2", "lag_3", "lag_7", "lag_14",
        "roll_mean_7", "roll_std_7",
        "total_events", "conflict_share", "negative_share", "weighted_avg_tone",
    ]

    # Train only where we actually know “tomorrow” (target_next_day is present).
    train = feats_all.dropna(subset=feature_cols + ["target_next_day"]).copy()

    X = train[feature_cols].to_numpy()
    y = train["target_next_day"].to_numpy()
    d = train["date"]

    # Simple time-ordered backtest: last 14 days as holdout (no shuffling).
    cutoff = d.max() - pd.Timedelta(days=14)
    train_mask = d <= cutoff
    test_mask = d > cutoff

    model = RandomForestRegressor(n_estimators=500, random_state=42, n_jobs=-1)
    model.fit(X[train_mask], y[train_mask])

    if test_mask.sum() > 0:
        pred = model.predict(X[test_mask])
        mae = mean_absolute_error(y[test_mask], pred)
        print(f"Backtest MAE (last 14 days): {mae:.4f}")

    # Refit on all training rows before producing the latest forecasts.
    model.fit(X, y)

    # Forecast from the true latest day per country (even though it has no target).
    latest_rows = (
        feats_all.sort_values(["CountryCode", "date"])
        .groupby("CountryCode")
        .tail(1)
        .dropna(subset=feature_cols)
        .copy()
    )

    X_latest = latest_rows[feature_cols].to_numpy()
    yhat = model.predict(X_latest)

    latest_rows["run_date"] = date.today()
    latest_rows["as_of_date"] = latest_rows["date"].dt.date
    latest_rows["forecast_date"] = (latest_rows["date"] + pd.Timedelta(days=1)).dt.date
    latest_rows["pred_risk_next_day"] = yhat

    out = latest_rows[
        [
            "run_date",
            "as_of_date",
            "forecast_date",
            "CountryCode",
            "pred_risk_next_day",
            "risk_raw",
            "total_events",
            "conflict_share",
            "negative_share",
            "weighted_avg_tone",
        ]
    ].rename(columns={"risk_raw": "risk_as_of"})

    # Overwrite the table so Tableau always reads the latest snapshot.
    pandas_gbq.to_gbq(
        out,
        DEST_TABLE,
        project_id=PROJECT,
        if_exists="replace",
        location=LOCATION,
        progress_bar=True,
    )

    print(f"Published: {PROJECT}.{DEST_TABLE}")
    print(f"Countries forecasted: {len(out)}")
    print(out.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
