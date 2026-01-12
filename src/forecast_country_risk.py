from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns
from google.cloud import bigquery
from sklearn.ensemble import RandomForestRegressor
from sklearn.inspection import permutation_importance
from sklearn.metrics import mean_absolute_error
from sklearn.model_selection import TimeSeriesSplit

BILLING_PROJECT = "gen-lang-client-0366281238"
TABLE = f"{BILLING_PROJECT}.gdelt_portfolio.country_risk_daily"


ROOT = Path(__file__).resolve().parents[1]
FIG_DIR = ROOT / "reports" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)

REP_DIR = ROOT / "reports"
REP_DIR.mkdir(parents=True, exist_ok=True)


def save_fig(name: str) -> None:
    # This writes a real image artifact for GitHub and your final report.
    out = FIG_DIR / name
    plt.tight_layout()
    plt.savefig(out, dpi=200)
    plt.close()
    print(f"Saved: {out}")


def make_features(df: pd.DataFrame) -> pd.DataFrame:
    # This creates lag and rolling features so the model can learn momentum and mean reversion.
    df = df.sort_values("date").copy()
    for k in [1, 2, 3, 7, 14]:
        df[f"lag_{k}"] = df["risk_raw"].shift(k)
    df["roll_mean_7"] = df["risk_raw"].rolling(7, min_periods=3).mean()
    df["roll_std_7"] = df["risk_raw"].rolling(7, min_periods=3).std()
    df["target_next_day"] = df["risk_raw"].shift(-1)
    return df


def main() -> None:
    sns.set_theme(style="whitegrid")

    client = bigquery.Client(project=BILLING_PROJECT)
    query = f"""
    SELECT date, CountryCode, risk_raw
    FROM `{TABLE}`
    WHERE date IS NOT NULL
      AND CountryCode IS NOT NULL
    ORDER BY CountryCode, date
    """
    df = client.query(query).to_dataframe()
    df["date"] = pd.to_datetime(df["date"])

    # This picks a country with enough activity so the forecast is meaningful.
    top_country = client.query(
        f"SELECT CountryCode FROM `{TABLE}` GROUP BY CountryCode ORDER BY SUM(total_events) DESC LIMIT 1"
    ).to_dataframe()["CountryCode"][0]

    one = df[df["CountryCode"] == top_country].copy()

    # This fills missing days so time-based splits behave like a real daily series.
    full_days = pd.date_range(one["date"].min(), one["date"].max(), freq="D")
    one = one.set_index("date").reindex(full_days).reset_index().rename(columns={"index": "date"})
    one["CountryCode"] = top_country
    one["risk_raw"] = one["risk_raw"].fillna(0.0)

    one = make_features(one)
    one = one.dropna(subset=["lag_14", "roll_mean_7", "roll_std_7", "target_next_day"]).reset_index(
        drop=True
    )

    feature_cols = ["lag_1", "lag_2", "lag_3", "lag_7", "lag_14", "roll_mean_7", "roll_std_7"]
    X = one[feature_cols].to_numpy()
    y = one["target_next_day"].to_numpy()

    # This uses time-ordered splits so we never train on the future.
    tss = TimeSeriesSplit(n_splits=5)
    maes = []

    last_fold = None
    for train_idx, test_idx in tss.split(X):
        model = RandomForestRegressor(
            n_estimators=500,
            random_state=42,
            n_jobs=-1,
        )
        model.fit(X[train_idx], y[train_idx])
        pred = model.predict(X[test_idx])
        mae = mean_absolute_error(y[test_idx], pred)
        maes.append(mae)
        last_fold = (model, test_idx, pred)

    avg_mae = float(np.mean(maes))

    model, test_idx, pred = last_fold
    test = one.iloc[test_idx].copy()
    test["pred_next_day"] = pred

    plt.figure(figsize=(11, 4))
    plt.plot(test["date"], test["target_next_day"], label="Actual next-day risk")
    plt.plot(test["date"], test["pred_next_day"], label="Predicted next-day risk")
    plt.title(f"Next-Day Risk Forecast (Country: {top_country}) | Avg MAE: {avg_mae:.4f}")
    plt.xlabel("Date")
    plt.ylabel("risk_raw")
    plt.legend()
    save_fig("10_risk_forecast_next_day.png")

    # This explains which features the model relied on the most.
    perm = permutation_importance(model, X[test_idx], y[test_idx], n_repeats=10, random_state=42)
    imp = pd.DataFrame({"feature": feature_cols, "importance": perm.importances_mean}).sort_values(
        "importance", ascending=False
    )

    report_path = REP_DIR / "risk_forecast_report.md"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Risk forecast report\n\n")
        f.write(f"- Country: `{top_country}`\n")
        f.write(f"- Avg MAE (TimeSeriesSplit): {avg_mae:.6f}\n\n")
        f.write("## Permutation importance (last fold)\n\n")
        f.write(imp.to_markdown(index=False))
        f.write("\n")

    print(f"Saved: {report_path}")


if __name__ == "__main__":
    main()
