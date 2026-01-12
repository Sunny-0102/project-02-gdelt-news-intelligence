from pathlib import Path

import matplotlib
import numpy as np
import pandas as pd

matplotlib.use("Agg")

import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

ROOT = Path(__file__).resolve().parents[1]
IN_PARQUET = ROOT / "data" / "processed" / "events_daily_clean.parquet"
IN_FALLBACK = ROOT / "data" / "processed" / "events_daily_clean.csv.gz"

OUT_REPORTS = ROOT / "reports" / "anomalies"
OUT_REPORTS.mkdir(parents=True, exist_ok=True)

FIG_DIR = ROOT / "reports" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)


def save_fig(name: str) -> None:
    # This writes a real image file that will render on GitHub.
    out = FIG_DIR / name
    plt.tight_layout()
    plt.savefig(out, dpi=200)
    plt.close()
    print(f"Saved: {out}")


def safe_read_clean() -> pd.DataFrame:
    # This prefers Parquet for speed, but still works if only the gzipped CSV exists.
    if IN_PARQUET.exists():
        return pd.read_parquet(IN_PARQUET)
    if IN_FALLBACK.exists():
        return pd.read_csv(IN_FALLBACK)
    raise FileNotFoundError(
        "Clean dataset not found in data/processed/. Run clean_events_daily.py first."
    )


def main() -> None:
    sns.set_theme(style="whitegrid")

    df = safe_read_clean()
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # These weighted numerators let us recompute country-day averages from root-code rows.
    df["tone_x_events"] = df["AvgTone"] * df["EventCount"]
    df["gold_x_events"] = df["AvgGoldstein"] * df["EventCount"]

    # This collapses root-code rows into one row per (date, country).
    panel = (
        df.groupby(["date", "CountryCode"], as_index=False)
        .agg(
            EventCount=("EventCount", "sum"),
            TotalMentions=("TotalMentions", "sum"),
            TotalArticles=("TotalArticles", "sum"),
            TotalSources=("TotalSources", "sum"),
            tone_x_events=("tone_x_events", "sum"),
            gold_x_events=("gold_x_events", "sum"),
        )
        .sort_values(["CountryCode", "date"])
        .reset_index(drop=True)
    )

    # These are the country-day averages we want to model.
    panel["AvgTone"] = panel["tone_x_events"] / panel["EventCount"].replace(0, np.nan)
    panel["AvgGoldstein"] = panel["gold_x_events"] / panel["EventCount"].replace(0, np.nan)

    # These logs tame extreme counts while keeping zero safe.
    panel["log_events"] = np.log1p(panel["EventCount"])
    panel["log_mentions"] = np.log1p(panel["TotalMentions"])

    # This creates rolling baselines per country so “unusual” means unusual for that country.
    def add_rolling(group: pd.DataFrame) -> pd.DataFrame:
        group = group.sort_values("date").copy()

        for col, out in [
            ("log_events", "z_events_7d"),
            ("log_mentions", "z_mentions_7d"),
            ("AvgTone", "z_tone_7d"),
            ("AvgGoldstein", "z_goldstein_7d"),
        ]:
            roll_mean = group[col].rolling(window=7, min_periods=3).mean()
            roll_std = group[col].rolling(window=7, min_periods=3).std()

            z = (group[col] - roll_mean) / roll_std.replace(0, np.nan)
            group[out] = z.fillna(0)

        return group

    panel = panel.groupby("CountryCode", group_keys=False).apply(add_rolling)

    # This is the feature set the anomaly model will learn from.
    feature_cols = ["z_events_7d", "z_mentions_7d", "z_tone_7d", "z_goldstein_7d"]
    X = panel[feature_cols].to_numpy()

    # This makes features comparable so one column cannot dominate by scale alone.
    scaler = StandardScaler()
    Xs = scaler.fit_transform(X)

    # This flags the most unusual country-days without needing labels.
    model = IsolationForest(
        n_estimators=300,
        contamination=0.01,
        random_state=42,
        n_jobs=-1,
    )
    model.fit(Xs)

    panel["anomaly_label"] = model.predict(Xs)
    panel["anomaly_score"] = model.decision_function(Xs)

    # This keeps only the strongest anomalies for a clean, shareable report.
    top = (
        panel.sort_values("anomaly_score", ascending=True)
        .head(50)
        .loc[
            :,
            [
                "date",
                "CountryCode",
                "EventCount",
                "AvgTone",
                "AvgGoldstein",
                "anomaly_score",
                "anomaly_label",
            ],
        ]
    )
    top_path = OUT_REPORTS / "top_50_country_day_anomalies.csv"
    top.to_csv(top_path, index=False)
    print(f"Saved: {top_path}")

    # This draws a single clear plot for the highest-activity country in the window.
    top_country = panel.groupby("CountryCode")["EventCount"].sum().idxmax()
    one = panel[panel["CountryCode"] == top_country].copy()
    one = one.sort_values("date")

    plt.figure(figsize=(11, 4))
    sns.lineplot(data=one, x="date", y="EventCount", label="EventCount")

    anomalies = one[one["anomaly_label"] == -1]
    plt.scatter(anomalies["date"], anomalies["EventCount"], label="Anomaly", marker="x")

    plt.title(f"Daily EventCount with Detected Anomalies ({top_country})")
    plt.xlabel("Date")
    plt.ylabel("EventCount")
    plt.legend()
    save_fig("07_anomalies_top_country_eventcount.png")


if __name__ == "__main__":
    main()
