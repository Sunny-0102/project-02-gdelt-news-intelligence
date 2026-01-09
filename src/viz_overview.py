from pathlib import Path
import numpy as np

import matplotlib
matplotlib.use("Agg")

import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns


ROOT = Path(__file__).resolve().parents[1]
DATA_PATH = ROOT / "data" / "processed" / "events_daily_clean.parquet"
FIG_DIR = ROOT / "reports" / "figures"
FIG_DIR.mkdir(parents=True, exist_ok=True)


def save_fig(name: str) -> None:
    # This saves the current chart to disk so the repo has real artifacts.
    out = FIG_DIR / name
    plt.tight_layout()
    plt.savefig(out, dpi=200)
    plt.close()
    print(f"Saved: {out}")


def main() -> None:
    # This keeps styling consistent across every plot in the project.
    sns.set_theme(style="whitegrid")

    # This dataset is already cleaned and labeled, so we can focus on insights.
    df = pd.read_parquet(DATA_PATH)

    # This makes sure our date column behaves like a real datetime everywhere.
    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    df = df.dropna(subset=["date"])

    # 1) Global activity over time.
    daily = df.groupby("date", as_index=False)["EventCount"].sum()
    plt.figure(figsize=(10, 4))
    sns.lineplot(data=daily, x="date", y="EventCount")
    plt.title("Global Event Volume Over Time")
    plt.xlabel("Date")
    plt.ylabel("Total Event Count")
    save_fig("01_global_event_volume_over_time.png")

    # 2) Top countries by total activity (based on event location).
    top_countries = (
        df.groupby("CountryCode", as_index=False)["EventCount"]
        .sum()
        .sort_values("EventCount", ascending=False)
        .head(15)
    )
    plt.figure(figsize=(10, 5))
    sns.barplot(data=top_countries, x="EventCount", y="CountryCode")
    plt.title("Top 15 Countries by Total Event Count")
    plt.xlabel("Total Event Count")
    plt.ylabel("Country Code")
    save_fig("02_top_countries_by_event_count.png")

    # 3) Tone distribution, weighted by how many events each row represents.
    plt.figure(figsize=(10, 4))
    plt.hist(df["AvgTone"], bins=60, weights=df["EventCount"])
    plt.title("Event-Weighted Tone Distribution (AvgTone)")
    plt.xlabel("AvgTone")
    plt.ylabel("Weighted Count")
    save_fig("03_weighted_tone_distribution.png")

    # 4) Which event categories dominate overall (CAMEO root codes).
    top_roots = (
        df.groupby(["EventRootCode", "EventRootLabel"], as_index=False)["EventCount"]
        .sum()
        .sort_values("EventCount", ascending=False)
        .head(12)
    )
    plt.figure(figsize=(10, 5))
    sns.barplot(data=top_roots, x="EventCount", y="EventRootLabel")
    plt.title("Top 12 Event Root Categories by Total Event Count")
    plt.xlabel("Total Event Count")
    plt.ylabel("Event Root Category")
    save_fig("04_top_event_root_categories.png")

    # 5) Monthly heatmap of activity for the top root categories.
    df["month"] = df["date"].dt.to_period("M").dt.to_timestamp()
    top_root_labels = top_roots["EventRootLabel"].tolist()

    heat = (
        df[df["EventRootLabel"].isin(top_root_labels)]
        .groupby(["month", "EventRootLabel"])["EventCount"]
        .sum()
        .reset_index()
        .pivot(index="EventRootLabel", columns="month", values="EventCount")
        .fillna(0)
    )

    plt.figure(figsize=(12, 6))
    sns.heatmap(np.log10(heat + 1), cbar=True)
    plt.title("Monthly Activity Heatmap (log-scaled) for Top Root Categories")
    plt.xlabel("Month")
    plt.ylabel("Event Root Category")
    save_fig("05_monthly_root_category_heatmap.png")

    # 6) Tone by category (restricted to top categories so it stays readable).
    df_top = df[df["EventRootLabel"].isin(top_root_labels)].copy()
    plt.figure(figsize=(12, 5))
    sns.boxplot(data=df_top, x="EventRootLabel", y="AvgTone")
    plt.title("Tone by Event Root Category (Top Categories)")
    plt.xlabel("Event Root Category")
    plt.ylabel("AvgTone")
    plt.xticks(rotation=25, ha="right")
    save_fig("06_tone_by_root_category_boxplot.png")


if __name__ == "__main__":
    main()
