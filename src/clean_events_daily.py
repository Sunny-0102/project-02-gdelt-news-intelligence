from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]

EXTRACT_DIR = ROOT / "data" / "extracts"
OUT_PARQUET = ROOT / "data" / "processed" / "events_daily_clean.parquet"
REPORT_PATH = ROOT / "reports" / "data_quality_events_daily.md"


# Simple CAMEO root labels so the dataset is self-explanatory in Tableau.
ROOT_LABEL = {
    "01": "Make Public Statement",
    "02": "Appeal",
    "03": "Express Intent to Cooperate",
    "04": "Consult",
    "05": "Engage in Diplomatic Cooperation",
    "06": "Engage in Material Cooperation",
    "07": "Provide Aid",
    "08": "Yield",
    "09": "Investigate",
    "10": "Demand",
    "11": "Disapprove",
    "12": "Reject",
    "13": "Threaten",
    "14": "Protest",
    "15": "Exhibit Military Posture",
    "16": "Reduce Relations",
    "17": "Coerce",
    "18": "Assault",
    "19": "Fight",
    "20": "Use Unconventional Mass Violence",
}


def latest_extract_file() -> Path:
    # Grab the newest extract so you never clean the wrong file by accident.
    files = sorted(EXTRACT_DIR.glob("events_daily_*.csv"))
    if not files:
        raise FileNotFoundError(f"No events_daily_*.csv found in {EXTRACT_DIR}")
    return max(files, key=lambda p: p.stat().st_mtime)


def tone_bucket(avg_tone: float) -> str:
    # Keep buckets stable and easy to explain to non-technical viewers.
    if pd.isna(avg_tone):
        return "unknown"
    if avg_tone <= -2:
        return "negative"
    if avg_tone >= 2:
        return "positive"
    return "neutral"


def main() -> None:
    OUT_PARQUET.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    in_path = latest_extract_file()
    print(f"Cleaning extract: {in_path}")

    # Force types so pandas doesn’t “guess” differently on different runs.
    df = pd.read_csv(
        in_path,
        dtype={
            "CountryCode": "string",
            "EventRootCode": "string",
        },
        low_memory=False,
    )

    # Normalize codes to 2-digit strings (01..20) so joins and maps behave.
    df["EventRootCode"] = df["EventRootCode"].astype("string").str.strip().str.zfill(2)

    # SQLDATE is YYYYMMDD; make it a real date column for Tableau and time-series work.
    df["SQLDATE"] = df["SQLDATE"].astype(str)
    df["date"] = pd.to_datetime(df["SQLDATE"], format="%Y%m%d", errors="coerce")

    # Make numeric columns numeric (bad rows become NaN instead of crashing later).
    for col in [
        "EventCount",
        "AvgTone",
        "AvgGoldstein",
        "TotalMentions",
        "TotalArticles",
        "TotalSources",
    ]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Add readable labels and simple sentiment buckets.
    df["EventRootLabel"] = df["EventRootCode"].map(ROOT_LABEL).fillna("Unknown")
    df["ToneBucket"] = df["AvgTone"].apply(tone_bucket)

    # Keep a clean, consistent column order for downstream scripts and Tableau.
    keep_cols = [
        "SQLDATE",
        "CountryCode",
        "EventRootCode",
        "EventCount",
        "AvgTone",
        "AvgGoldstein",
        "TotalMentions",
        "TotalArticles",
        "TotalSources",
        "date",
        "EventRootLabel",
        "ToneBucket",
    ]
    df = df[keep_cols]

    df.to_parquet(OUT_PARQUET, index=False)
    print(f"Saved cleaned dataset to: {OUT_PARQUET}")

    # Small report so the repo proves data coverage + quality at a glance.
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write("# Events Daily — Data Quality Report\n\n")
        f.write(f"- Source extract: `{in_path.name}`\n")
        f.write(f"- Rows: {len(df):,}\n")
        f.write(f"- Date range: {df['date'].min().date()} → {df['date'].max().date()}\n\n")

        f.write("## Missing values (by column)\n\n")
        f.write(df.isna().sum().to_frame("missing").to_markdown())
        f.write("\n\n")

        f.write("## Top EventRootCode by total EventCount\n\n")
        top_roots = (
            df.groupby(["EventRootCode", "EventRootLabel"], dropna=False)["EventCount"]
            .sum()
            .sort_values(ascending=False)
            .head(10)
            .reset_index()
        )
        f.write(top_roots.to_markdown(index=False))
        f.write("\n")

    print(f"Saved report to: {REPORT_PATH}")
    print(df.head(5).to_string(index=False))


if __name__ == "__main__":
    main()
