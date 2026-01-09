from pathlib import Path

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
IN_PATH = ROOT / "data" / "extracts" / "events_daily_20251001_20260101.csv"

OUT_DIR = ROOT / "data" / "processed"
OUT_DIR.mkdir(parents=True, exist_ok=True)

REPORT_DIR = ROOT / "reports"
REPORT_DIR.mkdir(parents=True, exist_ok=True)


ROOT_CODE_LABEL = {
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


def tone_bucket(x: float) -> str:
    if pd.isna(x):
        return "unknown"
    if x <= -2:
        return "negative"
    if x >= 2:
        return "positive"
    return "neutral"


def main() -> None:
    df = pd.read_csv(
    IN_PATH,
    dtype={"SQLDATE": "string", "CountryCode": "string", "EventRootCode": "string"},
    low_memory=False,
    )


    df["SQLDATE"] = df["SQLDATE"].astype(str)
    df["date"] = pd.to_datetime(df["SQLDATE"], format="%Y%m%d", errors="coerce")

    df["CountryCode"] = df["CountryCode"].astype(str).str.strip()
    df = df[df["CountryCode"].str.len() == 2]

    df["EventRootCode"] = df["EventRootCode"].astype(str).str.zfill(2)
    df = df[df["EventRootCode"].isin(ROOT_CODE_LABEL.keys())]

    df["EventRootLabel"] = df["EventRootCode"].map(ROOT_CODE_LABEL)
    df["ToneBucket"] = df["AvgTone"].apply(tone_bucket)

    df["EventCount"] = pd.to_numeric(df["EventCount"], errors="coerce").fillna(0).astype(int)
    df["TotalMentions"] = pd.to_numeric(df["TotalMentions"], errors="coerce").fillna(0).astype(int)
    df["TotalArticles"] = pd.to_numeric(df["TotalArticles"], errors="coerce").fillna(0).astype(int)
    df["TotalSources"] = pd.to_numeric(df["TotalSources"], errors="coerce").fillna(0).astype(int)

    df = df.dropna(subset=["date"])
    df = df.sort_values(["date", "CountryCode", "EventRootCode"]).reset_index(drop=True)

    out_parquet = OUT_DIR / "events_daily_clean.parquet"
    out_csv_gz = OUT_DIR / "events_daily_clean.csv.gz"

    try:
        df.to_parquet(out_parquet, index=False)
        saved_path = out_parquet
    except Exception:
        df.to_csv(out_csv_gz, index=False, compression="gzip")
        saved_path = out_csv_gz

    report_path = REPORT_DIR / "data_quality_events_daily.md"

    date_min = df["date"].min()
    date_max = df["date"].max()
    n_countries = df["CountryCode"].nunique()
    n_root_codes = df["EventRootCode"].nunique()
    n_rows = len(df)

    root_counts = (
        df.groupby(["EventRootCode", "EventRootLabel"])["EventCount"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
    )

    with open(report_path, "w", encoding="utf-8") as f:
        f.write("# Data Quality Report — events_daily_clean\n\n")
        f.write(f"- Input: `{IN_PATH}`\n")
        f.write(f"- Output: `{saved_path}`\n\n")
        f.write(f"- Rows: {n_rows}\n")
        f.write(f"- Date range: {date_min.date()} to {date_max.date()}\n")
        f.write(f"- Countries: {n_countries}\n")
        f.write(f"- Root codes present: {n_root_codes} / 20\n\n")
        f.write("## Top 10 root codes by total event count\n\n")
        f.write(root_counts.to_frame("TotalEventCount").to_markdown())
        f.write("\n")

    print(f"Saved cleaned dataset to: {saved_path}")
    print(f"Saved report to: {report_path}")
    print(df.head())


if __name__ == "__main__":
    main()
