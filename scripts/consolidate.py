"""
Monthly Consolidation — Rolls daily CSVs into monthly parquet files.

Runs after every scrape. Handles:
  - Building/rebuilding current month parquet from daily CSVs
  - Finalizing previous month's parquet (first few days of new month)
  - Deduplicating by (scheme_code, date), keeping last entry
  - Cleaning up daily CSVs older than 7 days
"""

import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd

# ---------------------------------------------------------------------------
# Paths (relative to repo root)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
DAILY_DIR = REPO_ROOT / "data" / "daily"
MONTHLY_DIR = REPO_ROOT / "data" / "monthly"

# How many days of daily CSVs to keep
DAILY_RETENTION_DAYS = 7

# IST timezone
IST = timezone(timedelta(hours=5, minutes=30))


def get_daily_csvs() -> list[Path]:
    """Get all daily CSV files sorted by date."""
    csvs = sorted(DAILY_DIR.glob("*.csv"))
    # Filter out non-date files (like .gitkeep)
    valid = []
    for f in csvs:
        try:
            datetime.strptime(f.stem, "%Y-%m-%d")
            valid.append(f)
        except ValueError:
            continue
    return valid


def load_daily_csv(path: Path) -> pd.DataFrame:
    """Load a daily CSV with proper dtypes."""
    df = pd.read_csv(path, dtype={
        "scheme_code": int,
        "isin_growth": str,
        "isin_div_reinv": str,
        "scheme_name": str,
        "nav": float,
        "date": str,
    })
    # Fill NaN ISINs with empty string
    df["isin_growth"] = df["isin_growth"].fillna("")
    df["isin_div_reinv"] = df["isin_div_reinv"].fillna("")
    return df


def build_monthly_parquet(year_month: str, daily_files: list[Path]) -> None:
    """
    Build a monthly parquet from daily CSVs for a given YYYY-MM.

    If a monthly parquet already exists, merge new daily data into it
    (handles the case where we add more days to the current month).
    """
    if not daily_files:
        return

    MONTHLY_DIR.mkdir(parents=True, exist_ok=True)
    parquet_path = MONTHLY_DIR / f"{year_month}.parquet"

    # Load all relevant daily CSVs
    frames = []
    for f in daily_files:
        try:
            df = load_daily_csv(f)
            frames.append(df)
        except Exception as e:
            print(f"  Warning: Could not read {f.name}: {e}")
            continue

    if not frames:
        return

    new_data = pd.concat(frames, ignore_index=True)

    # If existing parquet exists, merge with it
    if parquet_path.exists():
        try:
            existing = pd.read_parquet(parquet_path)
            # Ensure consistent dtypes
            existing["isin_growth"] = existing["isin_growth"].fillna("").astype(str)
            existing["isin_div_reinv"] = existing["isin_div_reinv"].fillna("").astype(str)
            combined = pd.concat([existing, new_data], ignore_index=True)
        except Exception as e:
            print(f"  Warning: Could not read existing {parquet_path.name}: {e}")
            combined = new_data
    else:
        combined = new_data

    # Deduplicate: keep last entry per (scheme_code, date)
    # This handles NAV corrections — later scrapes overwrite earlier ones
    combined = combined.drop_duplicates(
        subset=["scheme_code", "date"],
        keep="last"
    ).reset_index(drop=True)

    # Sort for clean output
    combined = combined.sort_values(["date", "scheme_code"]).reset_index(drop=True)

    combined.to_parquet(parquet_path, index=False, engine="pyarrow")
    print(f"  Saved {parquet_path.name}: {len(combined):,} rows")


def cleanup_old_dailies(daily_files: list[Path]) -> None:
    """Delete daily CSVs older than DAILY_RETENTION_DAYS."""
    now = datetime.now(timezone.utc)
    cutoff = now - timedelta(days=DAILY_RETENTION_DAYS)
    deleted = 0

    for f in daily_files:
        try:
            file_date = datetime.strptime(f.stem, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            if file_date < cutoff:
                f.unlink()
                deleted += 1
        except (ValueError, OSError):
            continue

    if deleted:
        print(f"  Cleaned up {deleted} daily CSV(s) older than {DAILY_RETENTION_DAYS} days")


def main() -> int:
    print("\n" + "=" * 60)
    print("Monthly Consolidation")
    print(f"Run time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

    daily_files = get_daily_csvs()

    if not daily_files:
        print("\nNo daily CSVs found. Nothing to consolidate.")
        return 0

    print(f"\nFound {len(daily_files)} daily CSV(s)")

    # --- Group daily files by month ---
    month_groups: dict[str, list[Path]] = {}
    for f in daily_files:
        try:
            file_date = datetime.strptime(f.stem, "%Y-%m-%d")
            year_month = file_date.strftime("%Y-%m")
            month_groups.setdefault(year_month, []).append(f)
        except ValueError:
            continue

    # --- Build/update monthly parquets ---
    print(f"\n[1/2] Building monthly parquets...")
    for year_month in sorted(month_groups.keys()):
        files = month_groups[year_month]
        print(f"\n  Processing {year_month} ({len(files)} daily file(s))...")
        build_monthly_parquet(year_month, files)

    # --- Cleanup old daily CSVs ---
    print(f"\n[2/2] Cleaning up old daily CSVs...")
    cleanup_old_dailies(daily_files)

    print("\nConsolidation complete.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
