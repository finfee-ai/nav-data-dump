"""
AMFI NAV Scraper — Fetches daily NAV data for all Direct Growth mutual funds.

Source: https://www.amfiindia.com/spages/NAVAll.txt
Schedule: 3x/day via GitHub Actions (9:30 PM, 10:30 PM, 11:30 PM IST)

Output:
  - data/daily/YYYY-MM-DD.csv   (daily snapshot)
  - data/latest.csv             (always the freshest scrape)
  - metadata/scrape_log.json    (run history)
  - metadata/last_success.txt   (ISO timestamp of last success)
"""

import json
import os
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Paths (relative to repo root)
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
DAILY_DIR = REPO_ROOT / "data" / "daily"
LATEST_CSV = REPO_ROOT / "data" / "latest.csv"
SCRAPE_LOG = REPO_ROOT / "metadata" / "scrape_log.json"
LAST_SUCCESS = REPO_ROOT / "metadata" / "last_success.txt"
SCHEME_MASTER = REPO_ROOT / "metadata" / "scheme_master.csv"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AMFI_NAV_URL = "https://www.amfiindia.com/spages/NAVAll.txt"
REQUEST_TIMEOUT = 60  # seconds
MAX_RETRIES = 3
RETRY_DELAY = 60  # seconds between retries
MIN_EXPECTED_SCHEMES = 1500  # sanity check — expect ~2000+ Direct Growth funds

# IST timezone for logging
IST = timezone(timedelta(hours=5, minutes=30))


def fetch_amfi_nav_text() -> str:
    """Fetch raw NAVAll.txt from AMFI with retry logic."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Attempt {attempt}/{MAX_RETRIES} — fetching {AMFI_NAV_URL}")
            response = requests.get(AMFI_NAV_URL, timeout=REQUEST_TIMEOUT)
            response.raise_for_status()

            # Basic sanity: file should be at least 100KB (typically ~2-3 MB)
            if len(response.text) < 100_000:
                raise ValueError(f"Response too small: {len(response.text)} bytes (expected >100KB)")

            print(f"  Fetched {len(response.text):,} bytes")
            return response.text

        except Exception as e:
            last_error = e
            print(f"  Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"  Waiting {RETRY_DELAY}s before retry...")
                time.sleep(RETRY_DELAY)

    raise ConnectionError(f"All {MAX_RETRIES} attempts failed. Last error: {last_error}")


def parse_nav_data(raw_text: str) -> pd.DataFrame:
    """
    Parse AMFI NAVAll.txt into a DataFrame.

    File format (semicolon-delimited):
        Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

    Category headers appear as standalone lines (no semicolons or fewer than 5 fields).
    """
    rows = []

    for line in raw_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        parts = line.split(";")
        if len(parts) < 6:
            continue  # Skip category headers and malformed lines

        scheme_code_str = parts[0].strip()
        isin_growth = parts[1].strip()
        isin_div_reinv = parts[2].strip()
        scheme_name = parts[3].strip()
        nav_str = parts[4].strip()
        date_str = parts[5].strip()

        # Skip non-numeric scheme codes (header rows)
        if not scheme_code_str.isdigit():
            continue

        # Skip invalid NAV values
        if nav_str in ("N.A.", "N.A", "-", "", "B.C."):
            continue

        # Parse NAV as float
        try:
            nav_value = float(nav_str)
        except ValueError:
            continue

        # NAV must be positive
        if nav_value <= 0:
            continue

        rows.append({
            "scheme_code": int(scheme_code_str),
            "isin_growth": isin_growth if isin_growth and isin_growth != "-" else "",
            "isin_div_reinv": isin_div_reinv if isin_div_reinv and isin_div_reinv != "-" else "",
            "scheme_name": scheme_name,
            "nav": nav_value,
            "date": date_str,
        })

    if not rows:
        raise ValueError("No valid NAV rows parsed from AMFI data")

    df = pd.DataFrame(rows)
    print(f"  Parsed {len(df):,} total schemes from AMFI data")
    return df


def filter_direct_growth(df: pd.DataFrame) -> pd.DataFrame:
    """Filter to Direct Growth schemes only (exclude IDCW/Dividend)."""
    name_col = df["scheme_name"].str.lower()

    mask_direct = name_col.str.contains("direct", na=False)
    mask_exclude = name_col.str.contains("idcw|dividend", na=False)

    filtered = df[mask_direct & ~mask_exclude].copy()
    filtered = filtered.reset_index(drop=True)

    print(f"  Filtered to {len(filtered):,} Direct Growth schemes")
    return filtered


def normalize_nav_date(date_str: str) -> str:
    """
    Convert AMFI date format to ISO format.

    AMFI uses formats like: '24-Mar-2026' or '24-03-2026'
    Returns: '2026-03-24'
    """
    # Try common AMFI formats
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue

    raise ValueError(f"Cannot parse AMFI date: '{date_str}'")


def should_save(nav_date_iso: str, new_count: int) -> bool:
    """
    Decide whether to save this scrape.

    Skip if a daily file already exists with the same or more schemes.
    Overwrite if the new scrape has more schemes (catches corrections/late additions).
    """
    daily_file = DAILY_DIR / f"{nav_date_iso}.csv"

    if not daily_file.exists():
        return True

    try:
        existing = pd.read_csv(daily_file)
        existing_count = len(existing)

        if new_count > existing_count:
            print(f"  Existing file has {existing_count} schemes, new scrape has {new_count} — overwriting")
            return True
        else:
            print(f"  Existing file has {existing_count} schemes, new scrape has {new_count} — skipping (no improvement)")
            return False
    except Exception:
        # If we can't read the existing file, overwrite it
        return True


def save_data(df: pd.DataFrame, nav_date_iso: str) -> None:
    """Save daily CSV and update latest.csv."""
    DAILY_DIR.mkdir(parents=True, exist_ok=True)

    daily_file = DAILY_DIR / f"{nav_date_iso}.csv"
    df.to_csv(daily_file, index=False)
    print(f"  Saved daily file: {daily_file.name} ({len(df)} schemes)")

    df.to_csv(LATEST_CSV, index=False)
    print(f"  Updated latest.csv")


def update_scheme_master(df: pd.DataFrame) -> None:
    """
    Update scheme_master.csv with the full list of Direct Growth schemes.
    Contains scheme_code, isin_growth, isin_div_reinv, scheme_name (no NAV/date).
    """
    master = df[["scheme_code", "isin_growth", "isin_div_reinv", "scheme_name"]].copy()
    master = master.drop_duplicates(subset=["scheme_code"], keep="last")
    master = master.sort_values("scheme_code").reset_index(drop=True)

    SCHEME_MASTER.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(SCHEME_MASTER, index=False)
    print(f"  Updated scheme_master.csv ({len(master)} schemes)")


def log_scrape(status: str, scheme_count: int, nav_date: str, message: str = "") -> None:
    """Append to scrape_log.json."""
    SCRAPE_LOG.parent.mkdir(parents=True, exist_ok=True)

    # Load existing log
    log_entries = []
    if SCRAPE_LOG.exists():
        try:
            with open(SCRAPE_LOG, "r") as f:
                log_entries = json.load(f)
        except (json.JSONDecodeError, Exception):
            log_entries = []

    # Keep last 100 entries to prevent unbounded growth
    if len(log_entries) >= 100:
        log_entries = log_entries[-99:]

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)

    log_entries.append({
        "timestamp_utc": now_utc.isoformat(),
        "timestamp_ist": now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "status": status,
        "nav_date": nav_date,
        "scheme_count": scheme_count,
        "message": message,
    })

    with open(SCRAPE_LOG, "w") as f:
        json.dump(log_entries, f, indent=2)


def update_last_success() -> None:
    """Write current timestamp to last_success.txt."""
    LAST_SUCCESS.parent.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)

    with open(LAST_SUCCESS, "w") as f:
        f.write(f"{now_utc.isoformat()}\n")
        f.write(f"{now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}\n")


def main() -> int:
    """
    Main scraper entry point.

    Returns:
        0 on success or expected skip (holiday/duplicate)
        1 on unexpected failure
    """
    print("=" * 60)
    print("AMFI NAV Scraper")
    print(f"Run time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

    # --- Step 1: Fetch ---
    try:
        print("\n[1/6] Fetching AMFI NAVAll.txt...")
        raw_text = fetch_amfi_nav_text()
    except ConnectionError as e:
        print(f"\nFATAL: {e}")
        log_scrape("fetch_failed", 0, "", str(e))
        # Exit 0 to avoid GitHub Actions failure emails on AMFI downtime
        return 0

    # --- Step 2: Parse ---
    try:
        print("\n[2/6] Parsing NAV data...")
        all_schemes = parse_nav_data(raw_text)
    except ValueError as e:
        print(f"\nFATAL: {e}")
        log_scrape("parse_failed", 0, "", str(e))
        return 1

    # --- Step 3: Filter ---
    print("\n[3/6] Filtering Direct Growth schemes...")
    direct_growth = filter_direct_growth(all_schemes)

    if len(direct_growth) == 0:
        print("FATAL: No Direct Growth schemes found after filtering")
        log_scrape("filter_empty", 0, "", "Zero schemes after Direct Growth filter")
        return 1

    # Validation: warn if fewer than expected
    if len(direct_growth) < MIN_EXPECTED_SCHEMES:
        print(f"  WARNING: Only {len(direct_growth)} schemes (expected >={MIN_EXPECTED_SCHEMES})")

    # --- Step 4: Extract NAV date ---
    print("\n[4/6] Determining NAV date...")
    raw_date = direct_growth["date"].iloc[0]
    try:
        nav_date_iso = normalize_nav_date(raw_date)
    except ValueError as e:
        print(f"FATAL: {e}")
        log_scrape("date_parse_failed", len(direct_growth), "", str(e))
        return 1

    print(f"  NAV date: {nav_date_iso} (from AMFI file: '{raw_date}')")

    # Normalize the date column to ISO format
    direct_growth["date"] = nav_date_iso

    # --- Step 5: Dedup check ---
    print("\n[5/6] Checking for duplicates...")
    if not should_save(nav_date_iso, len(direct_growth)):
        log_scrape("skipped_duplicate", len(direct_growth), nav_date_iso,
                    "Daily file already exists with same or more schemes")
        print("\nDone — no new data to save.")
        return 0

    # --- Step 6: Save ---
    print("\n[6/6] Saving data...")
    save_data(direct_growth, nav_date_iso)
    update_scheme_master(direct_growth)
    log_scrape("success", len(direct_growth), nav_date_iso)
    update_last_success()

    print(f"\nDone — saved {len(direct_growth)} schemes for {nav_date_iso}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
