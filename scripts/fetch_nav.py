"""
AMFI NAV Fetcher — Fetches daily NAV data for all Direct Growth mutual funds.

Source: https://www.amfiindia.com/spages/NAVAll.txt
Schedule: 4x/day via GitHub Actions (6:00 AM, 10:30 AM, 2:10 PM, 10:00 PM IST)

How daily files work:
  - 10:00 PM run  → creates data/daily/YYYY-MM-DD.csv for TODAY
  - Morning/afternoon runs → UPDATE previous date files with late-declaring AMCs
    e.g. PPFAS declares March 24 NAV on March 25 morning → added to 2026-03-24.csv

  Each daily file is the COMPLETE, FINAL record for that date.
  Each fund's NAV is stored under its OWN declared date, never mismatched.

Output:
  - data/daily/YYYY-MM-DD.csv   — complete NAV record for that date (updated as AMCs declare)
  - data/latest.csv             — today's NAVs only (majority date, for quick access)
  - metadata/fetch_log.json     — run history
  - metadata/last_success.txt   — ISO timestamp of last successful run
  - metadata/scheme_master.csv  — full Direct Growth scheme list with ISINs
"""

import json
import sys
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pandas as pd
import requests

# ---------------------------------------------------------------------------
# Paths (relative to repo root)
# ---------------------------------------------------------------------------
REPO_ROOT    = Path(__file__).resolve().parent.parent
DAILY_DIR    = REPO_ROOT / "data" / "daily"
LATEST_CSV   = REPO_ROOT / "data" / "latest.csv"
FETCH_LOG    = REPO_ROOT / "metadata" / "fetch_log.json"
LAST_SUCCESS = REPO_ROOT / "metadata" / "last_success.txt"
SCHEME_MASTER= REPO_ROOT / "metadata" / "scheme_master.csv"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
AMFI_NAV_URL       = "https://www.amfiindia.com/spages/NAVAll.txt"
REQUEST_TIMEOUT    = 60    # seconds per attempt
MAX_RETRIES        = 3
RETRY_DELAY        = 60    # seconds between retries
MIN_EXPECTED       = 1500  # sanity floor — expect ~3000+ Direct Growth funds

IST = timezone(timedelta(hours=5, minutes=30))


# ---------------------------------------------------------------------------
# Step 1 — Fetch
# ---------------------------------------------------------------------------
def fetch_amfi_text() -> str:
    """Fetch raw NAVAll.txt from AMFI with retry logic."""
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(f"  Attempt {attempt}/{MAX_RETRIES} — fetching {AMFI_NAV_URL}")
            resp = requests.get(AMFI_NAV_URL, timeout=REQUEST_TIMEOUT)
            resp.raise_for_status()

            if len(resp.text) < 100_000:
                raise ValueError(f"Response too small: {len(resp.text):,} bytes (expected >100 KB)")

            print(f"  Fetched {len(resp.text):,} bytes")
            return resp.text

        except Exception as e:
            last_error = e
            print(f"  Attempt {attempt} failed: {e}")
            if attempt < MAX_RETRIES:
                print(f"  Waiting {RETRY_DELAY}s before retry...")
                time.sleep(RETRY_DELAY)

    raise ConnectionError(f"All {MAX_RETRIES} attempts failed. Last error: {last_error}")


# ---------------------------------------------------------------------------
# Step 2 — Parse
# ---------------------------------------------------------------------------
def parse_nav_data(raw_text: str) -> pd.DataFrame:
    """
    Parse AMFI NAVAll.txt into a DataFrame.

    File format (semicolon-delimited):
        Scheme Code;ISIN Div Payout/ISIN Growth;ISIN Div Reinvestment;Scheme Name;Net Asset Value;Date

    Non-data lines (category headers) have fewer than 6 fields — skipped automatically.
    """
    rows = []

    for line in raw_text.strip().split("\n"):
        line = line.strip()
        if not line:
            continue

        parts = line.split(";")
        if len(parts) < 6:
            continue

        scheme_code_str = parts[0].strip()
        isin_growth     = parts[1].strip()
        isin_div_reinv  = parts[2].strip()
        scheme_name     = parts[3].strip()
        nav_str         = parts[4].strip()
        date_str        = parts[5].strip()

        if not scheme_code_str.isdigit():
            continue

        if nav_str in ("N.A.", "N.A", "-", "", "B.C."):
            continue

        try:
            nav_value = float(nav_str)
        except ValueError:
            continue

        if nav_value <= 0:
            continue

        rows.append({
            "scheme_code"  : int(scheme_code_str),
            "isin_growth"  : isin_growth    if isin_growth    not in ("", "-") else "",
            "isin_div_reinv": isin_div_reinv if isin_div_reinv not in ("", "-") else "",
            "scheme_name"  : scheme_name,
            "nav"          : nav_value,
            "date"         : date_str,   # raw AMFI format e.g. "24-Mar-2026"
        })

    if not rows:
        raise ValueError("No valid NAV rows parsed from AMFI data")

    df = pd.DataFrame(rows)
    print(f"  Parsed {len(df):,} total schemes from AMFI data")
    return df


# ---------------------------------------------------------------------------
# Step 3 — Filter
# ---------------------------------------------------------------------------
def filter_direct_growth(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only Direct Growth schemes (exclude IDCW / Dividend)."""
    name = df["scheme_name"].str.lower()
    mask = name.str.contains("direct", na=False) & \
          ~name.str.contains("idcw|dividend", na=False)
    result = df[mask].copy().reset_index(drop=True)
    print(f"  Filtered to {len(result):,} Direct Growth schemes")
    return result


# ---------------------------------------------------------------------------
# Step 4 — Normalize dates
# ---------------------------------------------------------------------------
def normalize_date(date_str: str) -> str:
    """
    Convert AMFI date string to ISO format YYYY-MM-DD.
    Handles: '24-Mar-2026', '24-03-2026', '24/03/2026'
    """
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%d/%m/%Y"):
        try:
            return datetime.strptime(date_str.strip(), fmt).strftime("%Y-%m-%d")
        except ValueError:
            continue
    raise ValueError(f"Cannot parse AMFI date: '{date_str}'")


# ---------------------------------------------------------------------------
# Step 5 — Smart per-date update (core logic)
# ---------------------------------------------------------------------------
def update_daily_files(df: pd.DataFrame) -> dict:
    """
    Update daily CSV files grouped by each fund's OWN NAV date.

    Logic:
      - Group all funds by their declared NAV date
      - For each date group, merge into existing daily/{date}.csv
        → New funds are ADDED
        → Existing funds are UPDATED (handles NAV corrections)
        → No fund is ever lost
      - Returns summary of what changed

    This ensures:
      - 2026-03-24.csv = complete record for March 24 (including PPFAS added next morning)
      - Each fund's NAV is stored under its correct declared date
    """
    DAILY_DIR.mkdir(parents=True, exist_ok=True)
    summary = {}  # {date: {"added": n, "updated": n, "unchanged": n, "total": n}}

    # Group incoming data by each fund's own NAV date
    for nav_date, group in df.groupby("date"):
        daily_file = DAILY_DIR / f"{nav_date}.csv"
        group = group.reset_index(drop=True)

        if not daily_file.exists():
            # Brand new date — save directly
            group.to_csv(daily_file, index=False)
            summary[nav_date] = {
                "added": len(group), "updated": 0,
                "unchanged": 0, "total": len(group), "action": "created"
            }
            print(f"  Created  {daily_file.name}: {len(group):,} schemes")

        else:
            # Merge into existing file
            try:
                existing = pd.read_csv(daily_file)
            except Exception:
                # Corrupted file — overwrite
                group.to_csv(daily_file, index=False)
                summary[nav_date] = {
                    "added": len(group), "updated": 0,
                    "unchanged": 0, "total": len(group), "action": "overwrite_corrupt"
                }
                print(f"  Replaced {daily_file.name} (was corrupt): {len(group):,} schemes")
                continue

            existing_idx = existing.set_index("scheme_code")
            incoming_idx = group.set_index("scheme_code")

            added     = 0
            updated   = 0
            unchanged = 0

            for scheme_code, row in incoming_idx.iterrows():
                if scheme_code not in existing_idx.index:
                    # New fund not seen before for this date
                    added += 1
                else:
                    existing_nav = existing_idx.at[scheme_code, "nav"]
                    if existing_nav != row["nav"]:
                        # NAV correction by AMC
                        updated += 1
                    else:
                        unchanged += 1

            if added > 0 or updated > 0:
                # Merge: existing + incoming, incoming wins on duplicates
                merged = pd.concat([existing, group], ignore_index=True)
                merged = merged.drop_duplicates(subset=["scheme_code"], keep="last")
                merged = merged.sort_values("scheme_code").reset_index(drop=True)
                merged.to_csv(daily_file, index=False)

                action_parts = []
                if added:   action_parts.append(f"+{added} new")
                if updated: action_parts.append(f"{updated} corrected")
                action_str = ", ".join(action_parts)

                summary[nav_date] = {
                    "added": added, "updated": updated,
                    "unchanged": unchanged, "total": len(merged), "action": "merged"
                }
                print(f"  Updated  {daily_file.name}: {action_str} ({len(merged):,} total)")
            else:
                summary[nav_date] = {
                    "added": 0, "updated": 0,
                    "unchanged": unchanged, "total": len(existing), "action": "skipped"
                }
                print(f"  Skipped  {daily_file.name}: no changes ({unchanged:,} schemes unchanged)")

    return summary


# ---------------------------------------------------------------------------
# Step 6 — Update latest.csv (today's majority-date funds only)
# ---------------------------------------------------------------------------
def update_latest_csv(df: pd.DataFrame) -> str:
    """
    Write latest.csv with only the majority-date funds.
    This gives a clean 'today's NAV' snapshot for quick access.
    Returns the majority date.
    """
    majority_date = df["date"].mode()[0]
    today_df = df[df["date"] == majority_date].copy()
    today_df.to_csv(LATEST_CSV, index=False)
    print(f"  Updated latest.csv: {len(today_df):,} schemes for {majority_date}")
    return majority_date


# ---------------------------------------------------------------------------
# Helpers — scheme master, logging
# ---------------------------------------------------------------------------
def update_scheme_master(df: pd.DataFrame) -> None:
    """Update scheme_master.csv — full Direct Growth list with ISINs (no NAV/date)."""
    master = df[["scheme_code", "isin_growth", "isin_div_reinv", "scheme_name"]].copy()
    master = master.drop_duplicates(subset=["scheme_code"], keep="last")
    master = master.sort_values("scheme_code").reset_index(drop=True)
    SCHEME_MASTER.parent.mkdir(parents=True, exist_ok=True)
    master.to_csv(SCHEME_MASTER, index=False)
    print(f"  Updated scheme_master.csv: {len(master):,} schemes")


def log_run(status: str, majority_date: str, summary: dict, message: str = "") -> None:
    """Append a run entry to fetch_log.json (keeps last 200 entries)."""
    FETCH_LOG.parent.mkdir(parents=True, exist_ok=True)

    entries = []
    if FETCH_LOG.exists():
        try:
            with open(FETCH_LOG) as f:
                entries = json.load(f)
        except Exception:
            entries = []

    if len(entries) >= 200:
        entries = entries[-199:]

    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)

    entries.append({
        "timestamp_utc" : now_utc.isoformat(),
        "timestamp_ist" : now_ist.strftime("%Y-%m-%d %H:%M:%S IST"),
        "status"        : status,
        "majority_date" : majority_date,
        "files_updated" : summary,
        "message"       : message,
    })

    with open(FETCH_LOG, "w") as f:
        json.dump(entries, f, indent=2)


def update_last_success() -> None:
    """Write current timestamp to last_success.txt."""
    LAST_SUCCESS.parent.mkdir(parents=True, exist_ok=True)
    now_utc = datetime.now(timezone.utc)
    now_ist = now_utc.astimezone(IST)
    with open(LAST_SUCCESS, "w") as f:
        f.write(f"{now_utc.isoformat()}\n")
        f.write(f"{now_ist.strftime('%Y-%m-%d %H:%M:%S IST')}\n")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> int:
    print("=" * 60)
    print("AMFI NAV Fetcher")
    print(f"Run time: {datetime.now(IST).strftime('%Y-%m-%d %H:%M:%S IST')}")
    print("=" * 60)

    # 1. Fetch
    try:
        print("\n[1/6] Fetching AMFI NAVAll.txt...")
        raw_text = fetch_amfi_text()
    except ConnectionError as e:
        print(f"\nFATAL: {e}")
        log_run("fetch_failed", "", {}, str(e))
        return 0  # Exit 0 — no failure emails on AMFI downtime

    # 2. Parse
    try:
        print("\n[2/6] Parsing NAV data...")
        all_schemes = parse_nav_data(raw_text)
    except ValueError as e:
        print(f"\nFATAL: {e}")
        log_run("parse_failed", "", {}, str(e))
        return 1

    # 3. Filter
    print("\n[3/6] Filtering Direct Growth schemes...")
    direct_growth = filter_direct_growth(all_schemes)

    if len(direct_growth) == 0:
        print("FATAL: Zero schemes after filtering")
        log_run("filter_empty", "", {}, "Zero Direct Growth schemes")
        return 1

    if len(direct_growth) < MIN_EXPECTED:
        print(f"  WARNING: Only {len(direct_growth):,} schemes (expected >={MIN_EXPECTED:,})")

    # 4. Normalize each fund's own date
    print("\n[4/6] Normalizing NAV dates...")
    try:
        direct_growth["date"] = direct_growth["date"].apply(normalize_date)
    except ValueError as e:
        print(f"FATAL: {e}")
        log_run("date_parse_failed", "", {}, str(e))
        return 1

    # Report date distribution
    date_counts = direct_growth["date"].value_counts()
    majority_date = date_counts.index[0]
    print(f"  Majority date : {majority_date} ({date_counts.iloc[0]:,} funds)")
    if len(date_counts) > 1:
        for d, c in date_counts.iloc[1:].items():
            print(f"  Late declaring: {c:,} fund(s) with date {d}")

    # 5. Update daily files (per-date merge)
    print("\n[5/6] Updating daily files...")
    summary = update_daily_files(direct_growth)

    # 6. Update latest.csv + scheme master
    print("\n[6/6] Updating latest.csv and scheme master...")
    update_latest_csv(direct_growth)
    update_scheme_master(direct_growth)
    log_run("success", majority_date, summary)
    update_last_success()

    # Final summary
    total_files = len(summary)
    created  = sum(1 for s in summary.values() if s["action"] == "created")
    merged   = sum(1 for s in summary.values() if s["action"] == "merged")
    skipped  = sum(1 for s in summary.values() if s["action"] == "skipped")

    print(f"\n{'='*60}")
    print(f"Done — {total_files} date file(s) processed")
    print(f"  Created : {created}  |  Updated : {merged}  |  Unchanged : {skipped}")
    print(f"{'='*60}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
