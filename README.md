# NAV Data Dump

Automated daily scraper for Indian mutual fund NAV data from AMFI (Association of Mutual Funds in India).

Builds a free, self-maintained historical NAV database — no third-party API dependency.

## What It Does

- **Scrapes** [AMFI NAVAll.txt](https://www.amfiindia.com/spages/NAVAll.txt) — the official daily NAV file published by AMFI
- **Filters** to Direct Growth schemes only (~2000+ funds)
- **Stores** daily CSVs + monthly parquet files with scheme codes, ISINs, names, and NAVs
- **Runs automatically** 3× daily via GitHub Actions (9:30 PM, 10:30 PM, 11:30 PM IST)
- **Handles** holidays, late publishes, NAV corrections, and AMFI downtime gracefully

## Data Schema

| Column | Type | Description |
|--------|------|-------------|
| `scheme_code` | int | AMFI scheme code (unique fund identifier) |
| `isin_growth` | str | ISIN for growth option |
| `isin_div_reinv` | str | ISIN for dividend reinvestment option |
| `scheme_name` | str | Full fund name |
| `nav` | float | Net Asset Value |
| `date` | str | NAV date (YYYY-MM-DD) |

## Directory Structure

```
data/
├── daily/           # Last 7 days of CSVs (~80KB each)
│   └── 2026-03-24.csv
├── monthly/         # Consolidated parquet per month (~100KB each)
│   └── 2026-03.parquet
└── latest.csv       # Always the most recent successful scrape

metadata/
├── scheme_master.csv    # Full Direct Growth scheme list with ISINs
├── scrape_log.json      # Run history (last 100 entries)
└── last_success.txt     # Timestamp of last successful scrape
```

## How to Use the Data

### Latest NAV (all funds, single day)
```python
import pandas as pd

# Via raw GitHub URL:
url = "https://raw.githubusercontent.com/{YOUR_USERNAME}/nav-data-dump/main/data/latest.csv"
df = pd.read_csv(url)

# Get NAV for a specific fund:
fund = df[df['scheme_code'] == 119551]
print(f"NAV: {fund['nav'].iloc[0]} on {fund['date'].iloc[0]}")

# Search by ISIN:
fund = df[df['isin_growth'] == 'INF209KA12Z1']
```

### Historical NAV (full month)
```python
# Monthly parquets contain all daily NAVs for that month:
url = "https://raw.githubusercontent.com/{YOUR_USERNAME}/nav-data-dump/main/data/monthly/2026-03.parquet"
df = pd.read_parquet(url)

# Filter for one fund's history:
fund_history = df[df['scheme_code'] == 119551].sort_values('date')
```

## Setup

1. **Fork** this repo (or create your own and copy the files)
2. Go to **Settings → Actions → General** and ensure "Read and write permissions" is enabled under "Workflow permissions"
3. **Enable GitHub Actions** — the workflow runs automatically on schedule
4. **Test manually**: Go to Actions → "Scrape AMFI NAV" → "Run workflow"
5. Verify a commit appears with `data/daily/YYYY-MM-DD.csv` and `data/latest.csv`

## Schedule

| Time (IST) | Time (UTC) | Purpose |
|-------------|------------|---------|
| 9:30 PM | 4:00 PM | First attempt — catches early publishes |
| 10:30 PM | 5:00 PM | Second attempt — catches normal publishes |
| 11:30 PM | 6:00 PM | Third attempt — catches late publishes |

Runs all 7 days. Debt/liquid funds may publish Saturday NAVs.

## Edge Cases

- **AMFI down**: 3 retries with 60s gaps. Logs failure, exits cleanly (no alert spam).
- **Market holiday**: Detects same date as previous day → skips saving duplicate.
- **NAV corrections**: Later scrape overwrites if more schemes found. Monthly parquet keeps latest value per (scheme, date).
- **Fund launch/closure**: New funds appear automatically; closed funds stop appearing. Historical data is preserved.
- **GitHub Actions delayed**: 5-30 min delay is normal. Three attempts across 2 hours ensure we catch the publish.

## Data Retention

- **Daily CSVs**: Last 7 days (older ones auto-deleted by consolidation script)
- **Monthly parquets**: Kept indefinitely (~1.2 MB/year)
- **Scrape log**: Last 100 entries

## License

NAV data is published freely by AMFI. This repo automates collection for personal use.
