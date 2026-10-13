import os
import sys
import io
import csv
import zipfile
import datetime
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

symbol = "ZRXUSDT"
HEADERS = [
    "timestamp", "datetime_utc", "open", "high", "low", "close",
    "volume", "quote_volume", "trade_count", "taker_buy_volume", "taker_buy_quote_volume"
]

now_utc = datetime.datetime.now(datetime.timezone.utc)
current_year = now_utc.year
current_month = now_utc.month
yesterday = now_utc - datetime.timedelta(days=1)

if current_month == 1:
    prev_year = current_year - 1
    prev_month = 12
else:
    prev_year = current_year
    prev_month = current_month - 1

print(f"[{datetime.datetime.now()}] Starting dynamic ingestion for {symbol}...")
print(f"Current UTC: {now_utc.strftime('%Y-%m-%d %H:%M:%S')}")

def parse_kline_content(content):
    rows = []
    for r in csv.reader(io.StringIO(content)):
        if not r: continue
        try:
            raw_ts = int(r[0])
            ts = raw_ts // 1000 if len(str(raw_ts)) > 13 else raw_ts
            dt_utc = datetime.datetime.fromtimestamp(ts / 1000, tz=datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
            rows.append([ts, dt_utc, r[1], r[2], r[3], r[4], r[5], r[7], r[8], r[9], r[10]])
        except Exception:
            continue
    return rows

def fetch_zip_url(url, retries=2, timeout=20):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    for attempt in range(retries + 1):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                data = resp.read()
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                name = z.namelist()[0]
                return z.read(name).decode("utf-8")
        except Exception:
            if attempt == retries:
                return None

def write_csv_atomic(csv_path, rows):
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    tmp_path = csv_path + ".tmp"
    with open(tmp_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow(HEADERS)
        w.writerows(rows)
    os.replace(tmp_path, csv_path)

# 1. CHECK PREVIOUS MONTH ROLLOVER
prev_csv = os.path.join("1m", str(prev_year), f"{prev_month:02d}", f"{symbol}_1m_{prev_year}-{prev_month:02d}.csv")
prev_monthly_url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{prev_year}-{prev_month:02d}.zip"
monthly_content = fetch_zip_url(prev_monthly_url)
if monthly_content:
    p_rows = parse_kline_content(monthly_content)
    if p_rows:
        write_csv_atomic(prev_csv, p_rows)
        print(f"Finalized previous month {prev_year}-{prev_month:02d} with official monthly archive ({len(p_rows)} candles).")

# 2. CHECK MISSING PAST MONTHS (Historical backfill)
past_tasks = []
for y in range(2017, current_year + 1):
    for m in range(1, 13):
        if (y, m) < (current_year, current_month):
            csv_path = os.path.join("1m", str(y), f"{m:02d}", f"{symbol}_1m_{y}-{m:02d}.csv")
            if not os.path.exists(csv_path) or os.path.getsize(csv_path) < 1024:
                past_tasks.append((y, m))

if past_tasks:
    print(f"Downloading {len(past_tasks)} missing past monthly archives...")
    def fetch_past_month(y, m):
        url = f"https://data.binance.vision/data/spot/monthly/klines/{symbol}/1m/{symbol}-1m-{y}-{m:02d}.zip"
        content = fetch_zip_url(url)
        if content:
            r = parse_kline_content(content)
            if r:
                t_path = os.path.join("1m", str(y), f"{m:02d}", f"{symbol}_1m_{y}-{m:02d}.csv")
                write_csv_atomic(t_path, r)
                return (y, m, len(r))
        return None

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(fetch_past_month, y, m) for y, m in past_tasks]
        for f in as_completed(futures):
            res = f.result()
            if res:
                print(f"Ingested missing month {res[0]}-{res[1]:02d}: {res[2]} candles")
else:
    print("All past historical months verified on disk.")

# 3. ONGOING CURRENT MONTH INGESTION
curr_dir = os.path.join("1m", str(current_year), f"{current_month:02d}")
curr_csv = os.path.join(curr_dir, f"{symbol}_1m_{current_year}-{current_month:02d}.csv")

curr_data = {}
if os.path.exists(curr_csv):
    try:
        with open(curr_csv, "r") as f:
            reader = csv.reader(f)
            header = next(reader, None)
            for r in reader:
                if r:
                    curr_data[int(r[0])] = r
    except Exception:
        curr_data = {}

print(f"Existing current month ({current_year}-{current_month:02d}) candles: {len(curr_data):,}")

max_day = yesterday.day if (yesterday.year == current_year and yesterday.month == current_month) else 0

if max_day > 0:
    print(f"Syncing daily archives for days 1 to {max_day} of {current_year}-{current_month:02d}...")
    def fetch_daily_day(d):
        url = f"https://data.binance.vision/data/spot/daily/klines/{symbol}/1m/{symbol}-1m-{current_year}-{current_month:02d}-{d:02d}.zip"
        content = fetch_zip_url(url)
        if content:
            return parse_kline_content(content)
        return []

    added_candles = 0
    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {executor.submit(fetch_daily_day, d): d for d in range(1, max_day + 1)}
        for f in as_completed(futures):
            day_rows = f.result()
            for r in day_rows:
                ts = int(r[0])
                if ts not in curr_data:
                    curr_data[ts] = r
                    added_candles += 1
    print(f"Added {added_candles:,} new candles for {current_year}-{current_month:02d}.")

if curr_data:
    sorted_rows = [curr_data[ts] for ts in sorted(curr_data.keys())]
    write_csv_atomic(curr_csv, sorted_rows)

# 4. RECALCULATE DATASET ACCURACY ACROSS DISK
all_files = []
for root, dirs, files in os.walk("1m"):
    for file in sorted(files):
        if file.endswith(".csv"):
            all_files.append(os.path.join(root, file))

all_files.sort()
total_months = len(all_files)
total_candles = 0
earliest_dt = "N/A"
latest_dt = "N/A"

if all_files:
    try:
        with open(all_files[0], "r") as f:
            reader = csv.reader(f)
            next(reader, None)
            first_row = next(reader, None)
            if first_row: earliest_dt = first_row[1]
    except Exception: pass

    try:
        with open(all_files[-1], "r") as f:
            lines = [l for l in csv.reader(f) if l]
            if len(lines) > 1:
                latest_dt = lines[-1][1]
    except Exception: pass

    for fp in all_files:
        try:
            with open(fp, "r") as f:
                total_candles += max(0, sum(1 for _ in f) - 1)
        except Exception: pass

print(f"Final Dataset Summary: {total_months} months | {total_candles:,} candles | Range: {earliest_dt} to {latest_dt}")

# 5. WRITE UP-TO-DATE README.MD
readme = f"""# {symbol} 1-Minute Historical Kline Dataset

An institutional-grade, open-source archive of cleaned, standardized, and validated **1-minute intraday historical kline data** for **{symbol}** (Binance Spot).

---

## 📊 Dataset Specifications
- **Symbol:** `{symbol}`
- **Market:** Binance Spot
- **Timeframe:** 1 Minute (`1m`)
- **Total Months:** `{total_months}` months
- **Total Candles:** `{total_candles:,}` 1-minute candles
- **Historical Range:** `{earliest_dt}` to `{latest_dt}` (UTC)
- **Schema:** `timestamp, datetime_utc, open, high, low, close, volume, quote_volume, trade_count, taker_buy_volume, taker_buy_quote_volume`

---

## 🏛️ Directory Hierarchy
```text
{symbol}-Historical-Data/
├── README.md
└── 1m/
    └── <YYYY>/
        └── <MM>/
            └── {symbol}_1m_<YYYY-MM>.csv
```

---

## ⚡ 1-Line Pandas Stream Loader
Load any month directly from GitHub's raw CDN without cloning the repository:

```python
import pandas as pd

url = "https://raw.githubusercontent.com/parshant231234-eng/{symbol}-Historical-Data/main/1m/{current_year}/{current_month:02d}/{symbol}_1m_{current_year}-{current_month:02d}.csv"
df = pd.read_csv(url)
print(df.head())
```
"""

with open("README.md", "w") as f:
    f.write(readme)
