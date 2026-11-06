"""
GridForge | Phase 2, Step 2 - Pull EIA-930 hourly region data

What it pulls : demand (D), EIA day-ahead demand forecast (DF),
                net generation (NG), total interchange (TI)
For           : the 13 EIA regions + the Lower 48 total
From          : Jan 2019 to now (timestamps are UTC)
Saves to      : data/raw/region_data/YYYY-MM.parquet (one file per month)

Re-runs are incremental: finished months are skipped,
the current and previous month are always refreshed (EIA revises recent data).
"""
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("EIA_API_KEY")
URL = "https://api.eia.gov/v2/electricity/rto/region-data/data/"

REGIONS = ["US48", "CAL", "CAR", "CENT", "FLA", "MIDA", "MIDW",
           "NE", "NW", "NY", "SE", "SW", "TEN", "TEX"]
TYPES = ["D", "DF", "NG", "TI"]
START = (2019, 1)
PAGE = 5000  # EIA API max rows per request
OUT = Path("data/raw/region_data")


def months(start, end):
    y, m = start
    while (y, m) <= end:
        yield y, m
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)


def month_bounds(y, m):
    ny, nm = (y + 1, 1) if m == 12 else (y, m + 1)
    last_day = (datetime(ny, nm, 1) - datetime(y, m, 1)).days
    return f"{y}-{m:02d}-01T00", f"{y}-{m:02d}-{last_day:02d}T23"


def get_page(start, end, offset):
    params = {
        "api_key": API_KEY,
        "frequency": "hourly",
        "data[0]": "value",
        "facets[respondent][]": REGIONS,
        "facets[type][]": TYPES,
        "start": start,
        "end": end,
        "sort[0][column]": "period",
        "sort[0][direction]": "asc",
        "offset": offset,
        "length": PAGE,
    }
    for attempt in range(5):
        try:
            r = requests.get(URL, params=params, timeout=60)
            if 400 <= r.status_code < 500 and r.status_code != 429:
                raise SystemExit(f"EIA rejected the request ({r.status_code}): {r.text[:300]}")
            r.raise_for_status()
            body = r.json()["response"]
            return body["data"], int(body["total"])
        except (requests.RequestException, KeyError, ValueError) as e:
            wait = 5 * 2 ** attempt
            print(f"   retry {attempt + 1}/5 in {wait}s ({e})")
            time.sleep(wait)
    raise RuntimeError(f"Gave up on {start} at offset {offset}")


def pull_month(y, m):
    start, end = month_bounds(y, m)
    rows, offset, total = [], 0, None
    while total is None or offset < total:
        data, total = get_page(start, end, offset)
        if not data:
            break
        rows.extend(data)
        offset += PAGE
        time.sleep(0.2)  # be polite to the API

    if not rows:
        return 0

    for r in rows:  # EIA sometimes sends numbers as strings
        r["value"] = None if r.get("value") is None else float(r["value"])

    df = (
        pl.DataFrame(rows)
        .rename({"respondent-name": "respondent_name",
                 "type-name": "type_name",
                 "value-units": "units"})
        .with_columns(
            (pl.col("period") + ":00").str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M")
              .dt.replace_time_zone("UTC").alias("period_utc"),
            pl.lit(datetime.now(timezone.utc)).alias("pulled_at_utc"),
        )
        .select(["period_utc", "respondent", "respondent_name", "type",
                 "type_name", "value", "units", "pulled_at_utc"])
    )
    df.write_parquet(OUT / f"{y}-{m:02d}.parquet")
    return df.height


def main():
    if not API_KEY:
        raise SystemExit("EIA_API_KEY is missing from .env")
    OUT.mkdir(parents=True, exist_ok=True)

    now = datetime.now(timezone.utc)
    prev = (now.year - 1, 12) if now.month == 1 else (now.year, now.month - 1)
    always_refresh = {(now.year, now.month), prev}

    new_rows = 0
    for y, m in months(START, (now.year, now.month)):
        f = OUT / f"{y}-{m:02d}.parquet"
        if f.exists() and (y, m) not in always_refresh:
            continue
        t0 = time.time()
        n = pull_month(y, m)
        new_rows += n
        print(f"{y}-{m:02d}: {n:>7,} rows  ({time.time() - t0:4.1f}s)")

    print(f"\nPulled {new_rows:,} rows this run.\n")
    print(duckdb.sql(f"""
        select type_name,
               count(*)                   as rows,
               count(distinct respondent) as regions,
               min(period_utc)            as first_hour,
               max(period_utc)            as last_hour
        from read_parquet('{OUT.as_posix()}/*.parquet')
        group by 1
        order by 1
    """))


if __name__ == "__main__":
    main()