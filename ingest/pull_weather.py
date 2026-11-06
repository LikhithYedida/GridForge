"""
GridForge | Phase 2, Step 5 - Pull hourly weather for 66 US metros

What it pulls : temperature (F), dew point (F), solar radiation (W/m2)
For           : every metro in transform/seeds/metros.csv
From          : Jan 2019 to 7 days ago (Open-Meteo archive lags ~5 days), UTC
Saves to      : data/raw/weather/<metro>.parquet (one file per metro)

Free-tier budget: Open-Meteo weights a call as
    locations x (days / 14) x (variables / 10)
One metro x ~7.8 years x 3 variables ~= 61 weighted calls, so 66 metros ~= 4,000.
Limits are 600/min, 5,000/hour, 10,000/day, so the script waits between metros.
Re-runs skip metros already saved. Use --refresh to pull everything again.
"""
import re
import sys
import time
from datetime import date, timedelta
from pathlib import Path

import polars as pl
import requests

URL = "https://archive-api.open-meteo.com/v1/archive"
METROS = Path("transform/seeds/metros.csv")
OUT = Path("data/raw/weather")
VARIABLES = ["temperature_2m", "dew_point_2m", "shortwave_radiation"]
START = date(2019, 1, 1)
PAUSE_SECONDS = 8  # ~7 metros a minute -> ~430 weighted calls/min, under the 600 limit


def slug(name):
    return re.sub(r"[^a-z0-9]+", "_", name.lower()).strip("_")


def fetch(lat, lon, end):
    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START.isoformat(),
        "end_date": end.isoformat(),
        "hourly": ",".join(VARIABLES),
        "temperature_unit": "fahrenheit",
        "timezone": "GMT",
    }
    for attempt in range(4):
        r = requests.get(URL, params=params, timeout=120)
        if r.status_code == 429:
            wait = 65 * (attempt + 1)
            print(f"   rate limit hit, waiting {wait}s ...")
            time.sleep(wait)
            continue
        if r.status_code >= 400:
            raise SystemExit(f"Open-Meteo error {r.status_code}: {r.text[:300]}")
        return r.json()["hourly"]
    raise SystemExit("Still rate-limited. Wait an hour and run again: finished metros are kept.")


def main():
    refresh = "--refresh" in sys.argv
    OUT.mkdir(parents=True, exist_ok=True)
    metros = pl.read_csv(METROS)
    end = date.today() - timedelta(days=7)

    todo = [m for m in metros.iter_rows(named=True)
            if refresh or not (OUT / f"{slug(m['metro'])}.parquet").exists()]
    print(f"{len(metros)} metros, {len(todo)} to pull, {START} to {end}\n")

    for i, m in enumerate(todo, 1):
        t0 = time.time()
        h = fetch(m["latitude"], m["longitude"], end)
        df = (
            pl.DataFrame({
                "time": h["time"],
                "temperature_f": h["temperature_2m"],
                "dew_point_f": h["dew_point_2m"],
                "solar_wm2": h["shortwave_radiation"],
            }, strict=False)
            .with_columns(
                pl.col("time").str.strptime(pl.Datetime("us"), "%Y-%m-%dT%H:%M")
                  .dt.replace_time_zone("UTC").alias("period_utc"),
                pl.lit(m["metro"]).alias("metro"),
                pl.lit(m["region_code"]).alias("region_code"),
                pl.col("temperature_f", "dew_point_f", "solar_wm2").cast(pl.Float64),
            )
            .select(["period_utc", "region_code", "metro",
                     "temperature_f", "dew_point_f", "solar_wm2"])
        )
        df.write_parquet(OUT / f"{slug(m['metro'])}.parquet")
        print(f"[{i:>2}/{len(todo)}] {m['region_code']:<4} {m['metro']:<16} "
              f"{df.height:>7,} hours  ({time.time() - t0:4.1f}s)")
        if i < len(todo):
            time.sleep(PAUSE_SECONDS)

    files = list(OUT.glob("*.parquet"))
    print(f"\nDone. {len(files)} metro files in {OUT.as_posix()}/")


if __name__ == "__main__":
    main()