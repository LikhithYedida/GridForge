"""
GridForge | Phase 2, Step 3 - Build the warehouse and audit data quality

1. Loads every monthly Parquet file into DuckDB: warehouse/gridforge.duckdb -> raw.region_data
2. Runs five quality checks and prints a report
3. Saves each check as a CSV in reports/quality/ (evidence for the README)

Checks
  A. Completeness  - missing hours, duplicate hours, nulls per region and type
  B. Impossible     - zero or negative demand / forecast values
  C. Spikes         - demand more than 50% away from its trailing 7-day median
  D. Daily coverage - days where a region has fewer than 24 demand hours
  E. Benchmark      - accuracy of EIA's own day-ahead forecast (the number GridForge must beat)
"""
from pathlib import Path

import duckdb

RAW = Path("data/raw/region_data")
DB = Path("warehouse/gridforge.duckdb")
REPORTS = Path("reports/quality")


def section(title):
    print(f"\n{'=' * 70}\n{title}\n{'=' * 70}")


def run(con, name, title, sql):
    section(title)
    rel = con.sql(sql)
    rel.show(max_rows=40)
    rel.write_csv(str(REPORTS / f"{name}.csv"))


def main():
    DB.parent.mkdir(parents=True, exist_ok=True)
    REPORTS.mkdir(parents=True, exist_ok=True)

    con = duckdb.connect(str(DB))
    con.execute("SET TimeZone = 'UTC'")
    con.execute("CREATE SCHEMA IF NOT EXISTS raw")
    con.execute(f"""
        CREATE OR REPLACE TABLE raw.region_data AS
        SELECT * FROM read_parquet('{RAW.as_posix()}/*.parquet')
    """)
    n = con.execute("SELECT count(*) FROM raw.region_data").fetchall()[0][0]
    section(f"Loaded {n:,} rows into {DB.as_posix()} -> raw.region_data")

    run(con, "a_completeness", "A. Completeness by region and type (worst first)", """
        WITH s AS (
            SELECT respondent, type,
                   count(*)                                   AS rows,
                   count(DISTINCT period_utc)                 AS distinct_hours,
                   datediff('hour', min(period_utc), max(period_utc)) + 1 AS expected_hours,
                   count(*) FILTER (WHERE value IS NULL)      AS null_values
            FROM raw.region_data
            GROUP BY 1, 2
        )
        SELECT respondent, type, rows, expected_hours,
               expected_hours - distinct_hours AS missing_hours,
               rows - distinct_hours           AS duplicate_hours,
               null_values,
               round(100.0 * (distinct_hours - null_values) / expected_hours, 3) AS pct_usable
        FROM s
        ORDER BY pct_usable, respondent, type
    """)

    run(con, "b_impossible", "B. Zero or negative demand / day-ahead forecast", """
        SELECT respondent, type,
               count(*) AS bad_rows,
               min(period_utc) AS first_seen,
               max(period_utc) AS last_seen
        FROM raw.region_data
        WHERE type IN ('D', 'DF') AND value <= 0
        GROUP BY 1, 2
        ORDER BY bad_rows DESC
    """)

    run(con, "c_spikes", "C. Demand spikes (>50% away from trailing 7-day median)", """
        WITH d AS (
            SELECT respondent, period_utc, value,
                   median(value) OVER (
                       PARTITION BY respondent ORDER BY period_utc
                       ROWS BETWEEN 168 PRECEDING AND 1 PRECEDING
                   ) AS med_7d
            FROM raw.region_data
            WHERE type = 'D' AND value IS NOT NULL
        )
        SELECT respondent,
               count(*) FILTER (WHERE med_7d > 0 AND abs(value - med_7d) / med_7d > 0.5) AS spike_hours,
               round(100.0 * spike_hours / count(*), 4) AS pct_of_hours,
               max(value / nullif(med_7d, 0)) FILTER (WHERE med_7d > 0) AS worst_ratio
        FROM d
        GROUP BY 1
        ORDER BY spike_hours DESC
    """)

    run(con, "d_daily_coverage", "D. Days with fewer than 24 demand hours", """
        WITH h AS (
            SELECT respondent, CAST(period_utc AS DATE) AS day_utc,
                   count(*) FILTER (WHERE value IS NOT NULL) AS hours
            FROM raw.region_data
            WHERE type = 'D'
            GROUP BY 1, 2
        )
        SELECT respondent,
               count(*) FILTER (WHERE hours < 24) AS short_days,
               min(hours) AS fewest_hours_in_a_day
        FROM h
        WHERE day_utc < (SELECT max(CAST(period_utc AS DATE)) FROM raw.region_data WHERE type = 'D')
        GROUP BY 1
        ORDER BY short_days DESC
    """)

    run(con, "e_eia_benchmark", "E. EIA day-ahead forecast accuracy, last 12 months (the bar to beat)", """
        WITH p AS (
            SELECT respondent, period_utc,
                   max(value) FILTER (WHERE type = 'D')  AS actual,
                   max(value) FILTER (WHERE type = 'DF') AS eia_forecast
            FROM raw.region_data
            WHERE period_utc >= (SELECT max(period_utc) FROM raw.region_data WHERE type = 'D') - INTERVAL 365 DAY
            GROUP BY 1, 2
        )
        SELECT respondent,
               count(*) AS hours_compared,
               round(100 * avg(abs(eia_forecast - actual) / actual), 2)    AS mape_pct,
               round(100 * median(abs(eia_forecast - actual) / actual), 2) AS median_ape_pct,
               round(100 * avg((eia_forecast - actual) / actual), 2)       AS bias_pct
        FROM p
        WHERE actual > 0 AND eia_forecast IS NOT NULL
        GROUP BY 1
        ORDER BY mape_pct
    """)

    con.close()
    section(f"Done. Warehouse: {DB.as_posix()}   Reports: {REPORTS.as_posix()}/")


if __name__ == "__main__":
    main()