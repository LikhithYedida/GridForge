-- Fails if any full UTC day (not the first or the latest) has fewer than 24 rows for a region.
with days as (
    select region_code, cast(period_utc as date) as day_utc, count(*) as hours
    from {{ ref('fct_region_hour') }}
    group by 1, 2
),
edges as (
    select region_code, min(day_utc) as first_day, max(day_utc) as last_day
    from days group by 1
)
select d.*
from days d join edges e using (region_code)
where d.day_utc > e.first_day and d.day_utc < e.last_day and d.hours <> 24
