-- Fails if a region is missing weather for more than 0.1% of the hours where weather exists.
with span as (
    select min(period_utc) as first_hour, max(period_utc) as last_hour
    from {{ ref('int_region_weather') }}
)
select f.region_code,
       count(*) filter (where f.temperature_f is null) as hours_without_weather,
       count(*) as hours
from {{ ref('fct_region_hour') }} f, span s
where f.period_utc between s.first_hour and s.last_hour
group by 1
having count(*) filter (where f.temperature_f is null) > 0.001 * count(*)
