-- Population-weighted weather per region and hour.
-- Each metro counts in proportion to its population, so a heat wave in Dallas
-- moves Texas more than one in Austin. US48 uses every metro.
with w as (
    select s.*, m.population_m
    from {{ ref('stg_weather') }} s
    join {{ ref('metros') }} m on m.metro = s.metro
),

regional as (
    select region_code, period_utc,
           sum(temperature_f * population_m) / sum(population_m) filter (where temperature_f is not null) as temperature_f,
           sum(dew_point_f * population_m) / sum(population_m) filter (where dew_point_f is not null)     as dew_point_f,
           sum(solar_wm2 * population_m) / sum(population_m) filter (where solar_wm2 is not null)         as solar_wm2,
           count(temperature_f) as metros_reporting
    from w
    group by 1, 2
),

national as (
    select 'US48' as region_code, period_utc,
           sum(temperature_f * population_m) / sum(population_m) filter (where temperature_f is not null),
           sum(dew_point_f * population_m) / sum(population_m) filter (where dew_point_f is not null),
           sum(solar_wm2 * population_m) / sum(population_m) filter (where solar_wm2 is not null),
           count(temperature_f)
    from w
    group by 2
),

unioned as (
    select * from regional
    union all
    select * from national
)

select *,
       greatest(temperature_f - 65, 0) as cooling_degree_hours,
       greatest(65 - temperature_f, 0) as heating_degree_hours
from unioned
