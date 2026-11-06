-- 1. Build a complete hourly spine per region (so a missing hour becomes a visible row).
-- 2. Flag every demand and EIA-forecast value: ok, missing, non_positive or spike.
--    A spike is a value more than 50% above or below its trailing 7-day median.
with bounds as (
    select region_code,
           min(period_utc) as first_hour,
           max(period_utc) filter (where demand_mwh is not null) as last_hour
    from {{ ref('stg_region_data') }}
    group by region_code
),

spine as (
    select b.region_code, h.period_utc
    from bounds b,
         generate_series(b.first_hour, b.last_hour, interval 1 hour) as h(period_utc)
),

joined as (
    select s.region_code,
           s.period_utc,
           r.region_name,
           r.demand_mwh,
           r.eia_forecast_mwh,
           r.net_generation_mwh,
           r.interchange_mwh
    from spine s
    left join {{ ref('stg_region_data') }} r
      on r.region_code = s.region_code and r.period_utc = s.period_utc
),

medians as (
    select *,
           median(demand_mwh) over w       as demand_median_7d,
           median(eia_forecast_mwh) over w as forecast_median_7d
    from joined
    window w as (partition by region_code order by period_utc
                 rows between 168 preceding and 1 preceding)
)

select
    region_code,
    max(region_name) over (partition by region_code) as region_name,
    period_utc,
    demand_mwh,
    eia_forecast_mwh,
    net_generation_mwh,
    interchange_mwh,
    demand_median_7d,
    case
        when demand_mwh is null then 'missing'
        when demand_mwh <= 0 then 'non_positive'
        when demand_median_7d > 0
             and (demand_mwh / demand_median_7d > 1.5 or demand_mwh / demand_median_7d < 0.5) then 'spike'
        else 'ok'
    end as demand_flag,
    case
        when eia_forecast_mwh is null then 'missing'
        when eia_forecast_mwh <= 0 then 'non_positive'
        when forecast_median_7d > 0
             and (eia_forecast_mwh / forecast_median_7d > 1.5 or eia_forecast_mwh / forecast_median_7d < 0.5) then 'spike'
        else 'ok'
    end as forecast_flag
from medians
