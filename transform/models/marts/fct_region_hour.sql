-- Model-ready demand plus weather, one row per region and UTC hour.
-- Raw values are kept; flagged demand hours get a repaired value:
--   gap of up to 4 hours -> linear interpolation between the nearest good hours
--   longer gap           -> the same hour one week earlier (if that was good)
with base as (
    select *,
           case when demand_flag = 'ok' then demand_mwh end              as demand_clean_mwh,
           case when forecast_flag = 'ok' then eia_forecast_mwh end      as eia_forecast_clean_mwh
    from {{ ref('int_demand_flagged') }}
),

neighbors as (
    select *,
           last_value(demand_clean_mwh ignore nulls) over before_w as prev_val,
           last_value(case when demand_clean_mwh is not null then period_utc end ignore nulls) over before_w as prev_time,
           first_value(demand_clean_mwh ignore nulls) over after_w as next_val,
           first_value(case when demand_clean_mwh is not null then period_utc end ignore nulls) over after_w as next_time,
           lag(demand_clean_mwh, 168) over (partition by region_code order by period_utc) as last_week_val
    from base
    window before_w as (partition by region_code order by period_utc
                        rows between unbounded preceding and 1 preceding),
           after_w  as (partition by region_code order by period_utc
                        rows between 1 following and unbounded following)
),

repaired as (
    select *,
           case
               when demand_clean_mwh is not null then 'none'
               when prev_val is not null and next_val is not null
                    and datediff('hour', prev_time, next_time) <= 5 then 'interpolated'
               when last_week_val is not null then 'last_week'
               else 'unrepaired'
           end as impute_method
    from neighbors
)

select
    r.region_code,
    r.region_name,
    r.period_utc,
    r.demand_mwh                as demand_raw_mwh,
    r.demand_flag,
    case r.impute_method
        when 'none' then r.demand_clean_mwh
        when 'interpolated' then r.prev_val + (r.next_val - r.prev_val)
             * datediff('hour', r.prev_time, r.period_utc) / datediff('hour', r.prev_time, r.next_time)
        when 'last_week' then r.last_week_val
    end                         as demand_model_mwh,
    r.impute_method,
    r.eia_forecast_mwh          as eia_forecast_raw_mwh,
    r.forecast_flag,
    r.eia_forecast_clean_mwh,
    r.net_generation_mwh,
    r.interchange_mwh,
    w.temperature_f,
    w.dew_point_f,
    w.solar_wm2,
    w.cooling_degree_hours,
    w.heating_degree_hours,
    w.metros_reporting
from repaired r
left join {{ ref('int_region_weather') }} w
  on w.region_code = r.region_code and w.period_utc = r.period_utc
