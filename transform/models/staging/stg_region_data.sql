-- One row per region per hour: pivots EIA's long format into columns.
select
    respondent                                   as region_code,
    any_value(respondent_name)                   as region_name,
    period_utc,
    max(value) filter (where type = 'D')         as demand_mwh,
    max(value) filter (where type = 'DF')        as eia_forecast_mwh,
    max(value) filter (where type = 'NG')        as net_generation_mwh,
    max(value) filter (where type = 'TI')        as interchange_mwh
from {{ source('eia', 'region_data') }}
group by region_code, period_utc
