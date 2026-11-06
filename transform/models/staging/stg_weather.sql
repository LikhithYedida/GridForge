-- Hourly weather per metro (UTC), straight from the Open-Meteo Parquet files.
select
    period_utc,
    region_code,
    metro,
    temperature_f,
    dew_point_f,
    solar_wm2
from {{ source('openmeteo', 'weather_hourly') }}
