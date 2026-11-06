-- Fails on physically impossible regional weather values.
select *
from {{ ref('int_region_weather') }}
where temperature_f not between -60 and 130
   or dew_point_f   not between -80 and 95
   or solar_wm2     not between 0 and 1400
