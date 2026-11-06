-- Fails if any repaired demand value is zero or negative.
select region_code, period_utc, demand_model_mwh
from {{ ref('fct_region_hour') }}
where demand_model_mwh <= 0
