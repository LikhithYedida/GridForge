-- Fails if any region has the same hour twice.
select region_code, period_utc, count(*) as n
from {{ ref('fct_region_hour') }}
group by 1, 2
having count(*) > 1
