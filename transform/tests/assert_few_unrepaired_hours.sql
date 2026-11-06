-- Fails if more than 0.1% of a region's hours could not be repaired.
select region_code,
       count(*) filter (where impute_method = 'unrepaired') as unrepaired,
       count(*) as total
from {{ ref('fct_region_hour') }}
group by 1
having count(*) filter (where impute_method = 'unrepaired') > 0.001 * count(*)
