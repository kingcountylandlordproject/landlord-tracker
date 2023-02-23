
select 
    major_minor
    ,'apartment_complex' as address_type
    ,address
from {{ ref('stg_apartment_complex') }}
union all
select
    major_minor,
    'residential_building' as address_type,
    address
from {{ ref('stg_residential_building') }}
