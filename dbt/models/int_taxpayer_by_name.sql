select
    taxpayer_name,
    count(distinct major_minor) as num_properties,
    count(distinct address_normalized) as num_taxpayer_addresses
from {{ ref('stg_real_property_account') }}
group by taxpayer_name
