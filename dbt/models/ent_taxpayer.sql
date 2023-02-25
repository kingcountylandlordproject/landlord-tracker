
-- count distincts are super slow

select
	rpa.taxpayer_name,
	count(distinct rpa.major_minor) as num_properties,
	count(distinct rpa.address_normalized) as num_taxpayer_addresses,
    count(distinct rpa2.major_minor) as num_properties_taxpayer_addr
from {{ ref('stg_real_property_account') }} rpa 
left join {{ ref('stg_real_property_account') }} rpa2
	on rpa.address_normalized = rpa2.address_normalized 
group by rpa.taxpayer_name
