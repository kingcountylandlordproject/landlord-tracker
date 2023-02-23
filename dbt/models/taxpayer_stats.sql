
-- count distincts are super slow

select
	rpa.taxpayer_name,
	count(distinct rpa.major_minor) as num_properties,
	count(distinct rpa.addr_hash) as num_taxpayer_addresses,
    count(distinct rpa2.major_minor) as num_properties_taxpayer_addr
from {{ ref('real_property_account') }} rpa 
left join {{ ref('real_property_account') }} rpa2
	on rpa.addr_hash = rpa2.addr_hash 
group by rpa.taxpayer_name
