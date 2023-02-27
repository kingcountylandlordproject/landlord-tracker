
select
	by_name.taxpayer_name
	,by_name.num_properties
	,by_name.num_taxpayer_addresses
	,by_addr.num_properties_taxpayer_addr
from {{ ref('int_taxpayer_by_name') }} by_name
left join {{ ref('int_taxpayer_by_addr') }} by_addr
	on by_name.taxpayer_name = by_addr.taxpayer_name
