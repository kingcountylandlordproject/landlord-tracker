
with addresses as (
    -- find addressess with more than 1 distinct name
	select
        addr_line
        ,zip_code
	from {{ ref('real_property_account') }}
	group by
        addr_line
        ,zip_code
	having count(distinct taxpayer_name) > 1
)
select
    rpa.*
from {{ ref('real_property_account') }} rpa
join addresses a
	on rpa.addr_line = a.addr_line
