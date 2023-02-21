
-- rough breakdown of parcel counts by their type (i.e. whether they can join to other tables).
-- there's some overlaps among the files, which is why they add up to more than total row count of parcels

select
	count(*) as total_parcels
	,sum(case when rac.major is not null then 1 else 0 end) as is_apt
	,sum(case when rrb.major is not null then 1 else 0 end) as is_residential
	,sum(case when rcc.major is not null then 1 else 0 end) as is_condo
	,sum(case when rcb.major is not null then 1 else 0 end) as is_comm
    ,sum(case when rvl.major is not null then 1 else 0 end) as is_vacant
    ,sum(case
        when rac.major is null
        and rrb.major is null
        and rcc.major is null
        and rcb.major is null
        and rvl.major is null then 1 else 0 end) as uncategorized
from {{ ref('parcel') }} p
left join {{ source('src', 'raw_apartment_complex') }} rac
	on p.major_minor  = concat(rac.major, rac.minor)
left join {{ source('src', 'raw_residential_building') }} rrb
	on p.major_minor  = concat(rrb.major, rrb.minor)
left join {{ source('src', 'raw_condo_complex') }} rcc
	on p.major  = concat(rcc.major)
left join {{ source('src', 'raw_commercial_building') }} rcb
	on p.major_minor  = concat(rcb.major, rcb.minor)
left join {{ source('src', 'raw_vacant_lot') }} rvl
	on p.major_minor  = concat(rvl.major, rvl.minor)
