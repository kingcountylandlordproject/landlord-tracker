
-- make sure rowcount of stg table = rowcount of raw tables summed

with
table_totals as (
    select 'raw' as table_type, count(*) as total from {{ source('src', 'raw_real_property_account_2022_07_06') }}
    union all 
    select 'raw' as table_type, count(*) as total from {{ source('src', 'raw_real_property_account') }} 
    union all 
    select 'stg' as table_type, count(*) as total from {{ ref('stg_real_property_account') }}
)
,sums as (
    select 
        table_type,
        sum(total) as total
    from table_totals
    group by table_type
)
select *
from sums t1
cross join sums t2
where 
    t1.table_type = 'raw'
    and t2.table_type = 'stg'
    and t1.total != t2.total
