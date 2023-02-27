-- count number of properties for ALL of a taxpayer name's addresses

-- we do this to avoid a count distinct which takes a really long time
with t as (
    select
        t.taxpayer_name,
        t2.major_minor
    from {{ ref('stg_real_property_account') }} t
    left join {{ ref('stg_real_property_account') }} t2
        on t.address_normalized = t2.address_normalized
    group by
        t.taxpayer_name,
        t2.major_minor
)
select
    taxpayer_name,
    count(*) as num_properties_taxpayer_addr
from t
group by
    taxpayer_name
