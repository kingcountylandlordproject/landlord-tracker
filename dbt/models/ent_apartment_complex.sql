select
    sac.major_minor,
    sac.complex_descr, 
    sac.address,
    sum(case when srpa.major_minor is not null then 1 else 0 end) as num_tax_records,
    count(distinct srpa.taxpayer_name) as distinct_taxpayer_names,
    count(distinct srpa_taxpayer.major_minor) as num_parcels_for_taxpayers,
    count(distinct srpa.address_normalized) as distinct_taxpayer_addresses,
    -- TODO: normalize address in complaints
    count(distinct com.record_num) as num_complaints
from {{ ref('stg_apartment_complex') }} sac
left join {{ ref('stg_real_property_account') }} srpa 
    on sac.major_minor = srpa.major_minor 
left join {{ ref('stg_real_property_account') }} srpa_taxpayer 
    on srpa.taxpayer_name = srpa_taxpayer.taxpayer_name 
left join {{ ref('stg_complaints') }} com
    on sac.address_normalized = com.original_address1
group by 
    sac.major_minor,
    sac.complex_descr,
    sac.address
