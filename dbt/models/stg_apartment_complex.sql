select
    ac.*
    ,COALESCE(a.address_normalized, a.address) as address_normalized
from {{ ref('int_apartment_complex') }} ac
left join {{ source('pre', 'pre_apartment_complex_address') }} a 
    on a.major = ac.major 
    and a.minor = ac.minor
