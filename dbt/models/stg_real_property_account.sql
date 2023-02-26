
{{ config(
    indexes=[
      {'columns': ['taxpayer_name', 'address_normalized']},
      {'columns': ['address_normalized']},
    ]
)}}

select 
    rpa.*,
    coalesce(d.address_normalized,
      -- concat_ws ignores nulls
      concat_ws(',', rpa.addr_line, rpa.city_state, rpa.zip_code)
    ) as address_normalized
from {{ ref('int_real_property_account_fix_names') }} rpa
left join {{ ref('int_real_property_account_address_dedupe') }} d
  on rpa.major = d.major
  and rpa.minor = d.minor
  and rpa.addr_line = d.addr_line
  and rpa.city_state = d.city_state
  and rpa.zip_code = d.zip_code
