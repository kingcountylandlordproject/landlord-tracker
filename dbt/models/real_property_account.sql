
-- TODO: this model should be generated in Python to normalize addresses

{{ config(
    indexes=[
      {'columns': ['addr_hash']},
      {'columns': ['taxpayer_name']},
    ]
)}}

select 
    *,
    md5(concat(addr_line, '-', zip_code)) as addr_hash
from {{ ref('int_real_property_account_fix_names') }}
