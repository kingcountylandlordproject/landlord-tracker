
{{ config(materialization='view') }}

SELECT
    major_minor
    ,acct_nbr
    ,major
    ,minor
    ,coalesce(taxpayer_name, 'UNKNOWN') as taxpayer_name
    ,attn_line
    ,addr_line
    ,city_state
    ,zip_code
    ,levy_code
    ,tax_stat
    ,bill_yr
    ,new_construction_flag
    ,tax_val_reason
    ,appr_land_val
    ,appr_imps_val
    ,taxable_land_val
    ,taxable_imps_val
    ,address_normalized
from {{ ref('stg_real_property_account') }}
