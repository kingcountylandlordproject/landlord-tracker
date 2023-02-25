{{ config(materialization='view') }}

select 
    major
    ,minor
    ,addr_line
    ,city_state
    ,zip_code
    ,address_normalized
from {{ source('pre', 'pre_real_property_account_address') }}
group by
    major
    ,minor
    ,addr_line
    ,city_state
    ,zip_code
    ,address_normalized
