
-- combine multiple years of data

select
    concat(major, minor) as major_minor
	,acct_nbr
	,major
	,minor
	,{{ xtrim('taxpayer_name') }}
	,{{ xtrim('attn_line') }}
	,{{ xtrim('addr_line') }}
	,{{ xtrim('city_state') }}
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
from {{ source('src', 'raw_real_property_account_2022_07_06') }}

union all 

select
    concat(major, minor) as major_minor
	,acct_nbr
	,major
	,minor
	,null as taxpayer_name
	,{{ xtrim('attn_line') }}
	,{{ xtrim('addr_line') }}
	,{{ xtrim('city_state') }}
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
from {{ source('src', 'raw_real_property_account') }}
