
with names as (
	-- get taxpayer_name from older file so we can merge it in
	select
		acct_nbr
		,taxpayer_name
	from {{ source('src', 'raw_real_property_account_2022_07_06') }}
	group by
		acct_nbr
		,taxpayer_name
)
select
	rrpa.acct_nbr
	,CONCAT(rrpa.major, rrpa.minor) as major_minor
	,rrpa.major
	,rrpa.minor
	,{{ xtrim('names.taxpayer_name') }}
	,{{ xtrim('rrpa.attn_line') }}
	,{{ xtrim('rrpa.addr_line') }}
	,{{ xtrim('rrpa.city_state') }}
	,rrpa.zip_code
	,rrpa.levy_code
	,rrpa.tax_stat
	,rrpa.bill_yr
	,rrpa.new_construction_flag
	,rrpa.tax_val_reason
	,rrpa.appr_land_val
	,rrpa.appr_imps_val
	,rrpa.taxable_land_val
	,rrpa.taxable_imps_val
from {{ source('src', 'raw_real_property_account') }} rrpa
left join names
	on rrpa.acct_nbr = names.acct_nbr
