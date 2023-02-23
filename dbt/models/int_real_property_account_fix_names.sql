
with names2022 as (
	-- get taxpayer_name from older file so we can merge it in 2023 data
	select
		acct_nbr
		,taxpayer_name
	from {{ ref('int_real_property_account_all_years') }}
	where bill_yr = '2022'
	group by
		acct_nbr
		,taxpayer_name
)
select
	rrpa.acct_nbr
	,CONCAT(rrpa.major, rrpa.minor) as major_minor
	,rrpa.major
	,rrpa.minor
	,coalesce(rrpa.taxpayer_name, names2022.taxpayer_name) as taxpayer_name
	,rrpa.attn_line
	,rrpa.addr_line
	,rrpa.city_state
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
from {{ ref('int_real_property_account_all_years') }} rrpa
left join names2022
	on rrpa.bill_yr = '2023'
	and rrpa.acct_nbr = names2022.acct_nbr
