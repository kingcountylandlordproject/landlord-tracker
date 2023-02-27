
-- make sure acct_nbr doesn't map to more than one taxpayer_name

select acct_nbr
from {{ source('src', 'raw_real_property_account_2022_07_06') }}
group by acct_nbr having count(distinct taxpayer_name) > 1
