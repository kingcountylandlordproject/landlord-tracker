
-- intermediate table before splitting into owners/ownership

-- SELECT
--     -- TODO: figure out a way to create a stable owner_id
--     -- owner_id can be of format: "UBI-[NNN]" if it's an LLC, or "IND-[NNN]"
--     md5(rpa.taxpayer_name) as owner_id
--     ,rpa.taxpayer_name
--     ,p.major_minor
-- FROM {{ ref('real_property_account') }} rpa
-- LEFT JOIN {{ ref('parcel') }} p
--     ON rpa.major_minor = p.major_minor

select 
    null as owner_id,
    null as taxpayer_name,
    null as major_minor
