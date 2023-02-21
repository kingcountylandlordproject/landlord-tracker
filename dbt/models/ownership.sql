
-- represents an association between an owner and a parcel

SELECT
    owner_id
    ,major_minor
FROM {{ ref('int_ownership') }} o
