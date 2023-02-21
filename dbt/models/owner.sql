
-- an entity that owns a property

SELECT
    owner_id
    ,taxpayer_name as name
FROM {{ ref('int_ownership') }}
GROUP BY
    owner_id
    ,taxpayer_name
