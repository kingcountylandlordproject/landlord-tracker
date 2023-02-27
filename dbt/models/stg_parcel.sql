select 
    CONCAT(major, minor) as major_minor
    {% for col in adapter.get_columns_in_relation(source('src', 'raw_parcel')) -%}
        ,{{ xtrim(col.column) }}
    {% endfor %}
from {{ source('src', 'raw_parcel') }}
