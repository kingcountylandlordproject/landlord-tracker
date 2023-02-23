select
    concat(major, minor) as major_minor
    {% for col in adapter.get_columns_in_relation(source('src', 'raw_apartment_complex')) -%}
        ,{{ xtrim(col.column) }}
    {% endfor %}
from {{ source('src', 'raw_apartment_complex') }}
