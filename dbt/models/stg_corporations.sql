select
    {% for col in adapter.get_columns_in_relation(source('src', 'raw_corporations')) -%}
        {% if not loop.first %},{% endif %} {{ xtrim(col.column) }}
    {% endfor %}
from {{ source('src', 'raw_corporations') }}
