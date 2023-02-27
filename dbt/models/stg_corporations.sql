select
    {% for col in adapter.get_columns_in_relation(source('pre', 'pre_corporations')) -%}
        {% if not loop.first %},{% endif %} {{ xtrim(col.column) }}
    {% endfor %}
from {{ source('pre', 'pre_corporations') }}
