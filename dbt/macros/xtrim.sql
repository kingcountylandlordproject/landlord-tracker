{# trim and convert empty strings to nulls #}
{% macro xtrim(column_name, as=True, new_column_name=None, column_type='VARCHAR') -%}
    CAST(NULLIF(TRIM({{ column_name }}), '') AS {{ column_type}})
    {% if as %}
        AS {% if new_column_name %}{{ new_column_name }}{% else %}{% set parts = column_name.split('.') %}{{ parts[-1] }}{% endif %}
    {% endif %}
{%- endmacro %}
