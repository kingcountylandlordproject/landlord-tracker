{# trim and convert empty strings to nulls #}
{% macro xtrim(column_name, new_column_name=None) -%}
    NULLIF(TRIM({{ column_name }}), '') AS {% if new_column_name %}{{ new_column_name }}{% else %}{% set parts = column_name.split('.') %}{{ parts[-1] }}{% endif %}
{%- endmacro %}
