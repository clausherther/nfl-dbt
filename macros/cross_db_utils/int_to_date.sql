{% macro int_to_date(val) -%}
    {{ adapter_macro('int_to_date', val) }}
{% endmacro %}

{%- macro default__int_to_date(val) -%}
{{ dbt_utils.safe_cast(val, 'date') }}
{%- endmacro -%}

{%- macro bigquery__int_to_date(val) -%}
parse_date('%Y%m%d', {{ dbt_utils.safe_cast(val, dbt_utils.type_string()) }})
{%- endmacro -%}