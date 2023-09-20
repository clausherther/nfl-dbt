{% macro int_to_date(val) -%}
    {{ adapter.dispatch("int_to_date")(val) }}
{% endmacro %}

{%- macro default__int_to_date(val) -%}
{{ dbt.safe_cast(val, "date") }}
{%- endmacro -%}

{%- macro bigquery__int_to_date(val) -%}
parse_date("%Y%m%d", {{ dbt.safe_cast(val, dbt.type_string()) }})
{%- endmacro -%}
