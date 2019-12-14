{%- macro to_string(val) -%}
{{ dbt_utils.safe_cast(val, dbt_utils.type_string()) }}
{%- endmacro -%}