{%- macro to_int(val) -%}
{{ dbt_utils.safe_cast(val, dbt_utils.type_int()) }}
{%- endmacro -%}