{%- macro to_string(val) -%}
{{ dbt.safe_cast(val, dbt.type_string()) }}
{%- endmacro -%}
