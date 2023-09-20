{%- macro to_int(val) -%}
{{ dbt.safe_cast(val, dbt.type_int()) }}
{%- endmacro -%}
