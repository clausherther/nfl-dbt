{%- macro numeric_to_bool(val) -%}
{{ dbt.safe_cast(to_int(val), "bool") }}
{%- endmacro -%}
