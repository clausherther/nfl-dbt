{%- macro numeric_to_bool(val) -%}
{{ dbt_utils.safe_cast(to_int(val), 'bool') }}
{%- endmacro -%}