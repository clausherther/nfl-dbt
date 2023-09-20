{%- macro to_int(val) -%}
cast(
    coalesce({{ dbt.safe_cast(val, "numeric") }}, 0) as {{ dbt.type_int() }}
)
{%- endmacro -%}
