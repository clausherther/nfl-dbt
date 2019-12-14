{%- macro today() -%}
{{ dbt_utils.safe_cast(dbt_utils.current_timestamp(), 'date') }}
{%- endmacro -%}
