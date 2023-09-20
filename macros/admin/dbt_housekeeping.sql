{% macro dbt_housekeeping() -%}
    cast('{{ invocation_id }}' as {{ dbt.type_string() }}) as dbt_batch_id,
    cast('{{ run_started_at }}' as {{ dbt.type_timestamp() }}) as dbt_batch_ts
{%- endmacro %}
