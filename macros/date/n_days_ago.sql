{%- macro n_days_ago(n) -%}
{%- set n = n|int -%}
{{ dbt_utils.dateadd('day', -1 * n,  today()) }}
{%- endmacro -%}