{%- macro incremental_refresh_date(n=0, plus=0) -%}
{%- set n_days = n if n > 0 else var("incremental_days") -%}
{%- set n_days = n_days + plus -%}
{{ dbt_date.n_days_ago(n_days) }}
{%- endmacro -%}
