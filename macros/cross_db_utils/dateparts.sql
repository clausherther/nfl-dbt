{# --- DAY OF WEEK --- #}
{% macro datepart_weekday() -%}
  {{ adapter_macro('datepart_weekday') }}
{%- endmacro %}

{% macro default__datepart_weekday() -%}
    dayofweek
{%- endmacro %}

{% macro bigquery__datepart_weekday() -%}
    dayofweek
{%- endmacro %}

{% macro postgres__datepart_weekday() -%}
    dow
{%- endmacro %}

{% macro snowflake__datepart_weekday() -%}
    dayofweek
{%- endmacro %}

{# --- MONTH --- #}
{% macro datepart_month() -%}
  {{ adapter_macro('datepart_month') }}
{%- endmacro %}

{% macro default__datepart_month() -%}
    month
{%- endmacro %}
