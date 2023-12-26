{{ config(materialized='table') }}

{%- set years = var("years") -%}


with raw_depth_charts as (
    {% for year in years %}

    {{ get_depth_chart_data(year) }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
)

select *
from raw_depth_charts
