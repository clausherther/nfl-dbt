{{ config(materialized='table') }}

{%- set years = var("years") -%}


with raw_injury as (
    {% for year in years %}

    {{ get_injury_data(year) }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
)

select *
from raw_injury
