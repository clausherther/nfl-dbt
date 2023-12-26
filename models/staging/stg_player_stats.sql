{{ config(materialized='table') }}

{%- set years = var("years") -%}


with raw_player_stats as (
    {% for year in years %}

    {{ get_player_stats_data(year) }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
)

select *
from raw_player_stats
