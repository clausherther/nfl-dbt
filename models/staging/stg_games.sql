{{
    config(
        materialized = 'incremental',
        unique_key = 'game_id',
        partition_by = 'game_date'
    )
}}
{# {%- set years = ['2019']-%} #}
{%- set pre_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set reg_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set post_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018', '2019']-%}

with raw_games as (
    {% for year in reg_years %}

    {{ get_games_data(year, "pre") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

    union all

    {% for year in reg_years %}

    {{ get_games_data(year, "reg") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

    union all

    {% for year in post_years %}

    {{ get_games_data(year, "post") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
),
new_games as (

    select *
    from raw_games
    {% if is_incremental() %}
    where 
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}

)
select
    {{ get_season_code('r.season_type_code', 'r.season_nbr') }} as season_code,
    r.*
from
    new_games r