{{
    config(
        materialized = 'incremental',
        unique_key = 'play_key',
        partition_by = 'game_date'
    )
}}
{# {%- set years = ['2019']-%} #}
{%- set pre_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set reg_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set post_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018', '2019']-%}

with raw_pbp as (
    {% for year in reg_years %}

    {{ get_play_by_play_data(year, "pre") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

    union all

    {% for year in reg_years %}

    {{ get_play_by_play_data(year, "reg") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

    union all

    {% for year in post_years %}

    {{ get_play_by_play_data(year, "post") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
),
new_plays as (

    select * 
    from raw_pbp
    {% if is_incremental() %}
    where 
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}

),
numbered_plays as (

    select
        p.*,
        row_number() over(partition by game_id, play_id order by (total_home_score+total_away_score) desc) as play_dedupe_sequence_nbr 
    from 
        new_plays p

),
deduped_plays as (
    select 
        *
    from
        numbered_plays
    where
        play_dedupe_sequence_nbr = 1
)
select 
    {{ dbt_utils.surrogate_key('r.game_id', 'r.play_id' )}} as play_key,
    {{ get_season_code('r.season_type_code', 'r.season_nbr') }} as season_code,
    r.*
from 
    deduped_plays r
