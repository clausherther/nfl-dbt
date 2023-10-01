{{ config(materialized = "table") }}

{%- set years = ["2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019", "2020", "2021", "2022", "2023"]-%}

with raw_pbp as (
    {% for year in years %}

    {{ get_play_by_play_data(year) }}

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

)
select
    {{ generate_surrogate_key(["r.game_id", "r.play_id"]) }} as play_key,
    {{ get_season_code("r.season_type_code", "r.season_nbr") }} as season_code,
    r.*
from
    new_plays r
where
    off_team_code is not null and
    def_team_code is not null
