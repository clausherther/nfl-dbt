{{ config(materialized = "table") }}

{%- set years = ["2009","2010","2011","2012","2013","2014","2015","2016","2017","2018","2019", "2020", "2021", "2022", "2023"]-%}

with raw_roster as (
    {% for year in years %}

    {{ get_roster_data(year) }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

),
play_data as (

    select distinct
        r.passer_player_id,
        r.passer_player_name,
        r.receiver_player_id as receiver_player_id,
        r.receiver_player_name as receiver_player_name,
        r.home_team_code as team_code,
        r.season_nbr
    from
        {{ ref("stg_play_by_play") }} r

),
bad_ids as (

    select
        player_name,
        team_code,
        position_code
    from
        raw_roster
    where
        player_id not like {{ dbt.string_literal("00-%") }}
    group by 1,2,3

),
bad_id_fix_passers as (

    select distinct
        r.passer_player_id as player_id,
        r.passer_player_name as player_name,
        r.team_code,
        r.season_nbr,
        b.position_code
    from
        play_data r
        inner join
        bad_ids b
            on r.passer_player_name = b.player_name
                and r.team_code = b.team_code
    where
        b.position_code = {{ dbt.string_literal("QB") }}

),
bad_id_fix_receivers as (

    select distinct
        r.receiver_player_id as player_id,
        r.receiver_player_name as player_name,
        r.team_code,
        r.season_nbr,
        b.position_code
    from
        play_data r
        inner join
        bad_ids b
            on r.receiver_player_name = b.player_name
                and b.position_code != {{ dbt.string_literal("QB") }}
                and r.team_code = b.team_code
),
bad_id_fix as (

    select * from bad_id_fix_passers
    union all
    select * from bad_id_fix_receivers

)
select
    r.season_nbr,
    coalesce(f.player_id, r.player_id) as player_id,
    r.player_name,
    r.team_code,
    r.position_code,
    r.jersey_number,
    {{ to_int("r.player_height") }} as player_height,
    {{ to_int("r.player_weight") }} as player_weight
from
    raw_roster r
    left outer join
    bad_id_fix f
        on r.player_name = f.player_name
            and r.position_code = f.position_code
            and r.team_code = f.team_code
            and r.season_nbr = f.season_nbr
