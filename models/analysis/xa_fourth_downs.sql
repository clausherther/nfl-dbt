{{
    config(
        materialized = 'incremental',
        unique_key = 'play_key',
        partition_by = 'game_date'
    )
}}
with plays as (
    select * from {{ ref('plays') }} 
    {% if is_incremental() %}
    where 
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}
),
dates as (
    select * from {{ ref('dates') }} 
),
teams as (
    select * from {{ ref('teams') }} 
),
fourth_downs as (

    select
        p.play_key,
        p.game_date,
        d.week_nbr,
        d.season_week_code,
        p.game_id,
        p.play_id,
        p.season_nbr,
        p.season_type_code,
        p.season_code,
        p.quarter,
        p.down,
        p.drive,
        p.play_type,
        ht.consolidated_team_code as home_team_code,
        wt.consolidated_team_code as away_team_code,
        ot.consolidated_team_code as off_team_code,
        dt.consolidated_team_code as def_team_code,
        p.is_fourth_down_converted,
        p.is_fourth_down_failed,
        p.is_penalty,
        p.yards_to_go,
        p.yardline_100,
        p.yards_gained,
        1 as fourth_down_attempts,
        case when p.is_fourth_down_converted = true then 1 else 0 end as fourth_down_conversions
    from
        plays p
        inner join
        dates d on p.game_date = d.game_date
        inner join 
        teams ot on p.off_team_code = ot.team_code
        inner join 
        teams dt on p.def_team_code = dt.team_code
        inner join 
        teams ht on p.home_team_code = ht.team_code
        inner join 
        teams wt on p.away_team_code = wt.team_code
    where
        p.is_field_goal_attempt=false 
        and
        p.is_fourth_down_attempt=true

)
select
    p.*
from
    fourth_downs p