{{ config(materialized = "table") }}

with plays as (
    select * from {{ ref("plays") }}
),
dates as (
    select * from {{ ref("dates") }}
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
        p.home_team_code,
        p.away_team_code,
        p.off_team_code,
        p.def_team_code,
        p.is_fourth_down_converted,
        p.is_fourth_down_failed,
        p.is_penalty,
        p.yards_to_go,
        p.yardline_100,
        p.yards_gained,
        1 as fourth_down_attempts,
        case when p.is_fourth_down_converted then 1 else 0 end as fourth_down_conversions
    from plays p
    inner join dates d
    on p.game_date = d.game_date
    where
        not p.is_field_goal_attempt and
        p.is_fourth_down_attempt

)
select
    p.*
from
    fourth_downs p
