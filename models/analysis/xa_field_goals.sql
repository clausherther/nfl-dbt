{{ config(materialized = "table") }}

with plays as (
    select * from {{ ref("plays") }}
),
dates as (
    select * from {{ ref("dates") }}
),
field_goal_kicks as (

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
        p.play_type,
        p.home_team_code,
        p.away_team_code,
        p.off_team_code,
        p.def_team_code,
        p.kicker_player_id,
        p.kicker_player_name,
        p.yards_to_go,
        p.yardline_100,
        {{ get_kick_distance("p.kick_distance", "p.yardline_100") }} as kick_distance_yards,
        p.is_within_goal_line,
        p.field_goal_result,
        p.is_field_goal_success,
        1 as field_goals,
        case when p.is_field_goal_success then 1 else 0 end as successful_field_goals
    from plays p
    inner join dates d
    on p.game_date = d.game_date
    where
        p.is_field_goal_attempt and
        not p.is_extra_point_attempt

)
select
    p.*,
    {{ get_kick_angle_horizontal("p.kick_distance_yards") }} as kick_angle_horizontal,
    {{ get_kick_angle_vertical("p.kick_distance_yards") }} as kick_angle_vertical,
    {{ convert_radians_to_degrees(
        get_kick_angle_horizontal("p.kick_distance_yards")
     ) }} as kick_angle_horizontal_degrees,
    {{ convert_radians_to_degrees(
        get_kick_angle_vertical("p.kick_distance_yards")
     ) }} as kick_angle_vertical_degrees

from
    field_goal_kicks p
