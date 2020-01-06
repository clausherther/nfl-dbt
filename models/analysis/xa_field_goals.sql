with plays as (
    select * from {{ ref('plays') }} 
),
teams as (
    select * from {{ ref('teams') }} 
),
field_goal_kicks as (

    select
        p.play_key,
        p.game_date,
        p.game_id,
        p.play_id,
        p.season_nbr,
        p.season_type_code,
        p.season_code,
        p.quarter,
        p.down,
        p.play_type,
        ht.consolidated_team_code as home_team_code,
        wt.consolidated_team_code as away_team_code,
        ot.consolidated_team_code as off_team_code,
        dt.consolidated_team_code as def_team_code,
        p.kicker_player_id,
        p.kicker_player_name,
        p.yards_to_go,
        p.yardline_100,
        {{ get_kick_distance('p.kick_distance', 'p.yardline_100') }} as kick_distance_yards,
        p.is_within_goal_line,
        p.field_goal_result,
        p.is_field_goal_success,
        1 as field_goals,
        case when p.is_field_goal_success = true then 1 else 0 end as successful_field_goals
    from
        plays p
        inner join 
        teams ot on p.off_team_code = ot.team_code
        inner join 
        teams dt on p.def_team_code = dt.team_code
        inner join 
        teams ht on p.home_team_code = ht.team_code
        inner join 
        teams wt on p.away_team_code = wt.team_code
    where
        p.is_field_goal_attempt=true 
        and
        p.is_extra_point_attempt=false

)
select
    p.*,
    {{ convert_radians_to_degrees(
        get_kick_angle_horizontal('p.kick_distance_yards')
     ) }} as kick_angle_horizontal,
    {{ convert_radians_to_degrees(
        get_kick_angle_vertical('p.kick_distance_yards')
     ) }} as kick_angle_vertical
from
    field_goal_kicks p