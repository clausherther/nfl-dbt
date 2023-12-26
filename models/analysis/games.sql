{{ config(materialized = "table") }}

with games as (
    select
        game_id as game_id,
        max(game_date) as game_date,
        max(season_nbr) as season_nbr,
        max(season_type_code) as season_type_code,
        max(week_nbr) as week_nbr,
        max(home_team_code) as home_team_code,
        max(away_team_code) as away_team_code,
        max(total_home_score) as home_score,
        max(total_away_score) as away_score
    from
        {{ ref("stg_plays") }}
    group by
        1
)
select
    r.*,
    {{ dbt_housekeeping() }}
from
    games r
