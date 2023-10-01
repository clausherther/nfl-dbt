{{
    config(
        materialized = "table",
        unique_key = "team_player_key"
    )
}}
with rosters as (
    select * from {{ ref("stg_rosters") }}
),
dedupe_rosters as (

    select
        *,
        row_number() over(partition by season_nbr, team_code, player_id order by player_id) as rn
    from
        rosters
)
select
    {{ generate_surrogate_key(["p.season_nbr", "p.team_code", "p.player_id"])}} as team_player_key,
    p.*,
    {{ dbt_housekeeping() }}
from
    dedupe_rosters p
where
    p.rn = 1 and
    p.player_id is not null and
    p.player_name is not null
order by
    p.season_nbr,
    p.team_code,
    p.player_id
