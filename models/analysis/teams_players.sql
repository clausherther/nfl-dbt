{{
    config(
        materialized = "table",
        unique_key = "team_player_key"
    )
}}
with rosters as (
    select * from {{ ref("stg_rosters") }}
),
players as (
    select * from {{ ref("players") }}
),
dedupe_rosters as (

    select
        *,
        row_number() over(partition by season_nbr, team_code, player_id order by player_id) as rn
    from
        rosters
)
select
    {{ generate_surrogate_key(["r.season_nbr", "r.team_code", "r.player_id"])}} as team_player_key,
    r.*,
    {{ dbt_housekeeping() }}
from dedupe_rosters r
join players p
    on p.player_id = r.player_id
where
    r.rn = 1 and
    r.player_id is not null and
    r.player_name is not null
order by
    r.season_nbr,
    r.team_code,
    r.player_id
