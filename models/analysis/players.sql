{{
    config(
        materialized = "table",
        unique_key = "player_id"
    )
}}
with players as (

    select
        player_id,
        max(player_name) as player_name,
        avg(player_height) as avg_player_height,
        avg(player_weight) as avg_player_weight
    from {{ ref("stg_rosters") }}
    where
        player_id is not null
        and
        player_name is not null
    group by
        1

)
select
    p.*,
    {{ dbt_housekeeping() }}
from
    players p
order by
    p.player_id
