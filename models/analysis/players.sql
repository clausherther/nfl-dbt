{{
    config(
        materialized = "table",
        unique_key = "player_id"
    )
}}

with players as (
    select
        player_id,
        player_name,
        entry_year,
        rookie_year,
        college_conference,
        draft_number,
        draft_round
    from {{ ref("stg_players") }}
)

select
    p.*,
    {{ dbt_housekeeping() }}
from players p
order by p.player_id
