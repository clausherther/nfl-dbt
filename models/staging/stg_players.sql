{{ config(materialized='table') }}

select
    gsis_id as player_id,
    display_name as player_name,
    height,
    weight,
    entry_year,
    rookie_year,
    college_conference,
    draft_number,
    draft_round
from {{ source('raw_players', 'players') }}
where gsis_id is not null
