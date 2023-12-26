{{ config(materialized='table') }}

with base as (
    select
        play_key,
        game_id,
        play_id,
        unnest(offense_players) as player_id
    from {{ ref('stg_play_participation') }}
    union all
    select
        play_key,
        game_id,
        play_id,
        unnest(defense_players) as player_id
    from {{ ref('stg_play_participation') }}
),

dedupe as (
    select distinct play_key, game_id, play_id, player_id
    from base
    where player_id is not null and player_id != ''
)

select *
from dedupe
