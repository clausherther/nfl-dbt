{{
    config(
        materialized = 'incremental',
        unique_key = 'game_date',
        incremental_strategy = 'delete+insert'
    )
}}
with plays as (
    select * from {{ ref('stg_play_by_play') }}
    {% if is_incremental() %}
    where 
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}
),
numbered_plays as (

    select
        p.*,
        row_number() over(partition by game_id, play_id order by (total_home_score+total_away_score) desc) as play_dedupe_sequence_nbr 
    from 
        plays p

),
deduped_plays as (
    select 
        *
    from
        numbered_plays
    where
        play_dedupe_sequence_nbr = 1
)
select 
    {{ dbt_utils.surrogate_key('r.game_id', 'r.play_id' )}} as play_key,
    r.*,
    {{ dbt_housekeeping() }} 
from 
    deduped_plays r