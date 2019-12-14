{{
    config(
        materialized = 'incremental',
        unique_key = 'game_date',
        incremental_strategy = 'delete+insert'
    )
}}
with regular_season_plays as (
    select * from {{ ref('stg_play_by_play') }}
    {% if is_incremental() %}
    where 
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}
)
select 
    r.*,
    {{ dbt_housekeeping() }} 
from 
    regular_season_plays r