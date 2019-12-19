{{
    config(
        materialized = 'table',
        unique_key = 'game_date'
    )
}}
with game_dates as (
    select * from {{ ref('stg_game_dates') }}
)
select
    d.*,
    {{ dbt_housekeeping() }}
from
    game_dates d
order by 
    d.game_date    