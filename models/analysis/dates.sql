{{
    config(
        materialized = 'table'
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