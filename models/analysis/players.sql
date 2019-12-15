{{
    config(
        materialized = 'table'
    )
}}
with players as (

    select distinct
        player_id,
        player_name_full,
        player_name_abbr 
    from {{ ref('stg_rosters') }}

)
select
    p.*,
    {{ dbt_housekeeping() }}
from
    players p
order by
    p.player_id