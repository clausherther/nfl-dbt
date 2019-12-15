{{
    config(
        materialized = 'table'
    )
}}
with rosters as (
    select * from {{ ref('stg_rosters') }}
)
select
    {{ dbt_utils.surrogate_key('p.season_nbr', 'p.team_code', 'p.player_id' )}} as player_key,
    p.*,
    {{ dbt_housekeeping() }}
from
    rosters p
order by
    p.season_nbr,
    p.season_type_code,
    p.team_code,
    p.player_id