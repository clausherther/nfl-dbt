{{
    config(
        materialized = 'table'
    )
}}
with teams as (
    select * from {{ ref('stg_teams') }}
)
select
    team_code,
    consolidated_team_code,
    {{ dbt_housekeeping() }}
from
    teams