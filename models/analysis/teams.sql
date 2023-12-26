{{
    config(
        materialized = "table",
        unique_key = "team_code"
    )
}}
with teams as (

    select
        home_team_code as team_code
    from
        {{ ref("stg_plays") }}
    where
        nullif(trim(home_team_code), '') is not null
    group by
        1

)
select
    team_code,
    {{ dbt_housekeeping() }}
from
    teams
