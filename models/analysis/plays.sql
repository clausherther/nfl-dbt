{{ config(materialized = "table") }}

with plays as (
    select * from {{ ref("stg_plays") }}
),

play_participation as (
    select
        play_key,
        offense_formation,
        offense_personnel,
        defenders_in_box,
        defense_personnel,
        number_of_pass_rushers
    from {{ ref("stg_play_participation") }}
)

select
    *,
    {{ dbt_housekeeping() }}
from plays
left join play_participation
using(play_key)
