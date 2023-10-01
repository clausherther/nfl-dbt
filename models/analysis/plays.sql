{{ config(materialized = "table") }}

with plays as (
    select * from {{ ref("stg_play_by_play") }}
)
select
    r.*,
    {{ dbt_housekeeping() }}
from
    plays r
