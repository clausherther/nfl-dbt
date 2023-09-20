{{
    config(
        materialized = "incremental",
        unique_key = "play_key",
        partition_by = {"field": "game_date", "data_type": "date", "granularity": "day"}
    )
}}
with plays as (
    select * from {{ ref("stg_play_by_play") }}
    {% if is_incremental() %}
    where
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}
)
select
    r.*,
    {{ dbt_housekeeping() }}
from
    plays r
