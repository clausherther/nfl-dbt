{{
    config(
        materialized = 'incremental',
        unique_key = 'game_id',
        partition_by = 'game_date'
    )
}}
with games as (
    select * from {{ ref('stg_games') }}
    {% if is_incremental() %}
    where 
        game_date >= cast({{ incremental_refresh_date() }} as date)
    {% endif %}
)
select 
    r.*,
    {{ dbt_housekeeping() }} 
from 
    games r
