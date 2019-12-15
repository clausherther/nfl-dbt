{{
    config(
        materialized = 'incremental',
        unique_key = 'game_date',
        incremental_strategy = 'delete+insert'
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