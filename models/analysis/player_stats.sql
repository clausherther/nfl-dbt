{{ config(materialized = "table") }}

select *
from {{ ref('stg_player_stats') }}
