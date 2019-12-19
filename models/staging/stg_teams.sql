{{
    config(
        materialized = 'table'
    )
}}
select 
    off_team_code as team_code,
    {{ consolidate_team_names('off_team_code') }} as consolidated_team_code
from
    {{ ref('stg_play_by_play') }}
where off_team_code is not null
group by 
    1,2