{%- macro get_roster_data(season, season_type) -%}

{%- set typed_cols %}

    {{ to_int('season') }} as season_nbr,
    upper(season_type) as season_type_code,
    gsis_id as player_id,
    full_player_name as player_name_full,
    abbr_player_name as player_name_abbr,
    team as team_code,
    position as position_code

{%- endset -%}
    
select
    {{ typed_cols }}  
from 
    {{ source('raw_roster', season_type ~ '_roster_' ~ season) }}

{%- endmacro -%}    