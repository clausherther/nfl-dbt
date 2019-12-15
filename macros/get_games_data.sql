{%- macro get_games_data(season, season_type) -%}

{%- set typed_cols %}
    {{ to_int('game_id') }} as game_id,
    cast(left(cast(game_id as {{ dbt_utils.type_string() }}), 8) as date) as game_date,
    {{ to_int('season') }} as season_nbr,
    upper(type) as season_type_code,
    {{ to_int('week') }} as week_nbr,
    home_team as home_team_code,
    away_team as away_team_code,
    {{ to_int('home_score') }} as home_score,
    {{ to_int('away_score') }} as away_score,
    game_url
{%- endset -%}
    
select
    {{ typed_cols }}  
from 
    {{ source('raw_games', season_type ~ '_games_' ~ season) }}

{%- endmacro -%}    