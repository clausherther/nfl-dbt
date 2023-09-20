{%- macro get_roster_data(season) -%}

{%- set typed_cols %}

    {{ to_int("season") }} as season_nbr,
    trim(gsis_id) as player_id,
    trim(full_name) as player_name,
    {{ fix_team_names("team") }} as team_code,
    trim(position) as position_code,
    trim(jersey_number) as jersey_number,
    trim(height) as player_height,
    weight as player_weight

{%- endset -%}

select
    {{ typed_cols }}
from
    {{ source("raw_roster", "roster_" ~ season) }}

{%- endmacro -%}
