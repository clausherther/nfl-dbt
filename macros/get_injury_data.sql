{%- macro get_injury_data(season) -%}

{%- set typed_cols %}

    {{ to_int("season") }} as season_nbr,
    {{ to_int("week") }} as week_nbr,
    game_type,
    {{ fix_team_names("team") }} as team_code,
    gsis_id as player_id,
    report_primary_injury,
    report_secondary_injury,
    report_status,
    practice_primary_injury,
    practice_secondary_injury,
    practice_status,
    date_modified

{%- endset -%}

select
    {{ typed_cols }}
from {{ source("raw_injury", "injuries_" ~ season) }}

{%- endmacro -%}
