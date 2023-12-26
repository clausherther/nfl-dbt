{%- macro get_depth_chart_data(season) -%}

{%- set typed_cols %}

    {{ to_int("season") }} as season_nbr,
    {{ to_int("week") }} as week_nbr,
    {{ fix_team_names("club_code") }} as team_code,
    depth_team,
    gsis_id as player_id,
    depth_position,
    position

{%- endset -%}

select
    {{ typed_cols }}
from {{ source("raw_depth_charts", "depth_charts_" ~ season) }}

{%- endmacro -%}
