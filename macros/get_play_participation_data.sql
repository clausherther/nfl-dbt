{%- macro get_play_participation_data(season) -%}

select * from {{ source("raw_play_participation", "pbp_participation_" ~ season) }}

{%- endmacro -%}
