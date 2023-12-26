{%- macro get_player_stats_data(season) -%}

{%- set typed_cols %}

    {{ to_int("season") }} as season_nbr,
    {{ to_int("week") }} as week_nbr,
    season_type,
    player_id,
    completions,
    attempts,
    passing_yards,
    passing_tds,
    interceptions,
    sacks,
    sack_yards,
    sack_fumbles,
    sack_fumbles_lost,
    passing_air_yards,
    passing_yards_after_catch,
    passing_first_downs,
    passing_epa,
    passing_2pt_conversions,
    pacr,
    dakota,
    carries,
    rushing_yards,
    rushing_tds,
    rushing_fumbles,
    rushing_fumbles_lost,
    rushing_first_downs,
    rushing_epa,
    rushing_2pt_conversions,
    receptions,
    targets,
    receiving_yards,
    receiving_tds,
    receiving_fumbles,
    receiving_fumbles_lost,
    receiving_air_yards,
    receiving_yards_after_catch,
    receiving_first_downs,
    receiving_epa,
    receiving_2pt_conversions,
    racr,
    target_share,
    air_yards_share,
    wopr,
    special_teams_tds,
    fantasy_points,
    fantasy_points_ppr

{%- endset -%}

select
    {{ typed_cols }}
from {{ source("raw_player_stats", "player_stats_" ~ season) }}
where week_nbr > 0

{%- endmacro -%}
