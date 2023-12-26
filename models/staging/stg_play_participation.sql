{{ config(materialized='table') }}

{%- set years = var("years") -%}


with raw_play_participation as (
    {% for year in years %}

    {{ get_play_participation_data(year) }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
)

select
    nflverse_game_id as game_id,
    play_id,
    {{ generate_surrogate_key(["game_id", "play_id"]) }} as play_key,
    offense_formation,
    offense_personnel,
    defenders_in_box,
    defense_personnel,
    number_of_pass_rushers,
    split(offense_players, ';') as offense_players,
    split(defense_players, ';') as defense_players,
    split(players_on_play, ';') as players,
    n_offense,
    n_defense
from raw_play_participation
