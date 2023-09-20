{%- macro fix_team_names(team_name) -%}
case
    when {{ team_name }} in ('JAC', 'JAX') then 'JAX'
    when {{ team_name }} in ('STL', 'LA', 'SL') then 'LAR/STL'
    when {{ team_name }} in ('SD', 'LAC') then 'LAC/SD'
    when {{ team_name }} in ('ARI', 'ARZ') then 'ARI'
    when {{ team_name }} in ('OAK', 'LV') then 'OAK/LV'
    when {{ team_name }} in ('HST', 'HOU') then 'HOU'
    when {{ team_name }} in ('BAL', 'BLT') then 'BLT'
    when {{ team_name }} in ('CLE', 'CLV') then 'CLE'
    else nullif(trim({{ team_name }}), '')
end
{%- endmacro -%}
