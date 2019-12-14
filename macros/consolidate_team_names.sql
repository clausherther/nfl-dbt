{%- macro consolidate_team_names(team_name) -%}
case 
    when {{ team_name }} in ('JAC', 'JAX') then 'JAX/JAC'
    when {{ team_name }} in ('STL', 'LA') then 'LA/STL'
    when {{ team_name }} in ('SD', 'LAC') then 'LAC/SD'
    else {{ team_name }}
end
{%- endmacro -%}