{%- macro get_kick_distance(distance_yards, yards_100) -%}
{%- set DISTANCE_FROM_GOAL_YARDS = 10.0 -%}
{%- set DISTANCE_FROM_LINE_OF_SCRIMMAGE_YARDS = 7.0 -%}
case when coalesce({{ distance_yards }}, 0) = 0 then
{{ yards_100 }} + {{ DISTANCE_FROM_GOAL_YARDS }} + {{ DISTANCE_FROM_LINE_OF_SCRIMMAGE_YARDS}} 
else {{ distance_yards }}
end
{%- endmacro -%}