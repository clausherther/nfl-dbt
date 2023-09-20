{%- macro get_kick_angle_vertical(distance_yards) -%}
{%- set GOAL_HEIGHT_FT = 35.0 -%}
{%- set total_distance_ft = "(" ~ distance_yards ~ "* 3.0" ~ ")" -%}
case when {{ distance_yards }} > 0 then
atan({{ GOAL_HEIGHT_FT }}/{{ total_distance_ft }})
else 0
end
{%- endmacro -%}
