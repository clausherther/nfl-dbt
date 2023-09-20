{%- macro get_kick_angle_horizontal(distance_yards) -%}
{%- set DISTANCE_FROM_GOAL_YARDS = 10.0 -%}
{%- set DISTANCE_TO_SIDELINE_FT = 10.75 -%}
{%- set GOAL_WIDTH_FT = 18.5 -%}
{%- set TOTAL_WIDTH_FT = DISTANCE_TO_SIDELINE_FT + GOAL_WIDTH_FT -%}
{%- set total_distance_ft = "(" ~ distance_yards ~ "* 3.0" ~ ")" -%}
case when {{ distance_yards }} > 0 then
atan({{ TOTAL_WIDTH_FT }}/{{ total_distance_ft }}) - atan({{ DISTANCE_TO_SIDELINE_FT }}/{{ total_distance_ft }})
else 0
end
{%- endmacro -%}
