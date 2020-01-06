{%- macro convert_radians_to_degrees(radians) -%}
{{ radians }}*180.0/ACOS(-1)
{%- endmacro -%}