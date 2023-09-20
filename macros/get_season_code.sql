{%- macro get_season_code(season_type_code, season_nbr) -%}
concat({{season_type_code}},
        cast({{ season_nbr }} as {{ dbt.type_string() }} )
        )
{%- endmacro -%}
