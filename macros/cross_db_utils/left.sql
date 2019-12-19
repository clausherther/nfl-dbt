{% macro left(string_text, length_expression) -%}
    {{ adapter_macro('left', string_text, length_expression) }}
{% endmacro %}

{% macro default__left(string_text, length_expression) %}

    left(
        {{ string_text }},
        {{ length_expression }}
    )
    
{%- endmacro -%}

{% macro bigquery__left(string_text, length_expression) %}

    case when {{ length_expression }} = 0 
        then ''
    else 
        substr(
            {{ string_text }},
            1,
            {{ length_expression }}
        )
    end

{%- endmacro -%}

{% macro snowflake__left(string_text, length_expression) %}

    case when {{ length_expression }} = 0 
        then ''
    else 
        left(
            {{ string_text }},
            {{ length_expression }}
        )
    end

{%- endmacro -%}