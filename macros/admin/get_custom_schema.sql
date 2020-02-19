{% macro generate_schema_name_for_env(custom_schema_name=none) -%}
    {%- set default_schema = target.schema -%}  
    {%- if "audit" in target.name -%}
        audit
    {%- elif custom_schema_name is not none -%}
        {{ custom_schema_name | trim }}
    {%- else -%}
        {{ default_schema }}
    {%- endif -%}
{%- endmacro %}

{% macro generate_schema_name(schema_name, node) -%}
    {{ generate_schema_name_for_env(schema_name) }}
{%- endmacro %}
