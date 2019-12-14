{% macro get_copy_file_command() %}

    {%- set table_name = var('table_name') -%}
    {%- set file_path = var('file_path') -%}

    begin;

    truncate table {{ table_name }};
    copy {{ table_name }}
    from '{{ file_path }}'
    csv 
    header;

    commit;
{%- endmacro -%}

{%- macro load_raw_pbp_data() %}

    {# 
    call this with:
        dbt run-operation load_raw_pbp_data --target pg --vars "{'raw_file_path':'/Users/claus/dev/clausherther/nfl-data-load/'}"
    #}
    {%- call statement('refresh', fetch_result=true, auto_begin=true) -%}

        {%- set copy_files_command = get_copy_file_command() -%}
        {{ copy_files_command }}
        {# {{log(copy_files_command, info = true)}} #}

    {%- endcall -%}

{%- endmacro -%}
