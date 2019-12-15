{{
    config(
        materialized = 'ephemeral'
    )
}}
{%- set pre_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set reg_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set post_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018']-%}

with raw_roster as (
    {% for year in reg_years %}

    {{ get_roster_data(year, "pre") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

    union all

    {% for year in reg_years %}

    {{ get_roster_data(year, "reg") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}

    union all

    {% for year in post_years %}

    {{ get_roster_data(year, "post") }}

    {% if not loop.last %}
    union all
    {% endif -%}
    {%- endfor %}
)
select
    r.*
from
    raw_roster r