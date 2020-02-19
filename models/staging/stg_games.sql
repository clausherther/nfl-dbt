{{
    config(
        materialized = 'incremental',
        unique_key = 'game_id',
        partition_by = 'game_date'
    )
}}
{# {%- set years = ['2019']-%} #}
{%- set pre_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set reg_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018','2019']-%}
{%- set post_years = ['2009','2010','2011','2012','2013','2014','2015','2016','2017','2018', '2019']-%}
{%- set season_types = ['pre', 'reg', 'post'] -%}

with games as (
    {% for year in reg_years %}

        {% for season_type in season_types %}    
    
        {{ get_games_data(year, season_type) }}

        {% if not loop.last %}
        union all
        {% endif -%}
    
    {%- endfor %}
    
    {% if not loop.last %}
    union all
    {% endif -%}
    
    {%- endfor %}

)
select
    {{ get_season_code('r.season_type_code', 'r.season_nbr') }} as season_code,
    r.*
from
    games r
{% if is_incremental() %}
where 
    r.game_date >= cast({{ incremental_refresh_date() }} as date)
{% endif %}
