{{
    config(
        materialized = 'ephemeral'
    )
}}

{%- set regular_season_start_dates = ['2009-09-10', 
                                    '2010-09-09', 
                                    '2011-09-08', 
                                    '2012-09-05', 
                                    '2013-09-05', 
                                    '2014-09-04', 
                                    '2015-09-10', 
                                    '2016-09-08', 
                                    '2017-09-07', 
                                    '2018-09-06', 
                                    '2019-09-05'
                                    ]
-%}
with season_start_dates as (

    {% for season_start_date in regular_season_start_dates -%}
        select 
            cast('{{ season_start_date }}' as date) as season_start_date
        {% if not loop.last -%}
        union all
        {%- endif %} 
    {% endfor %} 
),
next_season_start_dates as (

    select
        season_start_date,
        coalesce(
            lead(season_start_date) over(order by season_start_date),
            {{ today() }}
            ) as next_season_start_date
    from
        season_start_dates

),
game_dates as (

    select distinct 
        season_type_code,
        season_nbr,
        game_date 
    from
        {{ ref('stg_play_by_play') }}

),
season_dates as (

    select
        g.game_date,
        g.season_type_code,
        g.season_nbr,
        s.season_start_date
    from
        game_dates g
        inner join
        next_season_start_dates s
            on g.game_date >= s.season_start_date and
                g.game_date < s.next_season_start_date
)
select
    d.season_start_date,
    {# {{ date_part('year', 'd.season_start_date') }}  #}
    d.season_nbr,
    d.season_type_code,
    concat(d.season_type_code, 
        cast({{ date_part('year', 'd.season_start_date') }} as {{ dbt_utils.type_string() }} )
        ) as season_code,
    d.game_date,
    cast(
            case
                when {{ date_part('dow', 'd.game_date') }} = 0 then 7
                else {{ date_part('dow', 'd.game_date') }}
            end
        as {{ dbt_utils.type_int() }}
    ) as day_of_week,

    {{ day_name('d.game_date', short=false) }} as day_of_week_name,
    {{ day_name('d.game_date', short=true) }} as day_of_week_name_short,
{# 
    game_week_start_date
    game_week_code,
    season_week,

 #}
    cast({{ date_part('month', 'd.game_date') }} as {{ dbt_utils.type_int() }}) as month_of_year,
    {{ month_name('d.game_date', short=false) }}  as month_name,
    {{ month_name('d.game_date', short=true) }}  as month_name_short
from
    season_dates d