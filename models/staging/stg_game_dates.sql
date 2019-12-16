{{
    config(
        materialized = 'ephemeral'
    )
}}
with game_dates as (

    select distinct
        season_type_code,
        season_nbr,
        week_nbr,
        game_date 
    from
        {{ ref('stg_games') }}
    

)
select
    d.game_date,
    d.season_nbr,
    d.season_type_code,
    concat(d.season_type_code, 
        cast(d.season_nbr as {{ dbt_utils.type_string() }} )
        ) as season_code,
    min(d.game_date) over(partition by d.season_nbr) as season_start_date,
    min(d.game_date) over(partition by d.season_nbr, d.season_type_code) as season_type_start_date,
    d.week_nbr,
    concat(d.season_type_code, 
        cast(d.season_nbr as {{ dbt_utils.type_string() }} ),
        cast(d.week_nbr as {{ dbt_utils.type_string() }} )
        ) as season_week_code,
    cast(
            case
                when {{ date_part('dow', 'd.game_date') }} = 0 then 7
                else {{ date_part('dow', 'd.game_date') }}
            end
        as {{ dbt_utils.type_int() }}
    ) as day_of_week,

    {{ day_name('d.game_date', short=false) }} as day_of_week_name,
    {{ day_name('d.game_date', short=true) }} as day_of_week_name_short,

    cast({{ date_part('month', 'd.game_date') }} as {{ dbt_utils.type_int() }}) as month_of_year,
    {{ month_name('d.game_date', short=false) }}  as month_name,
    {{ month_name('d.game_date', short=true) }}  as month_name_short
from
    game_dates d
order by
    d.game_date