{{ config(materialized = "table") }}

with game_dates as (

    select distinct
        season_type_code,
        season_nbr,
        week_nbr,
        game_date
    from
        {{ ref("games") }}


)
select
    d.game_date,
    d.season_nbr,
    d.season_type_code,
    {{ get_season_code("d.season_type_code", "d.season_nbr") }} as season_code,
    min(d.game_date) over(partition by d.season_nbr) as season_start_date,
    min(d.game_date) over(partition by d.season_nbr, d.season_type_code) as season_type_start_date,
    d.week_nbr,
    concat(d.season_type_code,
        cast(d.season_nbr as {{ dbt.type_string() }} ),
        cast(d.week_nbr as {{ dbt.type_string() }} )
        ) as season_week_code,

    {{ dbt_date.day_of_week("d.game_date") }} as day_of_week,
    {{ dbt_date.day_name("d.game_date", short=false) }} as day_of_week_name,
    {{ dbt_date.day_name("d.game_date", short=true) }} as day_of_week_name_short,

    {{ dbt_date.date_part("month", "d.game_date") }} as month_of_year,
    {{ dbt_date.month_name("d.game_date", short=false) }} as month_name,
    {{ dbt_date.month_name("d.game_date", short=true) }} as month_name_short,

    {{ dbt_housekeeping() }}
from
    game_dates d
order by
    d.game_date
