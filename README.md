# NFL Play-by-play dbt models

This repo contains [dbt](https://www.getdbt.com) models to transform NFL Play-by-Play (pbp) data sourced from [https://github.com/ryurko/nflscrapR-data.git](https://github.com/ryurko/nflscrapR-data.git) into analytical models.

The repo currently assumes that raw data is loaded to and transformed on a local or remote **PostgreSQL** instance. The load script outlined below and the dbt models could be easily modified to work with other databases supported by dbt such as Snowflake, BigQuery or Redshift. PRs welcome!

## Update Frequency
The `nflscrapR-data` repo is updated with some regularity, but since this is a voluntary and free resource, we can't rely on play data being updated weekly. So, this dataset and the analytical models are best used for teaching and model building purposes, and perhaps less so for weekly decision on sports bets etc.

## Models
- `dates`: list of all game dates by season and season type (`PRE`, `REG`, `POST`)
- `games`: game id, dates, teams and final scores by game 
- `players`: player id and name for every player
- `plays`: combines play data from all available seasons (2009 to 2019) into a single table for easier analysis
- `teams`: team code and consolidated code, in case of team moves of renames
- `teams_players`: team rosters by season, showing player and (primary) position for the season

### XA (Transformed Aggregates)
These models are aggregates of one or more of the models above:
- `xa_field_goals`: field goal plays only with additional information about kick angle


### Notes 
- a few missing `player_id` values in the `players` and `teams_player` models have been (at least attempted to be) fixed
- any duplicate plays (likely a result of the scraping process) are removed from `plays`

## Data Load
The repo assumes that the raw scraped data has been loaded to either a **PostgreSQL** or **BigQuery** database, with one raw file corresponding to a single table in a database called `raw`.

The included Python script [`extract_load`](extract_load) is intended to do the following:
- Clone and/or locally refresh the `nflscrapR-data` repo
- Create empty tables in a BigQuery or Postgres instance
- Load raw data files to Postgres using a `dbt run-operation` to load each file using Postgres' `copy` command

The script uses the connection info defined in your local `~/.dbt/profiles.yml` file and needs to be configured with the appropriate profile name and target to use:

E.g.:
```yaml
dbt_profile_name = "nfl"
dbt_target_name = "pg_local"
```
The load portion currently only works for Postgres and BigQuery, but could probably be extended to work with Snowflake and Redshift (:OOF:) as well.

## Future Work
The following items would make great natural extensions and improvements to the repo:
- Add support for Snowflake and Redshift
- Add report models to more easily enable analytical models:
    - Player stats
    - Game stats
    - Season stats
- Remove dependency on `nflscrapR-data` and include `R` scripts to scrape the data independently