# NFL Play-by-play dbt models

This repo contains [dbt](https://www.getdbt.com) models to transform NFL Play-by-Play (pbp) data sourced from [https://github.com/nflverse/nflverse-data](https://github.com/nflverse/nflverse-data) into analytical models.
## Update Frequency
The `nflverse-data` repo is updated with some regularity, but since this is a voluntary and free resource, we can't rely on play data being updated weekly. So, this dataset and the analytical models are best used for teaching and model building purposes, and perhaps less so for weekly decision on sports bets etc.

## Models
- `dates`: list of all game dates by season and season type (`PRE`, `REG`, `POST`)
- `games`: game id, dates, teams and final scores by game 
- `players`: player id and name for every player
- `plays`: combines play data from all available seasons (1999 to most current season) into a single table for easier analysis
- `teams`: team code and consolidated code, in case of team moves of renames
- `teams_players`: team rosters by season, showing player and (primary) position for the season

### XA (Transformed Aggregates)
These models are aggregates of one or more of the models above:
- `xa_field_goals`: field goal plays only with additional information about kick angle, to help model field goal success probabilities (e.g. https://calogica.com/pymc3/python/2020/01/10/nfl-field-goals-bayes.html)
- `xa_fourth_downs`: fourth down, non-field goal attemp plays only, to help with fourth-down-conversion modeling (e.g. https://calogica.com/pymc3/python/2019/12/08/nfl-4thdown-attempts.html)


### Notes 
- a few missing `player_id` values in the `players` and `teams_player` models have been (at least attempted to be) fixed
- any duplicate plays (likely a result of the scraping process) are removed from `plays`

## Data Load
The repo assumes that the raw scraped data has been loaded to a **BigQuery** database, with one raw file corresponding to a single table in a database called `raw`.

The included Python script [`extract_load`](extract_load) is intended to do the following:
- ~Clone and/or locally refresh the `nflverse-data` repo~ (TBD)
- Create empty tables in a BigQuery instance
- Load raw data files to BigQuery using a `bq load` job to load each file

The script uses the connection info defined in your local `~/.dbt/profiles.yml` file and needs to be configured with the appropriate profile name and target to use:

E.g.:
```python
dbt_profile_name = "nfl"
dbt_target_name = "bq"
```
The load portion currently only works for BigQuery, but could probably be extended to work with Snowflake and Redshift (:OOF:) as well.

## Future Work
The following items would make great natural extensions and improvements to the repo:
- Add support for Snowflake
- Add report models to more easily enable analytical models:
    - Player stats
    - Game stats
    - Season stats
