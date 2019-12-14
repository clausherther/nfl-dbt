import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String

"""
DB Config for Postgres, adjust accordingly
for other database supported by SQLAlchemy.
(Redshift, Snowflake, BigQuery)
"""

db_host = "localhost"
load_database = "nfl"
load_schema = "raw"
url = f"postgresql+psycopg2://{db_host}:5432/{load_database}"
engine = create_engine(url)


def get_data(file_path):
    df = pd.read_csv(file_path, low_memory=False)
    return df


def save_df(df, file_path):

    Path(file_path).parents[0].mkdir(parents=True, exist_ok=True)
    df.to_csv(file_path, index=False)


def fix_dtypes(df):

    dtypes = {}

    for col, _ in df.dtypes.iteritems():
        if df.dtypes[col] == "int64":
            df[col] = df[col].fillna(0)
            dtypes[col] = Integer()
        elif df.dtypes[col] == "float64":
            df[col] = df[col].fillna(0)
        elif df.dtypes[col] == "O":
            df[col] = df[col].fillna("")
            dtypes[col] = String()

    return df, dtypes


def create_empty_table(engine, df, dtypes, table_name, schema_name):

    try:
        with engine.connect() as con:
            df.head(0).to_sql(
                name=table_name,
                schema=schema_name,
                con=con,
                if_exists="fail",
                index=False,
                dtype=dtypes
            )
    except ValueError as ex:
        print(ex)


def prep_data_files(data_files, target_dir):

    dfs = {}
    for f in data_files:
        file_path = f.resolve()
        table_name = f.stem
        parent_path = f.parents[0].relative_to(Path(base_dir, data_dir))

        df = get_data(file_path)
        df, dtypes = fix_dtypes(df)

        dfs[table_name] = {}
        dfs[table_name]["data"] = df
        dfs[table_name]["dtypes"] = dtypes
        target_file_path = Path(base_dir, target_dir, parent_path, f"{table_name}.csv").resolve()
        save_df(df, target_file_path)

    return dfs


def create_tables(data_frames, schema_name, dry_run=False):

    for table_name, value in data_frames.items():
        df = value["data"]
        dtypes = value["dtypes"]
        if not dry_run:
            create_empty_table(engine, df, dtypes, table_name, schema_name)


"""
Clone or refresh nflscrapR-data Github repo
"""
base_dir = "data_prep"
load_dir = "data_files_load"
data_dir = "nflscrapR-data"
file_filter = "*_2019.csv"
# file_filter = "*.csv"

Path(base_dir).mkdir(exist_ok=True)

if not Path(base_dir, data_dir).exists():
    clone_cmd = f"cd {base_dir}; git clone https://github.com/ryurko/nflscrapR-data.git"
    os.system(clone_cmd)
else:
    os.system(f"cd {base_dir};cd {data_dir};git pull")

source_data_sub_folders = ["games_data", "play_by_play_data", "roster_data"]

for sub_folder in source_data_sub_folders:
    p = Path(base_dir, data_dir, sub_folder)
    data_files = sorted(list(p.rglob(file_filter)))

    dfs = prep_data_files(data_files, load_dir)
    create_tables(dfs, load_schema, dry_run=False)


load_path = Path(base_dir, load_dir)

for load_file in load_path.rglob(file_filter):

    table_name = f"{load_schema}.{load_file.stem}"
    raw_file_path = load_file.resolve()
    dbt_var = f"'file_path':'{raw_file_path}', 'table_name': '{table_name}'"
    dbt_ops_cmd = f'dbt run-operation load_raw_pbp_data --target pg --vars "{{ {dbt_var} }}"'
    print(dbt_ops_cmd)
    os.system(dbt_ops_cmd)
