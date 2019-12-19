import os
from pathlib import Path

import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String


def read_yaml(yaml_path, storage_model='local'):

    try:
        with open(Path(yaml_path).resolve(), 'r') as f:
            yml = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print(f"Could not find {yaml_path}. Please check that {yaml_path} exists.")
        raise

    return yml


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


def prep_data_files(data_files, base_dir, data_dir, target_dir):

    dfs = {}
    for f in data_files:
        file_path = f.resolve()
        print(f"Processing {file_path}...")
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


def create_tables(engine, data_frames, schema_name, dry_run=False):

    for table_name, value in data_frames.items():
        df = value["data"]
        dtypes = value["dtypes"]
        if not dry_run:
            print(f"Creating table {schema_name}.{table_name}...")
            create_empty_table(engine, df, dtypes, table_name, schema_name)


def clone_nfl_data_repo(base_dir, data_dir):
    if not Path(base_dir, data_dir).exists():
        clone_cmd = f"cd {base_dir}; git clone https://github.com/ryurko/nflscrapR-data.git"
        os.system(clone_cmd)
    else:
        os.system(f"cd {base_dir};cd {data_dir};git pull")


def data_prep(engine, source_data_sub_folders, base_dir, data_dir, load_dir, file_filter, load_schema):

    for sub_folder in source_data_sub_folders:
        p = Path(base_dir, data_dir, sub_folder)
        data_files = sorted(list(p.rglob(file_filter)))

        dfs = prep_data_files(data_files, base_dir, data_dir, load_dir)
        create_tables(engine, dfs, load_schema, dry_run=False)


def data_load_pg(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema):

    for load_file in load_path.rglob(file_filter):

        table_name = f"{load_schema}.{load_file.stem}"
        raw_file_path = load_file.resolve()
        print(f"Loading {raw_file_path}...")

        truncate_cmd = f"truncate table {table_name};"
        copy_cmd = f"\copy {table_name} from '{raw_file_path}' with delimiter ',' csv header;"
        psql_cmd = f'PGPASSWORD={db_password} psql --host={db_host} --port=5432 --username={db_user} -w --dbname={load_database}'

        cmd = psql_cmd + f' --command="{truncate_cmd}"'
        print(cmd)
        os.system(cmd)

        cmd = psql_cmd + f' --command="{copy_cmd}"'
        print(cmd)
        os.system(cmd)


def data_load_bigquery(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema):

    for load_file in load_path.rglob(file_filter):
        table_name = f"{load_schema}.{load_file.stem}"
        raw_file_path = load_file.resolve()
        print(f"Loading {raw_file_path}...")

        bq_cmd = f"bq load --project_id {load_database} --dataset_id {load_schema} --replace --skip_leading_rows 1 {table_name} {raw_file_path}"

        print(bq_cmd)
        os.system(bq_cmd)


def main():

    do_prep = True
    do_load = True

    """
    Clone or refresh nflscrapR-data Github repo
    """
    base_dir = "data_prep"
    load_dir = "data_files_load"
    data_dir = "nflscrapR-data"
    # file_filter = "*_2019.csv"
    file_filter = "*.csv"

    """
    DB Config for Postgres, adjust accordingly
    for other database supported by SQLAlchemy.
    (Redshift, Snowflake, BigQuery)
    """

    dbt_profiles_path = Path(Path.home(), ".dbt", "profiles.yml")
    dbt_profiles = read_yaml(dbt_profiles_path)

    dbt_profile_name = "nfl"
    dbt_target_name = "bq"

    dbt_profile = dbt_profiles[dbt_profile_name]["outputs"][dbt_target_name]
    print(dbt_profile)

    db_type = dbt_profile.get("type")
    db_host = dbt_profile.get("host")
    db_user = dbt_profile.get("user")
    db_password = dbt_profile.get("password")
    load_database = dbt_profile.get("dbname", dbt_profile.get("project", "nfl"))
    load_schema = dbt_profile.get("schema", dbt_profile.get("dataset", "raw"))

    if db_type == "postgres":
        url = f"postgresql://{db_user}:{db_password}@{db_host}:5432/{load_database}"
        engine = create_engine(url)
    elif db_type == "bigquery":
        url = f"bigquery://{load_database}"
        key_file_path = dbt_profile["keyfile"]
        engine = create_engine(url, credentials_path=key_file_path)

    Path(base_dir).mkdir(exist_ok=True)
    clone_nfl_data_repo(base_dir, data_dir)

    source_data_sub_folders = ["games_data", "play_by_play_data", "roster_data"]

    if do_prep:
        data_prep(engine, source_data_sub_folders, base_dir, data_dir, load_dir, file_filter, load_schema)

    if do_load:
        load_path = Path(base_dir, load_dir)
        if db_type == "postgres":
            data_load_pg(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema)
        elif db_type == "bigquery":
            data_load_bigquery(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema)


if __name__ == "__main__":

    main()
