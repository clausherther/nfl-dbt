import os
from pathlib import Path
import argparse

import csv
import multiprocessing
from joblib import Parallel, parallel_backend, delayed

import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, TIMESTAMP


num_cores = multiprocessing.cpu_count()
N_JOBS = min(num_cores, 8)
FIELD_SEP = ","


def get_parsed_args():

    parser = argparse.ArgumentParser()

    parser.add_argument(
        "-s",
        "--sources",
        nargs="+",
        required=False,
        help="List of sources to process, e.g. pbp   rosters",
    )

    parser.add_argument(
        "-y",
        "--years",
        nargs="+",
        required=False,
        help="Years to process, e.g. 2022 2023",
    )

    parser.add_argument(
        "-np",
        "--no_prep",
        action="store_true",
        required=False,
        help="If set, skip data prep",
    )

    parser.add_argument(
        "-nl",
        "--no_load",
        action="store_true",
        required=False,
        help="If set, skip data load",
    )

    args = parser.parse_args()

    return args


def read_yaml(yaml_path, storage_model="local"):

    try:
        with open(Path(yaml_path).resolve(), "r") as f:
            yml = yaml.load(f, Loader=yaml.FullLoader)
    except FileNotFoundError:
        print(f"Could not find {yaml_path}. Please check that {yaml_path} exists.")
        raise

    return yml


def get_data(file_path):
    df = pd.read_csv(file_path, low_memory=False)
    return df


def save_df(df, file_path):

    # Path(file_path).parents[0].mkdir(parents=True, exist_ok=True)
    df.to_csv(
        file_path,
        index=False,
        sep=FIELD_SEP,
        quoting=csv.QUOTE_MINIMAL,
        compression="gzip",
    )


def fix_dtypes(df):

    dtypes = {}

    for col, _ in df.dtypes.iteritems():
        # if df.dtypes[col] == "int64":
        #     df[col] = df[col].fillna(0)
        #     dtypes[col] = Integer()
        # elif df.dtypes[col] == "float64":
        #     df[col] = df[col].fillna(0)
        # if "game_date" in col:
        #     dtypes[col] = TIMESTAMP
        # # elif df.dtypes[col] == "O":
        # else:
        # df[col] = df[col].fillna("")

        dtypes[col] = String()

    if "desc" in df.columns:
        df.drop(columns=["desc"], inplace=True)

    return df, dtypes


def create_empty_table(engine, df, dtypes, table_name, schema_name, replace=False):

    create_sql = pd.io.sql.get_schema(
        df, f"{schema_name}.{table_name}", con=engine, dtype=dtypes
    )
    if not replace:
        drop_sql = None
        create_sql = create_sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    else:
        drop_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"

    create_sql = create_sql.replace('"', "")
    # create_sql = create_sql.replace("desc ", '"desc" ')

    try:
        with engine.connect() as con:
            if drop_sql:
                con.execute(drop_sql)

            con.execute(create_sql)
    except Exception as ex:
        print(f"EXCEPTION: {ex}")


def create_tables(
    data_frames,
    load_config,
    dry_run=False,
    replace=False,
):

    schema_name = load_config["load_schema"]
    for f in data_frames:
        table_name = f["table_name"]
        df = f["data"]
        dtypes = f["dtypes"]

        if not dry_run:
            engine = get_engine(load_config)
            print(f"Creating table {schema_name}.{table_name}...")
            create_empty_table(
                engine,
                df,
                dtypes,
                table_name,
                schema_name,
                replace=replace,
            )


def prep_data_file(data_file, load_config):

    dfs = {}

    file_path = data_file.resolve()
    print(f"Processing {file_path}...")

    if data_file.suffix == "csv":
        table_name = data_file.stem
    else:
        table_name = Path(data_file.stem).stem

    print("Table: ", table_name)

    data_path = Path(load_config["base_dir"], load_config["data_dir"])
    parent_dir = data_file.parents[0].relative_to(data_path)

    df = get_data(file_path)

    date_col = "game_date"

    if date_col in df.columns:
        df[date_col] = df[date_col].apply(
            lambda x: pd.to_datetime(x) + pd.Timedelta(seconds=1)
        )
    # elif id_col in df.columns:
    #     df[date_col] = df[id_col].apply(
    #         lambda x: pd.to_datetime(str(x)[:8]) + pd.Timedelta(seconds=1)
    #     )

    df, dtypes = fix_dtypes(df)

    dfs["table_name"] = table_name
    dfs["data"] = df
    dfs["dtypes"] = dtypes

    save_ext = "gzip"  # don't use double extensions like csv.gz
    target_file_path = Path(
        load_config["base_dir"],
        load_config["load_dir"],
        parent_dir,
        f"{table_name}.{save_ext}",
    ).resolve()

    print(target_file_path)
    save_df(df, target_file_path)

    return dfs


def prep_data_files(data_files, load_config):

    backend = parallel_backend("multiprocessing")

    print(f"Spinning up {N_JOBS} jobs on {num_cores} cores...")

    with backend:

        dfs = Parallel(n_jobs=N_JOBS, verbose=10)(
            delayed(prep_data_file)(data_file, load_config) for data_file in data_files
        )

    return dfs


def data_prep(
    load_config,
    sources_list,
    years_list,
    replace,
):

    file_filter = "*.csv*"
    years_filter = [f"{y}.csv" for y in years_list] if years_list else [".csv"]

    for sub_folder in sources_list:
        p = Path(load_config["data_dir"], sub_folder)
        print(p, file_filter)

        data_files = sorted(list(p.rglob(file_filter)))

        data_files = (
            [f for f in data_files if list(filter(f.stem.endswith, years_filter)) != []]
            if years_list
            else data_files
        )

        print(data_files)
        dfs = prep_data_files(data_files, load_config)

        create_tables(
            dfs,
            load_config,
            replace=replace,
        )


def data_load_pg(
    load_path, file_filter, db_host, db_user, db_password, load_database, load_schema
):

    for load_file in load_path.rglob(file_filter):

        table_name = f"{load_schema}.{load_file.stem}"
        raw_file_path = load_file.resolve()
        print(f"Loading {raw_file_path}...")

        truncate_cmd = f"truncate table {table_name};"
        copy_cmd = (
            f"\\copy {table_name} from '{raw_file_path}' with delimiter ',' csv header;"
        )
        psql_cmd = f"PGPASSWORD={db_password} psql --host={db_host} --port=5432 --username={db_user} -w --dbname={load_database}"

        cmd = psql_cmd + f' --command="{truncate_cmd}"'
        print(cmd)
        os.system(cmd)

        cmd = psql_cmd + f' --command="{copy_cmd}"'
        print(cmd)
        os.system(cmd)


def data_load_bigquery(load_config, sources_list, years_list):

    for sub_folder in sources_list:

        sub_path = Path(load_config["load_path"], sub_folder)
        load_schema = load_config["load_schema"]
        load_database = load_config["load_database"]

        load_files = sub_path.rglob("*.*")

        load_files_filtered = (
            [f for f in load_files if list(filter(f.stem.endswith, years_list)) != []]
            if years_list
            else load_files
        )

        for load_file in load_files_filtered:

            table_name = f"{load_schema}.{load_file.stem}"
            raw_file_path = load_file.resolve()
            print(f"Loading {raw_file_path}...")

            bq_cmd = f'bq load --project_id {load_database} --dataset_id {load_schema} --replace --skip_leading_rows 1 --field_delimiter="{FIELD_SEP}" --source_format CSV {table_name} {raw_file_path}'

            print(bq_cmd)
            os.system(bq_cmd)


def get_load_config():

    load_config = {}

    load_config["base_dir"] = "data_prep_new"
    load_config["load_dir"] = "data_files_load"
    load_config["data_dir"] = "/Users/claus/dev/nflverse-data/data"

    dbt_profiles_path = Path(Path.home(), ".dbt", "profiles.yml")
    dbt_profiles = read_yaml(dbt_profiles_path)

    dbt_profile_name = "nfl"
    # dbt_target_name = "pg_local"
    dbt_target_name = "bq"

    dbt_profile = dbt_profiles[dbt_profile_name]["outputs"][dbt_target_name]

    db_type = dbt_profile.get("type")
    db_host = dbt_profile.get("host")

    db_user = dbt_profile.get("user")
    db_password = dbt_profile.get("password")
    load_database = "nfl-pbp"
    load_schema = "raw_new"

    load_config["db_type"] = db_type
    load_config["db_host"] = db_host
    load_config["db_user"] = db_user
    load_config["key_file_path"] = dbt_profile.get("keyfile")
    load_config["db_password"] = db_password
    load_config["load_database"] = load_database
    load_config["load_schema"] = load_schema
    load_config["load_path"] = Path(load_config["base_dir"], load_config["load_dir"])

    return load_config


def get_engine(load_config):

    if load_config["db_type"] == "postgres":
        url = f"postgresql://{load_config['db_user']}:{load_config['db_password']}@{load_config['db_host']}:5432/{load_config['load_database']}"
        engine = create_engine(url, echo=True)

    elif load_config["db_type"] == "bigquery":
        url = f"bigquery://{load_config['load_database']}/{load_config['load_schema']}"
        key_file_path = load_config["key_file_path"]
        engine = create_engine(url, credentials_path=key_file_path, echo=False)

    return engine


def main():

    args = get_parsed_args()

    do_prep = not args.no_prep
    do_load = not args.no_load

    # base_dir, load_dir, data_dir, file_filter, db_type, db_host, db_user, db_password, load_database, load_schema, engine
    load_config = get_load_config()

    Path(load_config["base_dir"]).mkdir(exist_ok=True)

    sources_list = args.sources if args.sources else ["pbp", "players", "rosters"]
    years_list = args.years

    if do_prep:
        replace = True
        data_prep(
            load_config,
            sources_list,
            years_list,
            replace,
        )

    if do_load:
        if load_config["db_type"] == "postgres":
            data_load_pg(
                load_config,
                sources_list,
                years_list,
            )
        elif load_config["db_type"] == "bigquery":
            data_load_bigquery(
                load_config,
                sources_list,
                years_list,
            )


if __name__ == "__main__":

    main()
