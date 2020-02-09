import os
from pathlib import Path

import multiprocessing
from joblib import Parallel, parallel_backend, delayed

import pandas as pd
import yaml
from sqlalchemy import create_engine
from sqlalchemy.types import Integer, String, TIMESTAMP


num_cores = multiprocessing.cpu_count()
N_JOBS = num_cores


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
        elif "_date" in col:
            dtypes[col] = TIMESTAMP
        elif df.dtypes[col] == "O":
            df[col] = df[col].fillna("")
            dtypes[col] = String()

    return df, dtypes


def create_empty_table(engine, df, dtypes, table_name, schema_name, partition_col=None, replace=False):

    create_sql = pd.io.sql.get_schema(df, f"{schema_name}.{table_name}", con=engine, dtype=dtypes)
    if not replace:
        drop_sql = None
        create_sql = create_sql.replace("CREATE TABLE", "CREATE TABLE IF NOT EXISTS")
    else:
        drop_sql = f"DROP TABLE IF EXISTS {schema_name}.{table_name};"

    create_sql = create_sql.replace('"', '')
    create_sql = create_sql.replace('desc ', '"desc" ')

    if partition_col:
        create_sql += f"PARTITION BY {partition_col}"
    # print(create_sql)
    try:
        with engine.connect() as con:
            if drop_sql:
                con.execute(drop_sql)

            con.execute(create_sql)
    except Exception as ex:
        print(f"EXCEPTION: {ex}")


def prep_data_file(data_file, base_dir, data_dir, target_dir):

    dfs = {}

    file_path = data_file.resolve()
    print(f"Processing {file_path}...")
    table_name = data_file.stem
    parent_path = data_file.parents[0].relative_to(Path(base_dir, data_dir))

    df = get_data(file_path)

    date_col = "game_date"
    id_col = "game_id"

    if date_col in df.columns:
        df[date_col] = df[date_col].apply(lambda x: pd.to_datetime(x) + pd.Timedelta(seconds=1))
    elif id_col in df.columns:
        df[date_col] = df[id_col].apply(lambda x: pd.to_datetime(str(x)[:8]) + pd.Timedelta(seconds=1))

    df, dtypes = fix_dtypes(df)

    dfs["table_name"] = table_name
    dfs["data"] = df
    dfs["dtypes"] = dtypes
    target_file_path = Path(base_dir, target_dir, parent_path, f"{table_name}.csv").resolve()
    save_df(df, target_file_path)

    return dfs


def prep_data_files(data_files, base_dir, data_dir, target_dir):

    backend = parallel_backend("multiprocessing")
    N_JOBS = 4

    print(f"Spinning up {N_JOBS} jobs on {num_cores} cores...")

    with backend:

        dfs = Parallel(n_jobs=N_JOBS, verbose=10)(
            delayed(prep_data_file)(
                data_file, base_dir, data_dir, target_dir
            )
            for data_file in data_files
        )

    return dfs


def create_tables(engine, data_frames, schema_name, dry_run=False, replace=False, supports_partitions=True):

    for f in data_frames:
        table_name = f["table_name"]
        df = f["data"]
        dtypes = f["dtypes"]

        date_col = "game_date"
        partition_col = None

        if supports_partitions and date_col in df.columns:
            partition_col = f"date({date_col})"
        else:
            partition_col = None

        if not dry_run:
            print(f"Creating table {schema_name}.{table_name}...")
            create_empty_table(engine, df, dtypes, table_name, schema_name, partition_col, replace=replace)


def clone_nfl_data_repo(base_dir, data_dir):
    if not Path(base_dir, data_dir).exists():
        clone_cmd = f"cd {base_dir}; git clone https://github.com/ryurko/nflscrapR-data.git"
        os.system(clone_cmd)
    else:
        os.system(f"cd {base_dir};cd {data_dir};git pull")


def data_prep(engine, source_data_sub_folders, base_dir, data_dir, load_dir, file_filter, load_schema, replace, supports_partitions):

    for sub_folder in source_data_sub_folders:
        p = Path(base_dir, data_dir, sub_folder)
        data_files = sorted(list(p.rglob(file_filter)))

        dfs = prep_data_files(data_files, base_dir, data_dir, load_dir)
        create_tables(engine, dfs, load_schema, replace=replace, supports_partitions=supports_partitions)


def data_load_pg(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema):

    for load_file in load_path.rglob(file_filter):

        table_name = f"{load_schema}.{load_file.stem}"
        raw_file_path = load_file.resolve()
        print(f"Loading {raw_file_path}...")

        truncate_cmd = f"truncate table {table_name};"
        copy_cmd = f"\\copy {table_name} from '{raw_file_path}' with delimiter ',' csv header;"
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

    do_prep = False
    do_load = True

    """
    Clone or refresh nflscrapR-data Github repo
    """
    base_dir = "data_prep"
    load_dir = "data_files_load"
    data_dir = "nflscrapR-data"
    file_filter = "*_2019.csv"
    # file_filter = "*.csv"

    dbt_profiles_path = Path(Path.home(), ".dbt", "profiles.yml")
    dbt_profiles = read_yaml(dbt_profiles_path)

    dbt_profile_name = "nfl"
    # dbt_target_name = "pg_local"
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
        engine = create_engine(url, echo=True)
        supports_partitions = False
    elif db_type == "bigquery":
        url = f"bigquery://{load_database}"
        key_file_path = dbt_profile["keyfile"]
        engine = create_engine(url, credentials_path=key_file_path, echo=False)
        supports_partitions = True

    Path(base_dir).mkdir(exist_ok=True)
    # clone_nfl_data_repo(base_dir, data_dir)

    source_data_sub_folders = ["games_data", "play_by_play_data", "roster_data"]

    if do_prep:
        replace = True
        data_prep(engine, source_data_sub_folders, base_dir, data_dir, load_dir, file_filter, load_schema, replace, supports_partitions)

    if do_load:
        load_path = Path(base_dir, load_dir)
        if db_type == "postgres":
            data_load_pg(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema)
        elif db_type == "bigquery":
            data_load_bigquery(load_path, file_filter, db_host, db_user, db_password, load_database, load_schema)


if __name__ == "__main__":

    main()
