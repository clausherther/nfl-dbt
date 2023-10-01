import yaml
import duckdb
from typing import NamedTuple, List
import logging


RAW_SOURCE_DB = "raw"
CONFIG_PATH = "nflverse.yml"
DUCKDB_PATH = "nfl.duckdb"

logging.basicConfig(format="%(asctime)s - %(levelname)s: %(message)s", level=logging.INFO)
logger = logging.getLogger()


class Source(NamedTuple):
    name: str
    base_url: str
    files: List[str]


def load_config() -> List[Source]:
    with open(CONFIG_PATH, "r") as f:
        config = yaml.load(f, Loader=yaml.FullLoader)
    for i, source in enumerate(config):
        config[i] = Source(**source)
    return config


def load_files(config):
    cursor = duckdb.connect(DUCKDB_PATH)
    cursor.execute(f"""CREATE SCHEMA IF NOT EXISTS "{RAW_SOURCE_DB}" """)
    for source in config:
        for file in source.files:
            url = f"{source.base_url}/{file}"
            logger.info(f"loading {url}")
            rows = cursor.execute(
                f"""CREATE OR REPLACE TABLE "{RAW_SOURCE_DB}"."{file.removesuffix(".parquet")}" as """
                f"""SELECT * FROM '{url}'"""
            ).fetchone()[0]
            logger.info(f"loaded {rows} rows")
    cursor.close()


def main():
    config = load_config()
    load_files(config)


if __name__ == "__main__":
    main()
