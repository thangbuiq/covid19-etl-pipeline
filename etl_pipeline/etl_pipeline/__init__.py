import os
from dagster import Definitions
from .assets.bronze_layer import covid19_cases_position, covid19_country_wise, covid19_time_series, covid19_worldometer
from .assets.silver_layer import covid19_country_by_continent, covid19_cases_by_time, covid19_cases_infos
from .assets.gold_layer   import covid19_daily_stats
from .resources.minio_io_manager import MinIOIOManager
from .resources.mysql_io_manager import MySQLIOManager
from .resources.psql_io_manager import PostgreSQLIOManager
from .resources.spark_io_manager import SparkIOManager

MYSQL_CONFIG = {
    "host": os.getenv("MYSQL_HOST"),
    "port": os.getenv("MYSQL_PORT"),
    "database": os.getenv("MYSQL_DATABASE"),
    "user": os.getenv("MYSQL_USER"),
    "password": os.getenv("MYSQL_PASSWORD"),
}
MINIO_CONFIG = {
    "endpoint_url": os.getenv("MINIO_ENDPOINT"),
    "bucket": os.getenv("DATALAKE_BUCKET"),
    "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID"),
    "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY"),
}
PSQL_CONFIG = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "database": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "password": os.getenv("POSTGRES_PASSWORD"),
}
defs = Definitions(
    assets=[
        covid19_cases_position,
        covid19_country_wise,
        covid19_time_series,
        covid19_worldometer,
        covid19_country_by_continent,
        covid19_cases_by_time,
        covid19_cases_infos,
        covid19_daily_stats
    ],
    resources={
        "mysql_io_manager": MySQLIOManager(MYSQL_CONFIG),
        "minio_io_manager": MinIOIOManager(MINIO_CONFIG),
        "psql_io_manager": PostgreSQLIOManager(PSQL_CONFIG),
        "spark_io_manager": SparkIOManager(MINIO_CONFIG)
    }
)