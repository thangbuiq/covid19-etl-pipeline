import pandas as pd
from dagster import asset, Output

asset_list = ['covid19_cases_position','covid19_country_wise','covid19_time_series','covid19_worldometer']

@asset(
    io_manager_key="minio_io_manager",
    description="(Lat/Long) wise no. of cases ",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "medical"],
    group_name = "bronze_layer",
    compute_kind="MYSQL"
)
def covid19_cases_position(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_cases_position"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "covid19_cases_position",
        "records count": len(pd_data),
        },
    )
    
@asset(
    io_manager_key="minio_io_manager",
    description="Latest country level no. of cases",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "medical"],
    group_name = "bronze_layer",
    compute_kind="MYSQL"
)
def covid19_country_wise(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_country_wise"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "covid19_country_wise",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    description="Date-country wise no. of cases",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "medical"],
    group_name = "bronze_layer",
    compute_kind="MYSQL"
)
def covid19_time_series(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_time_series"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "covid19_time_series",
        "records count": len(pd_data),
        },
    )

@asset(
    io_manager_key="minio_io_manager",
    description="Latest Worldometers Covid-19 data",
    required_resource_keys={"mysql_io_manager"},
    key_prefix=["bronze", "medical"],
    group_name = "bronze_layer",
    compute_kind="MYSQL"
)
def covid19_worldometer(context) -> Output[pd.DataFrame]:
    sql_stm = "SELECT * FROM covid19_worldometer"
    pd_data = context.resources.mysql_io_manager.extract_data(sql_stm)
    return Output(
        pd_data,
        metadata={
        "table": "covid19_worldometer",
        "records count": len(pd_data),
        },
    )
