import pandas as pd
from dagster import Output, AssetIn, multi_asset, AssetOut

@multi_asset(
    ins={
        "covid19_daily_stats": AssetIn(key_prefix=["gold", "medical"],)
    },
    outs={
        "covid19_daily_stats": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="warehouse_layer"
        )
    },
    description="WAREHOUSE date-by-date with pos of cases",
    compute_kind="postgres"
    )
def warehouse_covid19_daily_stats(covid19_daily_stats) -> Output[pd.DataFrame]:
    return Output(
        covid19_daily_stats,
        metadata={
            "schema": "public",
            "table": "covid19_daily_stats",
            "records counts": len(covid19_daily_stats),
        },
    )
    
@multi_asset(
    ins={
        "covid19_continent_stats": AssetIn(key_prefix=["gold", "medical"],)
    },
    outs={
        "covid19_continent_stats": AssetOut(
            io_manager_key="psql_io_manager",
            key_prefix=["warehouse", "public"],
            group_name="warehouse_layer"
        )
    },
    description="WAREHOUSE total cases of continent",
    compute_kind="postgres"
    )
def warehouse_covid19_continent_stats(covid19_continent_stats) -> Output[pd.DataFrame]:
    return Output(
        covid19_continent_stats,
        metadata={
            "schema": "public",
            "table": "covid19_continent_stats",
            "records counts": len(covid19_continent_stats),
        },
    )