import pandas as pd
from dagster import asset, Output, AssetIn
from datetime import datetime
from pyspark.sql import SparkSession

#covid19_daily_stats
@asset(
    ins = {
        "covid19_cases_infos" : AssetIn(key_prefix = ["silver", "medical"]),
        "covid19_country_by_continent"   : AssetIn(key_prefix = ["silver", "medical"]),
        "covid19_cases_by_time"   : AssetIn(key_prefix = ["silver", "medical"]),
    },
    key_prefix=["gold", "medical"],
    io_manager_key="spark_io_manager",
    group_name="gold_layer",
    compute_kind="PySpark"
)
def covid19_daily_stats(context, 
                        covid19_cases_infos: pd.DataFrame, 
                        covid19_country_by_continent: pd.DataFrame,
                        covid19_cases_by_time: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("covid19-benchmark-{}".format(datetime.today()))
                .master("spark://spark-master:7077")
                .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    spark_covid19_cases_infos          = spark.createDataFrame(covid19_cases_infos)
    spark_covid19_country_by_continent = spark.createDataFrame(covid19_country_by_continent)
    spark_covid19_cases_by_time        = spark.createDataFrame(covid19_cases_by_time)
    spark_covid19_cases_infos.createOrReplaceTempView("covid19_cases_infos")
    spark_covid19_country_by_continent.createOrReplaceTempView("covid19_country_by_continent")
    spark_covid19_cases_by_time.createOrReplaceTempView("covid19_cases_by_time")
    sql_stm = """
    -- 1
    SELECT 
        t.`date`, 
        t.country_region, 
        t.confirmed, 
        t.deaths, 
        t.recovered
    FROM 
        covid19_cases_by_time AS t
    JOIN 
        covid19_cases_infos AS c 
    ON 
        t.country_region = c.country_region
    JOIN 
        covid19_country_by_continent AS w 
    ON 
        t.country_region = w.country_region
    WHERE 
        t.confirmed > 0 OR 
        t.deaths > 0 OR 
        t.recovered > 0;
    """
    sparkDF = spark.sql(sql_stm)
    pd_data = sparkDF.toPandas()
    context.log.debug("Nothing")
    return Output(
        pd_data,
        metadata={
            "table": "covid19_daily_stats",
            "records counts": len(pd_data),
        },
    )