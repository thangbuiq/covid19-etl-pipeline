import pandas as pd
from dagster import asset, Output, AssetIn
from datetime import datetime
from pyspark.sql import SparkSession

@asset(
    ins = {
        "covid19_country_wise"  : AssetIn(key_prefix = ["bronze", "medical"]),
        "covid19_worldometer"   : AssetIn(key_prefix = ["bronze", "medical"]),
    },
    description="FILTERED country no. of cases",
    key_prefix=["silver", "medical"],
    io_manager_key="spark_io_manager",
    group_name="silver_layer",
    compute_kind="PySpark"
)
def covid19_cases_country(context, 
                                covid19_country_wise: pd.DataFrame,
                                covid19_worldometer: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("covid19-benchmark-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    spark_covid19_country_wise = spark.createDataFrame(covid19_country_wise)
    spark_covid19_worldometer  = spark.createDataFrame(covid19_worldometer)
    spark_covid19_country_wise.createOrReplaceTempView("covid19_country_wise")
    spark_covid19_worldometer.createOrReplaceTempView("covid19_worldometer")
    sql_stm = """
    SELECT 
        cw.country_region,
        w.continent,
        w.population,
        cw.confirmed,
        cw.deaths,
        cw.recovered,
        cw.who_region
    FROM 
        covid19_country_wise AS cw 
    JOIN covid19_worldometer AS w
    ON w.country_region = cw.country_region
    """
    sparkDF = spark.sql(sql_stm)
    pd_data = sparkDF.toPandas()
    context.log.debug("Nothing")
    return Output(
        pd_data,
        metadata={
            "table": "covid19_cases_country",
            "records counts": len(pd_data),
        },
    )
    
@asset(
    ins = {
        "covid19_cases_position"  : AssetIn(key_prefix = ["bronze", "medical"]),
    },
    description="FILTERED Position no. of cases",
    key_prefix=["silver", "medical"],
    io_manager_key="spark_io_manager",
    group_name="silver_layer",
    compute_kind="PySpark"
)
def covid19_cases_infos(context, covid19_cases_position: pd.DataFrame,) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("covid19-benchmark-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    spark_covid19_cases_position = spark.createDataFrame(covid19_cases_position)
    spark_covid19_cases_position.createOrReplaceTempView("covid19_cases_position")
    sql_stm = """
    SELECT 
        country_region,
        lat,
        long,
        date,
        confirmed,
        deaths,
        recovered,
        who_region
    FROM 
        covid19_cases_position
    """
    sparkDF = spark.sql(sql_stm)
    pd_data = sparkDF.toPandas()
    context.log.debug("Nothing")
    return Output(
        pd_data,
        metadata={
            "table": "covid19_cases_infos",
            "records counts": len(pd_data),
        },
    )
    
@asset(
    ins = {
        "covid19_time_series"  : AssetIn(key_prefix = ["bronze", "medical"]),
    },
    description="FILTERED Timeseries no. of cases",
    key_prefix=["silver", "medical"],
    io_manager_key="spark_io_manager",
    group_name="silver_layer",
    compute_kind="PySpark"
)
def covid19_cases_by_time(context, covid19_time_series: pd.DataFrame,) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("covid19-benchmark-{}".format(datetime.today()))
            .master("spark://spark-master:7077")
            .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    spark_covid19_time_series = spark.createDataFrame(covid19_time_series)
    spark_covid19_time_series.createOrReplaceTempView("covid19_time_series")
    sql_stm = """
    SELECT * FROM covid19_time_series
    """
    sparkDF = spark.sql(sql_stm)
    pd_data = sparkDF.toPandas()
    context.log.debug("Nothing")
    return Output(
        pd_data,
        metadata={
            "table": "covid19_cases_by_time",
            "records counts": len(pd_data),
        },
    )
    
