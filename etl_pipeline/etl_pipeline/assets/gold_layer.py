import pandas as pd
from dagster import asset, Output, AssetIn
from datetime import datetime
from pyspark.sql import SparkSession

@asset(
    ins = {
        "covid19_cases_infos"           : AssetIn(key_prefix = ["silver", "medical"]),
        "covid19_cases_by_time"         : AssetIn(key_prefix = ["silver", "medical"]),
    },
    description="CLEAN Day-by-day/Lat-long no. of cases",
    key_prefix=["gold", "medical"],
    io_manager_key="spark_io_manager",
    group_name="gold_layer",
    compute_kind="plotly"
)
def covid19_daily_stats(context, 
                        covid19_cases_infos: pd.DataFrame, 
                        covid19_cases_by_time: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("covid19-benchmark-{}".format(datetime.today()))
                .master("spark://spark-master:7077")
                .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    spark_covid19_cases_infos          = spark.createDataFrame(covid19_cases_infos)
    spark_covid19_cases_by_time        = spark.createDataFrame(covid19_cases_by_time)
    spark_covid19_cases_infos.createOrReplaceTempView("covid19_cases_infos")
    spark_covid19_cases_by_time.createOrReplaceTempView("covid19_cases_by_time")
    sql_stm = """
    -- 1
    SELECT 
        t.`date`, 
        t.country_region, 
        t.confirmed, 
        t.deaths, 
        t.recovered,
        t.active,
        c.lat as latitude,
        c.long as longtitude,
        c.who_region
    FROM 
        covid19_cases_by_time AS t
    JOIN 
        covid19_cases_infos AS c 
    ON 
        t.country_region = c.country_region
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
    
@asset(
    ins = {
        "covid19_cases_country"   : AssetIn(key_prefix = ["silver", "medical"])
    },
    description="STATISTICS Continent table",
    key_prefix=["gold", "medical"],
    io_manager_key="spark_io_manager",
    group_name="gold_layer",
    compute_kind="pandas"
)
def covid19_continent_stats(context, 
                            covid19_cases_country: pd.DataFrame) -> Output[pd.DataFrame]:
    spark = (SparkSession.builder.appName("covid19-benchmark-{}".format(datetime.today()))
                .master("spark://spark-master:7077")
                .getOrCreate())
    spark.conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    spark.conf.set("spark.sql.execution.arrow.pyspark.fallback.enabled","true")
    spark_covid19_cases_country = spark.createDataFrame(covid19_cases_country)
    spark_covid19_cases_country.createOrReplaceTempView("covid19_cases_country")
    sql_stm = """
    SELECT 
        w.continent AS Continent, 
        SUM(w.confirmed) AS TotalCases, 
        SUM(w.deaths) AS TotalDeaths, 
        SUM(w.recovered) AS TotalRecovered 
    FROM 
        covid19_cases_country AS w 
    GROUP BY 
        w.continent;
    """
    sparkDF = spark.sql(sql_stm)
    pd_data = sparkDF.toPandas()
    context.log.debug("Nothing")
    return Output(
        pd_data,
        metadata={
            "table": "covid19_continent_stats",
            "records counts": len(pd_data),
        },
    )