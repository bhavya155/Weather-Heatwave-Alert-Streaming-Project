# Databricks notebook source
check_point_dir="/mnt/devadlsprojects/checkpoint"

# COMMAND ----------

# MAGIC %md
# MAGIC Intial loading full load with avtive flag as no

# COMMAND ----------

from pyspark.sql.functions import col, lit, from_unixtime

# Read all heatwave alerts from silver table
silver_df = spark.readStream.table("db_projects.dev.weather_sv")

initial_load = silver_df \
    .filter((col("temperature") > 35) & (col("humidity") < 30)) \
    .select(
        "city_name",
        "temperature",
        "humidity",
        "weather_description",
        from_unixtime(col("timestamp_epoch")).alias("start_date")
    ) \
    .withColumn("end_date", lit(None).cast("timestamp")) \
    .withColumn("is_active", lit("no"))

# Save to Delta table (overwrite mode)
initial_load.writeStream.queryName("HeatWave").option("checkpointLocation", f"{check_point_dir}/HeatWaveAlret").outputMode("append").toTable("db_projects.dev.heatwave_alerts_gold")


# COMMAND ----------

# MAGIC %md
# MAGIC Streaming

# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_unixtime, lit
from delta.tables import DeltaTable

spark = SparkSession.builder.getOrCreate()
# read table
def readSilver():
    return spark.readStream.table("db_projects.dev.weather_sv")

#  Filter the records
def getAggregates(weather_df):
    return weather_df.filter((col("temperature") > 35) & (col("humidity") < 30)) \
        .select(
            "city_name",
            "temperature",
            "humidity",
            "weather_description",
            from_unixtime(col("timestamp_epoch")).alias("start_date")
        ) \
        .withColumn("end_date", lit(None).cast("timestamp")) \
        .withColumn("is_active", lit("yes"))
 
#  for every microbatch merge to target table
def aggregate_upsert(newAlertDf, batch_id):
    delta_table = DeltaTable.forName(spark, "db_projects.dev.heatwave_alerts_gold")
    
    # Get records already present in target with is_active='yes'
    existing_df = delta_table.toDF().select("city_name", "start_date")

    #  Do an anti-join: keep only new records not already processed
    filtered_new = newAlertDf.alias("new").join(
        existing_df.alias("existing"),
        on=["city_name", "start_date"],
        how="left_anti"
    )
    # Active records in heatwave alert table
    expired = delta_table.toDF() \
        .alias("existing") \
        .join(filtered_new.alias("new"),
              (col("existing.city_name") == col("new.city_name")) &
              (col("existing.is_active") == lit("yes"))
        ) \
        .selectExpr("existing.city_name","existing.temperature","existing.humidity","existing.weather_description","existing.start_date","new.start_date as end_date")
    
    old= expired.withColumn("is_active", lit("no"))

    # Union expired and new alerts
    final_df = old.unionByName(filtered_new)
    

    final_df.createOrReplaceTempView("heat_wave_df_temp_view")

    merge_statement = """
        MERGE INTO db_projects.dev.heatwave_alerts_gold t
        USING heat_wave_df_temp_view s
        ON t.city_name = s.city_name AND t.start_date = s.start_date
        WHEN MATCHED THEN
          UPDATE SET t.temperature = s.temperature,
                     t.humidity = s.humidity,
                     t.weather_description = s.weather_description,
                     t.is_active = s.is_active,
                     t.end_date = s.end_date
        WHEN NOT MATCHED THEN
          INSERT *
    """

    final_df._jdf.sparkSession().sql(merge_statement)

def saveResults(newAlertDf):
    print("\nStarting gold Stream...", end='')
    return (newAlertDf.writeStream
                .queryName("gold-update")
                .option("checkpointLocation", f"{check_point_dir}/HeatWaveAlret")
                .outputMode("update")
                .foreachBatch(aggregate_upsert)
                .start())

def process():
    weather_df = readSilver()
    newAlertDf = getAggregates(weather_df)
    sQuery = saveResults(newAlertDf)
    return sQuery


# COMMAND ----------

process()

# COMMAND ----------

# MAGIC
# MAGIC %sql
# MAGIC select * from db_projects.dev.heatwave_alerts_gold where city_name ='Hyderabad' order by start_date desc
