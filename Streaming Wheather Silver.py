# Databricks notebook source
check_point_dir="/mnt/devadlsprojects/checkpoint"

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import from_json, col, current_timestamp, lit

# Define Schema 
def getSchema():
    return StructType([
        StructField("coord", StructType([
            StructField("lon", DoubleType()),
            StructField("lat", DoubleType())
        ])),
        StructField("weather", ArrayType(StructType([
            StructField("id", IntegerType()),
            StructField("main", StringType()),
            StructField("description", StringType()),
            StructField("icon", StringType())
        ]))),
        StructField("base", StringType()),
        StructField("main", StructType([
            StructField("temp", DoubleType()),
            StructField("feels_like", DoubleType()),
            StructField("temp_min", DoubleType()),
            StructField("temp_max", DoubleType()),
            StructField("pressure", IntegerType()),
            StructField("humidity", IntegerType()),
            StructField("sea_level", IntegerType()),
            StructField("grnd_level", IntegerType())
        ])),
        StructField("visibility", IntegerType()),
        StructField("wind", StructType([
            StructField("speed", DoubleType()),
            StructField("deg", IntegerType()),
            StructField("gust", DoubleType())
        ])),
        StructField("clouds", StructType([
            StructField("all", IntegerType())
        ])),
        StructField("dt", LongType()),
        StructField("sys", StructType([
            StructField("country", StringType()),
            StructField("sunrise", LongType()),
            StructField("sunset", LongType())
        ])),
        StructField("timezone", IntegerType()),
        StructField("id", LongType()),
        StructField("name", StringType()),
        StructField("cod", IntegerType())
    ])


# Weather Stream Parser
def readWheather(schema, bronze_table):
    bronze_df = spark.readStream.table(bronze_table)

    df1 = bronze_df.filter(col("value").contains('"coord"')) \
        .withColumn("json", from_json(col("value"), schema)) \
        .select(
            col("json.name").alias("city_name"),
            col("json.id").alias("city_id"),
            col("json.sys.country").alias("country"),
            col("json.coord.lat").alias("latitude"),
            col("json.coord.lon").alias("longitude"),
            col("json.main.temp").alias("temperature"),
            col("json.main.feels_like").alias("feels_like"),
            col("json.main.temp_min").alias("temp_min"),
            col("json.main.temp_max").alias("temp_max"),
            col("json.main.pressure").alias("pressure"),
            col("json.main.humidity").alias("humidity"),
            col("json.main.sea_level").alias("sea_level"),
            col("json.main.grnd_level").alias("grnd_level"),
            col("json.weather")[0]["main"].alias("weather_main"),
            col("json.weather")[0]["description"].alias("weather_description"),
            col("json.weather")[0]["icon"].alias("weather_icon"),
            col("json.visibility").alias("visibility"),
            col("json.wind.speed").alias("wind_speed"),
            col("json.wind.deg").alias("wind_deg"),
            col("json.wind.gust").alias("wind_gust"),
            col("json.clouds.all").alias("cloud_coverage"),
            col("json.dt").alias("timestamp_epoch"),
            col("json.timezone").alias("timezone_offset"),
            col("json.sys.sunrise").alias("sunrise_epoch"),
            col("json.sys.sunset").alias("sunset_epoch"),
            current_timestamp().alias("insert_timestamp")
        ).dropDuplicates()

    return df1

# Main Process Function
def process():
    print(f"\nStarting Silver Stream...", end="")
    schema = getSchema1()
    bronze_table = "db_projects.dev.wheather_bz"

    silver_df = readWheather(schema, bronze_table)

    sQuery = (
        silver_df.writeStream.queryName("silver-ingestion")
        .option("checkpointLocation", f"{check_point_dir}/Wheather_silver_checkpoint")
        .outputMode("append")
        .toTable("db_projects.dev.weather_sv")
    )

    print("Done")
    return sQuery


# COMMAND ----------

process()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from db_projects.dev.weather_sv



#