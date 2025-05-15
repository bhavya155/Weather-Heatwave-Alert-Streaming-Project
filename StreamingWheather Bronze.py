# Databricks notebook source
check_point_dir="/mnt/devadlsprojects/checkpoint"

# COMMAND ----------

def read_kafka_topic():
    from pyspark.sql.functions import col, cast
    TOPIC = "wheather"
    event_hub_ns="ehnamespacewheather"
    connection_string="""Endpoint=sb://ehnamespacewheather.servicebus.windows.net/;SharedAccessKeyName=databricks;SharedAccessKey=SNlBC58bCdkq/URWW9aClZAmY2vgSuVBC+AEhC9v3ZE=;EntityPath=wheather"""
    BOOTSTRAP_SERVERS = f"{event_hub_ns}.servicebus.windows.net:9093"
    EH_SASL = f"kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"$ConnectionString\" password=\"{connection_string};\";"


    df = spark.readStream \
        .format("kafka") \
        .option("subscribe", TOPIC) \
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS) \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.jaas.config", EH_SASL) \
        .option("kafka.request.timeout.ms", "60000") \
        .option("kafka.session.timeout.ms", "60000") \
        .option("failOnDataLoss", "false") \
        .option("startingOffsets", "latest") \
        .load()
    return df.select(col('value').cast('string'))

def process():
    print(f"\nStarting Bronze Stream...", end="")

    Bronze_df =read_kafka_topic()
    sQuery = (
        Bronze_df.writeStream.queryName("Bronze-ingestion")
        .option("checkpointLocation", f"{check_point_dir}/WheatherCheckpoint")
        .outputMode("append")
        .toTable("db_projects.dev.wheather_bz")
            )

    print("Done")
    return sQuery

# COMMAND ----------

process()
