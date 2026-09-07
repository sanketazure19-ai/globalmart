# Databricks notebook source
# ============================================================
# Environment Configuration
# ============================================================

dbutils.widgets.dropdown(
    "env",
    "dev",
    ["dev", "staging", "prod"]
)

ENV = dbutils.widgets.get("env")

CATALOG_MAP = {
    "dev": "retail_dev",
    "staging": "retail_staging",
    "prod": "retail_prod"
}

CATALOG = CATALOG_MAP[ENV]

print("Environment:", ENV)
print("Catalog:", CATALOG)

# COMMAND ----------

from pyspark.sql import functions as F

SOURCE_PATH = (
    "abfss://raw@"
    "dataloadadlsproject.dfs.core.windows.net/"
    "dev/orders"
)

SCHEMA_PATH = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    "dev/schema/orders"
)

CHECKPOINT_PATH = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    "dev/orders"
)

TARGET_TABLE = "retail_dev.bronze.orders"

orders_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.schemaLocation", SCHEMA_PATH)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("header", "true")
        .load(SOURCE_PATH)
)

orders_bronze_df = (
    orders_stream_df
        .withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.col("_metadata.file_path"))
)

orders_query = (
    orders_bronze_df
        .writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
)

orders_query.awaitTermination()

print("Count:", spark.table(TARGET_TABLE).count())
display(spark.table(TARGET_TABLE))

# COMMAND ----------

display(spark.table("retail_dev.bronze.orders"))

# COMMAND ----------

spark.table("retail_dev.bronze.orders").printSchema()