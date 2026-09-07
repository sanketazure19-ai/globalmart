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

BASE_PATH = (
    "abfss://raw@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}"
)

PROMOTIONS_SOURCE = f"{BASE_PATH}/promotions"
RETURNS_SOURCE = f"{BASE_PATH}/returns"

PROMOTIONS_TABLE = f"{CATALOG}.bronze.promotions"
RETURNS_TABLE = f"{CATALOG}.bronze.returns"

PROMOTIONS_CHECKPOINT = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/promotions"
)

PROMOTIONS_SCHEMA = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/schema/promotions"
)

RETURNS_CHECKPOINT = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/returns"
)

RETURNS_SCHEMA = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/schema/returns"
)

# COMMAND ----------

promotions_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaLocation", PROMOTIONS_SCHEMA)
        .option("header", "true")
        .load(PROMOTIONS_SOURCE)
)

# COMMAND ----------

promotions_bronze_df = (
    promotions_stream_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

promotions_query = (
    promotions_bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            PROMOTIONS_CHECKPOINT
        )
        .trigger(availableNow=True)
        .toTable(PROMOTIONS_TABLE)
)

promotions_query.awaitTermination()

print("Promotions ingestion completed.")

# COMMAND ----------

returns_stream_df = (
    spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", RETURNS_SCHEMA)
        .load(RETURNS_SOURCE)
)

# COMMAND ----------

returns_bronze_df = (
    returns_stream_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

returns_query = (
    returns_bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option(
            "checkpointLocation",
            RETURNS_CHECKPOINT
        )
        .trigger(availableNow=True)
        .toTable(RETURNS_TABLE)
)

returns_query.awaitTermination()

print("Returns ingestion completed.")