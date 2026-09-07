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
    f"{ENV}/clickstream"
)

TARGET_TABLE = f"{CATALOG}.bronze.clickstream_events"

CHECKPOINT_PATH = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/clickstream"
)

SCHEMA_PATH = (
    "abfss://checkpoints@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/schema/clickstream"
)

print(f"Source     : {SOURCE_PATH}")
print(f"Target     : {TARGET_TABLE}")
print(f"Checkpoint : {CHECKPOINT_PATH}")
print(f"Schema     : {SCHEMA_PATH}")

# COMMAND ----------

clickstream_stream_df = (
    spark.readStream
         .format("cloudFiles")
         .option("cloudFiles.format", "json")
         .option("cloudFiles.schemaLocation", SCHEMA_PATH)
         .load(SOURCE_PATH)
)

# COMMAND ----------

clickstream_bronze_df = (
    clickstream_stream_df
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path"))
)

# COMMAND ----------

clickstream_query = (
    clickstream_bronze_df.writeStream
        .format("delta")
        .outputMode("append")
        .option("checkpointLocation", CHECKPOINT_PATH)
        .trigger(availableNow=True)
        .toTable(TARGET_TABLE)
)

clickstream_query.awaitTermination()

print("Clickstream ingestion completed.")