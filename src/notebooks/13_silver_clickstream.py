# Databricks notebook source
# ============================================================
# GlobalMart Retail Lakehouse
# Notebook: 13_silver_clickstream
# Purpose : Clean and standardize clickstream Bronze data
# Source  : retail_dev.bronze.clickstream_events
# Target  : retail_dev.silver.clickstream_events
# ============================================================

from pyspark.sql import functions as F
from pyspark.sql.window import Window


# ------------------------------------------------------------
# 1. Configuration
# ------------------------------------------------------------

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

SOURCE_TABLE = f"{CATALOG}.bronze.clickstream_events"
TARGET_TABLE = f"{CATALOG}.silver.clickstream_events"


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

clickstream_bronze_df = spark.table(SOURCE_TABLE)

display(clickstream_bronze_df)

clickstream_bronze_df.printSchema()

# COMMAND ----------

clickstream_clean_df = (
    clickstream_bronze_df
    .select(
        F.col("event_id")
            .cast("string")
            .alias("event_id"),

        F.col("customer_id")
            .cast("string")
            .alias("customer_id"),

        F.col("product_id")
            .cast("string")
            .alias("product_id"),

        F.lower(
            F.trim(F.col("event_type"))
        ).alias("event_type"),

        F.to_timestamp(
            F.col("event_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("event_timestamp"),

        F.col("_ingested_at"),
        F.col("_source_file")
    )
)

# COMMAND ----------

clickstream_clean_df = (
    clickstream_clean_df

    .withColumn(
        "event_id",
        F.when(
            F.trim(F.col("event_id")) == "",
            None
        ).otherwise(F.col("event_id"))
    )

    .withColumn(
        "customer_id",
        F.when(
            F.trim(F.col("customer_id")) == "",
            None
        ).otherwise(F.col("customer_id"))
    )

    .withColumn(
        "product_id",
        F.when(
            F.trim(F.col("product_id")) == "",
            None
        ).otherwise(F.col("product_id"))
    )

    .withColumn(
        "event_type",
        F.when(
            F.trim(F.col("event_type")) == "",
            None
        ).otherwise(F.col("event_type"))
    )
)

# COMMAND ----------

clickstream_valid_df = (
    clickstream_clean_df
    .filter(F.col("event_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("event_type").isNotNull())
    .filter(F.col("event_timestamp").isNotNull())
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("event_id")
    .orderBy(
        F.col("_ingested_at").desc()
    )
)

clickstream_dedup_df = (
    clickstream_valid_df
    .withColumn(
        "_rn",
        F.row_number().over(window_spec)
    )
    .filter(
        F.col("_rn") == 1
    )
    .drop("_rn")
)

# COMMAND ----------

(
    clickstream_dedup_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)