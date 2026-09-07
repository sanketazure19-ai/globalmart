# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# Source: Unity Catalog external volume
SOURCE_PATH = (
    "abfss://raw@"
    "dataloadadlsproject.dfs.core.windows.net/"
    f"{ENV}/customers"
)

# Target: Unity Catalog Bronze table
TARGET_TABLE = f"{CATALOG}.bronze.customers"

print(f"Source : {SOURCE_PATH}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

from pyspark.sql import functions as F
customers_df = (
    spark.read
         .format("csv")
         .option("header", "true")
         .option("inferSchema", "true")
         .load(SOURCE_PATH)
         .select(
             "*",
             F.col("_metadata.file_path").alias("_source_file")
         )
)

# COMMAND ----------

customers_bronze_df = (
    customers_df
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

(
    customers_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

print(f"Source path : {SOURCE_PATH}")
print(f"Target table: {TARGET_TABLE}")

print(
    f"Record count: "
    f"{spark.table(TARGET_TABLE).count()}"
)

display(
    spark.table(TARGET_TABLE)
)

# COMMAND ----------

spark.sql("""
    SHOW TABLES IN retail_dev.bronze
""").show(truncate=False)