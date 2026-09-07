# Databricks notebook source
SOURCE_PATH = "/Volumes/{CATALOG}/landing/raw_files/products"

TARGET_TABLE = f"{CATALOG}.bronze.products"

print(f"Source : {SOURCE_PATH}")
print(f"Target : {TARGET_TABLE}")

# COMMAND ----------

products_df = (
    spark.read
         .format("json")
         .option("inferSchema", "true")
         .load(SOURCE_PATH)
)

# COMMAND ----------

from pyspark.sql import functions as F

products_bronze_df = (
    products_df
    .withColumn("_ingested_at", F.current_timestamp())
)

# COMMAND ----------

(
    products_bronze_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

print(
    f"Product count: "
    f"{spark.table(TARGET_TABLE).count()}"
)