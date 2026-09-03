# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.window import Window

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

SOURCE_TABLE = f"{CATALOG}.bronze.customers"
TARGET_TABLE = f"{CATALOG}.silver.customers"

print("Environment:", ENV)
print("Catalog:", CATALOG)

customers_bronze_df = spark.table(SOURCE_TABLE)

display(customers_bronze_df)

# COMMAND ----------

customers_clean_df = (
    customers_bronze_df
    .select(
        F.col("customer_id").cast("string").alias("customer_id"),
        F.trim(F.col("first_name")).alias("first_name"),
        F.trim(F.col("last_name")).alias("last_name"),
        F.lower(F.trim(F.col("email"))).alias("email"),
        F.trim(F.col("city")).alias("city"),
        F.trim(F.col("state")).alias("state"),
        F.col("_ingested_at"),
        F.col("_source_file")
    )
)

# COMMAND ----------

customers_clean_df = (
    customers_clean_df
    .withColumn(
        "first_name",
        F.when(
            F.trim(F.col("first_name")) == "",
            None
        ).otherwise(F.col("first_name"))
    )
    .withColumn(
        "last_name",
        F.when(
            F.trim(F.col("last_name")) == "",
            None
        ).otherwise(F.col("last_name"))
    )
    .withColumn(
        "email",
        F.when(
            F.trim(F.col("email")) == "",
            None
        ).otherwise(F.col("email"))
    )
    .withColumn(
        "city",
        F.when(
            F.trim(F.col("city")) == "",
            None
        ).otherwise(F.col("city"))
    )
    .withColumn(
        "state",
        F.when(
            F.trim(F.col("state")) == "",
            None
        ).otherwise(F.col("state"))
    )
)

# COMMAND ----------

customers_valid_df = (
    customers_clean_df
    .filter(
        F.col("customer_id").isNotNull()
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("customer_id")
    .orderBy(F.col("_ingested_at").desc())
)

customers_dedup_df = (
    customers_valid_df
    .withColumn(
        "_rn",
        F.row_number().over(window_spec)
    )
    .filter(F.col("_rn") == 1)
    .drop("_rn")
)

# COMMAND ----------

(
    customers_dedup_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)