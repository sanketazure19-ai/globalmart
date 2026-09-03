# Databricks notebook source
# ============================================================
# GlobalMart Retail Lakehouse
# Notebook: 11_silver_products
# Purpose : Clean and standardize product Bronze data
# Source  : retail_dev.bronze.products
# Target  : retail_dev.silver.products
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

SOURCE_TABLE = f"{CATALOG}.bronze.products"
TARGET_TABLE = f"{CATALOG}.silver.products"


# ------------------------------------------------------------
# 2. Read Bronze
# ------------------------------------------------------------

products_bronze_df = spark.table(SOURCE_TABLE)

display(products_bronze_df)

# COMMAND ----------

products_clean_df = (
    products_bronze_df
    .select(
        F.col("product_id")
            .cast("string")
            .alias("product_id"),

        F.trim(F.col("product_name"))
            .alias("product_name"),

        F.trim(F.col("category"))
            .alias("category"),

        F.col("price")
            .cast("decimal(18,2)")
            .alias("price"),

        F.col("_ingested_at")
    )
)

# COMMAND ----------

products_clean_df = (
    products_clean_df

    .withColumn(
        "product_name",
        F.when(
            F.trim(F.col("product_name")) == "",
            None
        ).otherwise(F.col("product_name"))
    )

    .withColumn(
        "category",
        F.when(
            F.trim(F.col("category")) == "",
            None
        ).otherwise(F.col("category"))
    )
)

# COMMAND ----------

products_valid_df = (
    products_clean_df
    .filter(
        F.col("product_id").isNotNull()
    )
    .filter(
        F.col("product_name").isNotNull()
    )
    .filter(
        F.col("category").isNotNull()
    )
    .filter(
        F.col("price").isNotNull()
    )
    .filter(
        F.col("price") > 0
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("product_id")
    .orderBy(
        F.col("_ingested_at").desc()
    )
)

products_dedup_df = (
    products_valid_df
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
    products_dedup_df.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

invalid_products = spark.sql("""
    SELECT *
    FROM retail_dev.silver.products
    WHERE product_id IS NULL
       OR product_name IS NULL
       OR category IS NULL
       OR price IS NULL
       OR price <= 0
""")

print(
    "Invalid Silver products:",
    invalid_products.count()
)

display(invalid_products)