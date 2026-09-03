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

SOURCE_TABLE = f"{CATALOG}.bronze.orders"
TARGET_TABLE = f"{CATALOG}.silver.orders"

orders_bronze_df = spark.table(SOURCE_TABLE)

display(orders_bronze_df)

# COMMAND ----------

orders_clean_df = (
    orders_bronze_df
    .select(
        F.col("order_id")
            .cast("string")
            .alias("order_id"),

        F.col("customer_id")
            .cast("string")
            .alias("customer_id"),

        F.col("product_id")
            .cast("string")
            .alias("product_id"),

        F.col("quantity")
            .cast("int")
            .alias("quantity"),

        F.col("order_amount")
            .cast("decimal(18,2)")
            .alias("order_amount"),

        F.to_timestamp(
            F.col("order_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        ).alias("order_timestamp"),

        F.upper(
            F.trim(F.col("order_status"))
        ).alias("order_status"),

        F.col("_ingested_at"),
        F.col("_source_file")
    )
    .withColumn(
        "order_date",
        F.to_date(F.col("order_timestamp"))
    )
)

# COMMAND ----------

display(orders_clean_df)

# COMMAND ----------

orders_valid_df = (
    orders_clean_df
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("order_timestamp").isNotNull())
    .filter(F.col("order_status").isNotNull())
)

# COMMAND ----------

orders_valid_df = (
    orders_valid_df
    .filter(F.col("quantity") > 0)
    .filter(F.col("order_amount") > 0)
)

# COMMAND ----------

orders_valid_df = (
    orders_valid_df
    .filter(
        F.col("order_status").isin(
            "COMPLETED",
            "CANCELLED"
        )
    )
)

# COMMAND ----------

window_spec = (
    Window
    .partitionBy("order_id")
    .orderBy(
        F.col("_ingested_at").desc()
    )
)

orders_dedup_df = (
    orders_valid_df
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
    orders_dedup_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

# COMMAND ----------

print(
    f"Source table : {SOURCE_TABLE}"
)

print(
    f"Target table : {TARGET_TABLE}"
)

print(
    "Silver order count:",
    spark.table(TARGET_TABLE).count()
)

display(
    spark.table(TARGET_TABLE)
)