# Databricks notebook source
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

ORDERS_SOURCE = f"{CATALOG}.silver.orders"
PRODUCTS_SOURCE = f"{CATALOG}.silver.products"
TARGET_TABLE = f"{CATALOG}.gold.sales_summary"

print("Environment:", ENV)
print("Catalog:", CATALOG)
print("Orders source:", ORDERS_SOURCE)
print("Products source:", PRODUCTS_SOURCE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

orders_silver_df = spark.table(ORDERS_SOURCE)
products_silver_df = spark.table(PRODUCTS_SOURCE)

print("Silver Orders count:", orders_silver_df.count())
print("Silver Products count:", products_silver_df.count())

print("\nOrders schema:")
orders_silver_df.printSchema()

print("\nProducts schema:")
products_silver_df.printSchema()

# COMMAND ----------

print("ORDERS_SOURCE:", ORDERS_SOURCE)

orders_silver_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
sales_enriched_df = (
    orders_silver_df.alias("o")
    .join(
        products_silver_df.alias("p"),
        F.col("o.product_id") == F.col("p.product_id"),
        "left"
    )
    .select(
        F.col("o.order_id"),
        F.col("o.customer_id"),
        F.col("o.product_id"),
        F.col("p.product_name"),
        F.col("p.category"),
        F.col("o.quantity"),
        F.col("o.order_amount"),
        F.col("o.order_timestamp"),
        F.col("o.order_date"),
        F.col("o.order_status")
    )
)

# COMMAND ----------

completed_sales_df = (
    sales_enriched_df
    .filter(F.col("order_status") == "COMPLETED")
)

# COMMAND ----------

sales_summary_df = (
    completed_sales_df
    .groupBy(
        "order_date",
        "category"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold"),
        F.sum("order_amount").cast("decimal(18,2)").alias("total_sales"),
        F.avg("order_amount").cast("decimal(18,2)").alias("average_order_value")
    )
    .select(
        "order_date",
        "category",
        "total_orders",
        "unique_customers",
        "units_sold",
        "total_sales",
        "average_order_value"
    )
    .orderBy(
        "order_date",
        "category"
    )
)

# COMMAND ----------

total_rows = sales_summary_df.count()

null_dates = (
    sales_summary_df
    .filter(F.col("order_date").isNull())
    .count()
)

null_categories = (
    sales_summary_df
    .filter(F.col("category").isNull())
    .count()
)

invalid_sales = (
    sales_summary_df
    .filter(F.col("total_sales") < 0)
    .count()
)

invalid_units = (
    sales_summary_df
    .filter(F.col("units_sold") <= 0)
    .count()
)

print("Gold summary rows:", total_rows)
print("Null order dates:", null_dates)
print("Null categories:", null_categories)
print("Negative sales:", invalid_sales)
print("Invalid units:", invalid_units)

# COMMAND ----------

(
    sales_summary_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print("Gold table created:", TARGET_TABLE)