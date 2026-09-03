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

TARGET_TABLE = f"{CATALOG}.gold.product_performance"

print("Environment:", ENV)
print("Catalog:", CATALOG)
print("Orders source:", ORDERS_SOURCE)
print("Products source:", PRODUCTS_SOURCE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

orders_silver_df = spark.table(ORDERS_SOURCE)
products_silver_df = spark.table(PRODUCTS_SOURCE)

print("Orders Silver count:", orders_silver_df.count())
print("Products Silver count:", products_silver_df.count())

print("\nOrders schema:")
orders_silver_df.printSchema()

print("\nProducts schema:")
products_silver_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F
product_orders_df = (
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

display(product_orders_df)

# COMMAND ----------

completed_product_orders_df = (
    product_orders_df
    .filter(F.col("order_status") == "COMPLETED")
)

print(
    "Completed product orders:",
    completed_product_orders_df.count()
)

display(completed_product_orders_df)

# COMMAND ----------

from pyspark.sql.window import Window
product_performance_df = (
    completed_product_orders_df
    .groupBy(
        "product_id",
        "product_name",
        "category"
    )
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.countDistinct("customer_id").alias("unique_customers"),
        F.sum("quantity").alias("units_sold"),
        F.sum("order_amount")
            .cast("decimal(18,2)")
            .alias("total_sales"),
        F.avg("order_amount")
            .cast("decimal(18,2)")
            .alias("average_order_value"),
        F.min("order_timestamp").alias("first_order_timestamp"),
        F.max("order_timestamp").alias("last_order_timestamp")
    )
)

# COMMAND ----------

product_performance_df = (
    product_performance_df
    .withColumn(
        "sales_rank",
        F.dense_rank().over(
            Window.orderBy(F.col("total_sales").desc())
        )
    )
    .select(
        "product_id",
        "product_name",
        "category",
        "total_orders",
        "unique_customers",
        "units_sold",
        "total_sales",
        "average_order_value",
        "sales_rank",
        "first_order_timestamp",
        "last_order_timestamp"
    )
)

# COMMAND ----------

total_rows = product_performance_df.count()

duplicate_products = (
    product_performance_df
    .groupBy("product_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_product_ids = (
    product_performance_df
    .filter(F.col("product_id").isNull())
    .count()
)

negative_sales = (
    product_performance_df
    .filter(F.col("total_sales") < 0)
    .count()
)

invalid_units = (
    product_performance_df
    .filter(F.col("units_sold") <= 0)
    .count()
)

print("Total Gold rows:", total_rows)
print("Duplicate product IDs:", duplicate_products)
print("Null product IDs:", null_product_ids)
print("Negative sales:", negative_sales)
print("Invalid units sold:", invalid_units)

# COMMAND ----------

(
    product_performance_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print(f"Gold table written successfully: {TARGET_TABLE}")