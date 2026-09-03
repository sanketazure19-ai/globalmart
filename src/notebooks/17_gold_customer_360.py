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

CUSTOMERS_SOURCE = f"{CATALOG}.silver.customers"
ORDERS_SOURCE = f"{CATALOG}.silver.orders"
CLICKSTREAM_SOURCE = f"{CATALOG}.silver.clickstream_events"
RETURNS_SOURCE = f"{CATALOG}.silver.returns"

TARGET_TABLE = f"{CATALOG}.gold.customer_360"

print("Environment:", ENV)
print("Catalog:", CATALOG)
print("Customers:", CUSTOMERS_SOURCE)
print("Orders:", ORDERS_SOURCE)
print("Clickstream:", CLICKSTREAM_SOURCE)
print("Returns:", RETURNS_SOURCE)
print("Target:", TARGET_TABLE)

# COMMAND ----------

customers_df = spark.table(CUSTOMERS_SOURCE)
orders_df = spark.table(ORDERS_SOURCE)
clickstream_df = spark.table(CLICKSTREAM_SOURCE)
returns_df = spark.table(RETURNS_SOURCE)

print("Customers:", customers_df.count())
print("Orders:", orders_df.count())
print("Clickstream events:", clickstream_df.count())
print("Returns:", returns_df.count())

# COMMAND ----------

from pyspark.sql import functions as F
customer_orders_df = (
    orders_df
    .filter(F.col("order_status") == "COMPLETED")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("quantity").alias("total_units_purchased"),
        F.sum("order_amount")
            .cast("decimal(18,2)")
            .alias("total_spend"),
        F.avg("order_amount")
            .cast("decimal(18,2)")
            .alias("average_order_value"),
        F.min("order_timestamp").alias("first_order_timestamp"),
        F.max("order_timestamp").alias("last_order_timestamp")
    )
)

# COMMAND ----------

customer_clickstream_df = (
    clickstream_df
    .groupBy("customer_id")
    .agg(
        F.countDistinct("event_id").alias("total_events"),

        F.sum(
            F.when(
                F.col("event_type") == "view",
                1
            ).otherwise(0)
        ).alias("product_views"),

        F.sum(
            F.when(
                F.col("event_type") == "add_to_cart",
                1
            ).otherwise(0)
        ).alias("add_to_cart_events"),

        F.sum(
            F.when(
                F.col("event_type") == "purchase",
                1
            ).otherwise(0)
        ).alias("clickstream_purchase_events"),

        F.min("event_timestamp").alias("first_event_timestamp"),
        F.max("event_timestamp").alias("last_event_timestamp")
    )
)

# COMMAND ----------

customer_returns_df = (
    returns_df
    .groupBy("customer_id")
    .agg(
        F.countDistinct("return_id").alias("total_returns"),
        F.sum("quantity").alias("total_returned_units")
    )
)

# COMMAND ----------

customer_360_df = (
    customers_df.alias("c")

    .join(
        customer_orders_df.alias("o"),
        F.col("c.customer_id") == F.col("o.customer_id"),
        "left"
    )

    .join(
        customer_clickstream_df.alias("e"),
        F.col("c.customer_id") == F.col("e.customer_id"),
        "left"
    )

    .join(
        customer_returns_df.alias("r"),
        F.col("c.customer_id") == F.col("r.customer_id"),
        "left"
    )

    .select(
        F.col("c.customer_id"),
        F.col("c.first_name"),
        F.col("c.last_name"),
        F.col("c.email"),
        F.col("c.city"),
        F.col("c.state"),

        F.coalesce(F.col("o.total_orders"), F.lit(0))
            .alias("total_orders"),

        F.coalesce(F.col("o.total_units_purchased"), F.lit(0))
            .alias("total_units_purchased"),

        F.coalesce(
            F.col("o.total_spend"),
            F.lit(0).cast("decimal(18,2)")
        ).alias("total_spend"),

        F.coalesce(
            F.col("o.average_order_value"),
            F.lit(0).cast("decimal(18,2)")
        ).alias("average_order_value"),

        F.col("o.first_order_timestamp"),
        F.col("o.last_order_timestamp"),

        F.coalesce(F.col("e.total_events"), F.lit(0))
            .alias("total_events"),

        F.coalesce(F.col("e.product_views"), F.lit(0))
            .alias("product_views"),

        F.coalesce(F.col("e.add_to_cart_events"), F.lit(0))
            .alias("add_to_cart_events"),

        F.coalesce(F.col("e.clickstream_purchase_events"), F.lit(0))
            .alias("clickstream_purchase_events"),

        F.col("e.first_event_timestamp"),
        F.col("e.last_event_timestamp"),

        F.coalesce(F.col("r.total_returns"), F.lit(0))
            .alias("total_returns"),

        F.coalesce(F.col("r.total_returned_units"), F.lit(0))
            .alias("total_returned_units")
    )
)

# COMMAND ----------

customer_360_df = (
    customer_360_df

    .withColumn(
        "return_rate",
        F.when(
            F.col("total_units_purchased") > 0,
            F.round(
                F.col("total_returned_units") /
                F.col("total_units_purchased") * 100,
                2
            )
        ).otherwise(F.lit(0))
    )

    .withColumn(
        "customer_segment",
        F.when(
            F.col("total_spend") >= 1000,
            "VIP"
        )
        .when(
            F.col("total_spend") >= 500,
            "HIGH_VALUE"
        )
        .when(
            F.col("total_spend") > 0,
            "ACTIVE"
        )
        .otherwise(
            "PROSPECT"
        )
    )
)

# COMMAND ----------

total_customers = customer_360_df.count()

duplicate_customers = (
    customer_360_df
    .groupBy("customer_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

null_customer_ids = (
    customer_360_df
    .filter(F.col("customer_id").isNull())
    .count()
)

negative_spend = (
    customer_360_df
    .filter(F.col("total_spend") < 0)
    .count()
)

negative_orders = (
    customer_360_df
    .filter(F.col("total_orders") < 0)
    .count()
)

invalid_return_rate = (
    customer_360_df
    .filter(
        (F.col("return_rate") < 0) |
        (F.col("return_rate") > 100)
    )
    .count()
)

print("Total customers:", total_customers)
print("Duplicate customer IDs:", duplicate_customers)
print("Null customer IDs:", null_customer_ids)
print("Negative spend:", negative_spend)
print("Negative orders:", negative_orders)
print("Invalid return rates:", invalid_return_rate)

# COMMAND ----------

(
    customer_360_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TARGET_TABLE)
)

print("Gold table created:", TARGET_TABLE)