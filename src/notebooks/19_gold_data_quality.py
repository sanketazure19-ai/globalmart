# Databricks notebook source
from pyspark.sql import functions as F

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

SALES_SUMMARY_TABLE = f"{CATALOG}.gold.sales_summary"
CUSTOMER_360_TABLE = f"{CATALOG}.gold.customer_360"
PRODUCT_PERFORMANCE_TABLE = f"{CATALOG}.gold.product_performance"

print("Environment:", ENV)
print("Catalog:", CATALOG)
print("Sales Summary:", SALES_SUMMARY_TABLE)
print("Customer 360:", CUSTOMER_360_TABLE)
print("Product Performance:", PRODUCT_PERFORMANCE_TABLE)

# COMMAND ----------

sales_summary_df = spark.table(SALES_SUMMARY_TABLE)
customer_360_df = spark.table(CUSTOMER_360_TABLE)
product_performance_df = spark.table(PRODUCT_PERFORMANCE_TABLE)

print("Sales Summary rows:", sales_summary_df.count())
print("Customer 360 rows:", customer_360_df.count())
print("Product Performance rows:", product_performance_df.count())

# COMMAND ----------

sales_summary_checks = {
    "null_order_dates": sales_summary_df
        .filter(F.col("order_date").isNull())
        .count(),

    "null_categories": sales_summary_df
        .filter(F.col("category").isNull())
        .count(),

    "negative_total_sales": sales_summary_df
        .filter(F.col("total_sales") < 0)
        .count(),

    "invalid_units_sold": sales_summary_df
        .filter(F.col("units_sold") < 0)
        .count(),

    "invalid_total_orders": sales_summary_df
        .filter(F.col("total_orders") < 0)
        .count()
}

print("Sales Summary checks:")

for check, value in sales_summary_checks.items():
    print(f"{check}: {value}")

# COMMAND ----------

customer_360_checks = {
    "null_customer_ids": customer_360_df
        .filter(F.col("customer_id").isNull())
        .count(),

    "duplicate_customer_ids": customer_360_df
        .groupBy("customer_id")
        .count()
        .filter(F.col("count") > 1)
        .count(),

    "negative_total_spend": customer_360_df
        .filter(F.col("total_spend") < 0)
        .count(),

    "negative_total_orders": customer_360_df
        .filter(F.col("total_orders") < 0)
        .count(),

    "negative_return_rate": customer_360_df
        .filter(F.col("return_rate") < 0)
        .count(),

    "return_rate_over_100": customer_360_df
        .filter(F.col("return_rate") > 100)
        .count()
}

print("Customer 360 checks:")

for check, value in customer_360_checks.items():
    print(f"{check}: {value}")

# COMMAND ----------

product_performance_checks = {
    "null_product_ids": product_performance_df
        .filter(F.col("product_id").isNull())
        .count(),

    "duplicate_product_ids": product_performance_df
        .groupBy("product_id")
        .count()
        .filter(F.col("count") > 1)
        .count(),

    "negative_total_sales": product_performance_df
        .filter(F.col("total_sales") < 0)
        .count(),

    "invalid_units_sold": product_performance_df
        .filter(F.col("units_sold") <= 0)
        .count(),

    "invalid_total_orders": product_performance_df
        .filter(F.col("total_orders") <= 0)
        .count()
}

print("Product Performance checks:")

for check, value in product_performance_checks.items():
    print(f"{check}: {value}")

# COMMAND ----------

customers_silver_df = spark.table(
    f"{CATALOG}.silver.customers"
)

products_silver_df = spark.table(
    f"{CATALOG}.silver.products"
)

missing_customer_keys = (
    customer_360_df
    .select("customer_id")
    .distinct()
    .join(
        customers_silver_df
        .select("customer_id")
        .distinct(),
        "customer_id",
        "left_anti"
    )
    .count()
)

missing_product_keys = (
    product_performance_df
    .select("product_id")
    .distinct()
    .join(
        products_silver_df
        .select("product_id")
        .distinct(),
        "product_id",
        "left_anti"
    )
    .count()
)

print("Customer IDs missing from Silver customers:", missing_customer_keys)
print("Product IDs missing from Silver products:", missing_product_keys)

# COMMAND ----------

dq_results = [
    ("sales_summary", "null_order_dates", sales_summary_checks["null_order_dates"]),
    ("sales_summary", "null_categories", sales_summary_checks["null_categories"]),
    ("sales_summary", "negative_total_sales", sales_summary_checks["negative_total_sales"]),
    ("sales_summary", "invalid_units_sold", sales_summary_checks["invalid_units_sold"]),
    ("sales_summary", "invalid_total_orders", sales_summary_checks["invalid_total_orders"]),

    ("customer_360", "null_customer_ids", customer_360_checks["null_customer_ids"]),
    ("customer_360", "duplicate_customer_ids", customer_360_checks["duplicate_customer_ids"]),
    ("customer_360", "negative_total_spend", customer_360_checks["negative_total_spend"]),
    ("customer_360", "negative_total_orders", customer_360_checks["negative_total_orders"]),
    ("customer_360", "negative_return_rate", customer_360_checks["negative_return_rate"]),
    ("customer_360", "return_rate_over_100", customer_360_checks["return_rate_over_100"]),

    ("product_performance", "null_product_ids", product_performance_checks["null_product_ids"]),
    ("product_performance", "duplicate_product_ids", product_performance_checks["duplicate_product_ids"]),
    ("product_performance", "negative_total_sales", product_performance_checks["negative_total_sales"]),
    ("product_performance", "invalid_units_sold", product_performance_checks["invalid_units_sold"]),
    ("product_performance", "invalid_total_orders", product_performance_checks["invalid_total_orders"]),

    ("referential_integrity", "missing_customer_keys", missing_customer_keys),
    ("referential_integrity", "missing_product_keys", missing_product_keys)
]

dq_df = spark.createDataFrame(
    dq_results,
    ["table_name", "check_name", "failed_records"]
)

dq_df = dq_df.withColumn(
    "status",
    F.when(F.col("failed_records") == 0, "PASS")
     .otherwise("FAIL")
)

display(dq_df)

# COMMAND ----------

failed_checks = (
    dq_df
    .filter(F.col("status") == "FAIL")
    .count()
)

total_checks = dq_df.count()

if failed_checks == 0:
    overall_status = "PASS"
else:
    overall_status = "FAIL"

print("Total checks:", total_checks)
print("Failed checks:", failed_checks)
print("Overall DQ status:", overall_status)

if overall_status == "FAIL":
    raise Exception(
        f"Gold data quality validation failed. "
        f"{failed_checks} check(s) failed."
    )

# COMMAND ----------

print("=" * 60)
print("GLOBALMART GOLD DATA QUALITY SUMMARY")
print("=" * 60)

print("Environment:", ENV)
print("Catalog:", CATALOG)

print("\nGold Tables:")
print(f"  Sales Summary        : {sales_summary_df.count()} rows")
print(f"  Customer 360         : {customer_360_df.count()} rows")
print(f"  Product Performance  : {product_performance_df.count()} rows")

print("\nData Quality:")
print(f"  Total checks         : {total_checks}")
print(f"  Failed checks        : {failed_checks}")
print(f"  Overall status       : {overall_status}")

print("=" * 60)