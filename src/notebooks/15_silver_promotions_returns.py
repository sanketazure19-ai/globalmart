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

PROMOTIONS_SOURCE = f"{CATALOG}.bronze.promotions"
PROMOTIONS_TARGET = f"{CATALOG}.silver.promotions"

RETURNS_SOURCE = f"{CATALOG}.bronze.returns"
RETURNS_TARGET = f"{CATALOG}.silver.returns"

print("Environment:", ENV)
print("Catalog:", CATALOG)
print("Promotions source:", PROMOTIONS_SOURCE)
print("Promotions target:", PROMOTIONS_TARGET)
print("Returns source:", RETURNS_SOURCE)
print("Returns target:", RETURNS_TARGET)

# COMMAND ----------

promotions_bronze_df = spark.table(PROMOTIONS_SOURCE)
returns_bronze_df = spark.table(RETURNS_SOURCE)

print("Promotions Bronze count:", promotions_bronze_df.count())
print("Returns Bronze count:", returns_bronze_df.count())

print("\nPromotions schema:")
promotions_bronze_df.printSchema()

print("\nReturns schema:")
returns_bronze_df.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

promotions_silver_df = (
    promotions_bronze_df

    .withColumn(
        "promotion_id",
        F.trim(F.col("promotion_id"))
    )
    .withColumn(
        "product_id",
        F.trim(F.col("product_id"))
    )
    .withColumn(
        "promotion_name",
        F.trim(F.col("promotion_name"))
    )
    .withColumn(
        "discount_percent",
        F.col("discount_percent").cast("decimal(5,2)")
    )
    .withColumn(
        "start_date",
        F.col("start_date").cast("date")
    )
    .withColumn(
        "end_date",
        F.col("end_date").cast("date")
    )

    # Required-field validation
    .filter(F.col("promotion_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("promotion_name").isNotNull())
    .filter(F.col("discount_percent").isNotNull())

    # Business validation
    .filter(F.col("discount_percent") >= 0)
    .filter(F.col("discount_percent") <= 100)

    # Date validation
    .filter(F.col("start_date").isNotNull())
    .filter(F.col("end_date").isNotNull())
    .filter(F.col("end_date") >= F.col("start_date"))

    .select(
        "promotion_id",
        "product_id",
        "promotion_name",
        "discount_percent",
        "start_date",
        "end_date",
        "_ingested_at",
        "_source_file"
    )
)

# COMMAND ----------

promotions_total = promotions_silver_df.count()

promotions_duplicate_ids = (
    promotions_silver_df
    .groupBy("promotion_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

promotions_null_ids = (
    promotions_silver_df
    .filter(F.col("promotion_id").isNull())
    .count()
)

promotions_invalid_discount = (
    promotions_silver_df
    .filter(
        (F.col("discount_percent") < 0) |
        (F.col("discount_percent") > 100)
    )
    .count()
)

promotions_invalid_dates = (
    promotions_silver_df
    .filter(F.col("end_date") < F.col("start_date"))
    .count()
)

print("Promotions total:", promotions_total)
print("Duplicate promotion IDs:", promotions_duplicate_ids)
print("Null promotion IDs:", promotions_null_ids)
print("Invalid discounts:", promotions_invalid_discount)
print("Invalid date ranges:", promotions_invalid_dates)

# COMMAND ----------

(
    promotions_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(PROMOTIONS_TARGET)
)

print("Promotions Silver table created:", PROMOTIONS_TARGET)

# COMMAND ----------

returns_silver_df = (
    returns_bronze_df

    .withColumn(
        "return_id",
        F.trim(F.col("return_id"))
    )
    .withColumn(
        "order_id",
        F.trim(F.col("order_id"))
    )
    .withColumn(
        "customer_id",
        F.trim(F.col("customer_id"))
    )
    .withColumn(
        "product_id",
        F.trim(F.col("product_id"))
    )
    .withColumn(
        "quantity",
        F.col("quantity").cast("int")
    )
    .withColumn(
        "return_reason",
        F.trim(F.col("return_reason"))
    )
    .withColumn(
        "return_timestamp",
        F.to_timestamp(F.col("return_timestamp"))
    )
    .withColumn(
        "return_date",
        F.to_date(F.col("return_timestamp"))
    )

    # Required-field validation
    .filter(F.col("return_id").isNotNull())
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("product_id").isNotNull())
    .filter(F.col("quantity").isNotNull())
    .filter(F.col("return_timestamp").isNotNull())

    # Business validation
    .filter(F.col("quantity") > 0)

    .select(
        "return_id",
        "order_id",
        "customer_id",
        "product_id",
        "quantity",
        "return_reason",
        "return_timestamp",
        "return_date",
        "_ingested_at",
        "_source_file"
    )
)

# COMMAND ----------

returns_total = returns_silver_df.count()

returns_duplicate_ids = (
    returns_silver_df
    .groupBy("return_id")
    .count()
    .filter(F.col("count") > 1)
    .count()
)

returns_null_ids = (
    returns_silver_df
    .filter(F.col("return_id").isNull())
    .count()
)

returns_invalid_quantity = (
    returns_silver_df
    .filter(F.col("quantity") <= 0)
    .count()
)

returns_null_timestamp = (
    returns_silver_df
    .filter(F.col("return_timestamp").isNull())
    .count()
)

print("Returns total:", returns_total)
print("Duplicate return IDs:", returns_duplicate_ids)
print("Null return IDs:", returns_null_ids)
print("Invalid quantities:", returns_invalid_quantity)
print("Null timestamps:", returns_null_timestamp)

# COMMAND ----------

(
    returns_silver_df
    .write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(RETURNS_TARGET)
)

print("Returns Silver table created:", RETURNS_TARGET)

# COMMAND ----------

promotions_silver_table = spark.table(PROMOTIONS_TARGET)
returns_silver_table = spark.table(RETURNS_TARGET)

print("Promotions Silver count:", promotions_silver_table.count())
print("Returns Silver count:", returns_silver_table.count())

print("\nPromotions Silver:")
display(
    promotions_silver_table
    .orderBy("promotion_id")
)

print("\nReturns Silver:")
display(
    returns_silver_table
    .orderBy("return_id")
)