# ============================================
# Velora Analytics — Silver Layer
# Notebook: 02_silver_orders
# Purpose: Clean, standardize and enrich Bronze data into Silver
# Input: Bronze Delta tables
# Output: 5 Silver Delta tables
# ============================================

from pyspark.sql.functions import (
    col, when, lit, current_timestamp, upper,
    count, round as spark_round, row_number, coalesce
)
from pyspark.sql.window import Window

BRONZE_FILES = "abfss://E-commerce@onelake.dfs.fabric.microsoft.com/Velora_Bronze.Lakehouse/Files"

# ── Cell 1: Silver Orders ────────────────────────────────────────
# Read from Bronze CSV (direct file read)
df = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_orders_dataset.csv")

# NULL CHECK before cleaning
print("=== NULL CHECK — ORDERS ===")
for c in df.columns:
    print(f"{c}: {df.filter(col(c).isNull()).count()} nulls")

# Clean and enrich
df_silver = df \
    .withColumn("is_approved",
        when(col("order_approved_at").isNull(), lit(0)).otherwise(lit(1))) \
    .withColumn("is_delivered",
        when(col("order_delivered_customer_date").isNull(), lit(0)).otherwise(lit(1))) \
    .withColumn("is_late_delivery",
        when(
            (col("order_delivered_customer_date").isNotNull()) &
            (col("order_delivered_customer_date") > col("order_estimated_delivery_date")),
            lit(1)
        ).otherwise(lit(0))) \
    .withColumn("delivery_delay_days",
        when(
            col("order_delivered_customer_date").isNotNull(),
            (col("order_delivered_customer_date").cast("long") -
             col("order_estimated_delivery_date").cast("long")) / 86400
        ).otherwise(lit(None))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_layer", lit("bronze"))

df_silver.write.format("delta").mode("overwrite").saveAsTable("silver_orders")
print("silver_orders written:", df_silver.count(), "rows")

# Validation
print("Late deliveries:", df_silver.filter(col("is_late_delivery") == 1).count())
print("Undelivered orders:", df_silver.filter(col("is_delivered") == 0).count())
print("Unapproved orders:", df_silver.filter(col("is_approved") == 0).count())

# ── Cell 2: Silver Order Items ───────────────────────────────────
df_items = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_order_items_dataset.csv")

df_silver_items = df_items \
    .withColumn("total_item_value", spark_round(col("price") + col("freight_value"), 2)) \
    .withColumn("shipping_limit_date", col("shipping_limit_date").cast("timestamp")) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_layer", lit("bronze"))

df_silver_items.write.format("delta").mode("overwrite").saveAsTable("silver_order_items")
print("silver_order_items written:", df_silver_items.count(), "rows")

total_gmv = df_silver_items.selectExpr("round(sum(price), 2) as total_gmv").collect()[0][0]
print("Total GMV (R$):", total_gmv)

# ── Cell 3: Silver Payments ──────────────────────────────────────
df_payments = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_order_payments_dataset.csv")

# Fix: replace "not_defined" payment type with "unknown"
df_silver_payments = df_payments \
    .withColumn("payment_type",
        when(col("payment_type") == "not_defined", lit("unknown"))
        .otherwise(col("payment_type"))) \
    .withColumn("is_installment",
        when(col("payment_installments") > 1, lit(1)).otherwise(lit(0))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_layer", lit("bronze"))

df_silver_payments.write.format("delta").mode("overwrite").saveAsTable("silver_payments")
print("silver_payments written:", df_silver_payments.count(), "rows")
print("Installment orders:", df_silver_payments.filter(col("is_installment") == 1).count())

# ── Cell 4: Silver Customers ─────────────────────────────────────
df_customers = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_customers_dataset.csv")

# Detect repeat customers using customer_unique_id
repeat_ids = df_customers.groupBy("customer_unique_id") \
    .count().filter(col("count") > 1) \
    .select("customer_unique_id").rdd.flatMap(lambda x: x).collect()

df_silver_customers = df_customers \
    .withColumn("customer_state", upper(col("customer_state"))) \
    .withColumn("customer_city",  upper(col("customer_city"))) \
    .withColumn("is_repeat_customer",
        when(col("customer_unique_id").isin(repeat_ids), lit(1)).otherwise(lit(0))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_layer", lit("bronze"))

df_silver_customers.write.format("delta").mode("overwrite").saveAsTable("silver_customers")
print("silver_customers written:", df_silver_customers.count(), "rows")
print("Repeat customers:", df_silver_customers.filter(col("is_repeat_customer") == 1).count())

# ── Cell 5: Silver Products ──────────────────────────────────────
df_products = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_products_dataset.csv")
df_category = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/product_category_name_translation.csv")

# Join with English category translation
df_silver_products = df_products \
    .join(df_category, on="product_category_name", how="left") \
    .withColumn("product_category_english",
        when(col("product_category_name_english").isNull(), lit("unknown"))
        .otherwise(col("product_category_name_english"))) \
    .withColumn("product_category_name",
        when(col("product_category_name").isNull(), lit("unknown"))
        .otherwise(col("product_category_name"))) \
    .withColumn("product_weight_g",   coalesce(col("product_weight_g"),   lit(0))) \
    .withColumn("product_length_cm",  coalesce(col("product_length_cm"),  lit(0))) \
    .withColumn("product_height_cm",  coalesce(col("product_height_cm"),  lit(0))) \
    .withColumn("product_width_cm",   coalesce(col("product_width_cm"),   lit(0))) \
    .withColumn("ingested_at", current_timestamp()) \
    .withColumn("source_layer", lit("bronze")) \
    .drop("product_category_name_english")

df_silver_products.write.format("delta").mode("overwrite").saveAsTable("silver_products")
print("silver_products written:", df_silver_products.count(), "rows")
print("Unknown category:", df_silver_products.filter(col("product_category_english") == "unknown").count())

# ── Cell 6: Silver Layer Summary ─────────────────────────────────
print("=== SILVER LAYER COMPLETE ===")
for t in ["silver_orders", "silver_order_items", "silver_payments",
          "silver_customers", "silver_products"]:
    print(f"{t}: {spark.table(t).count()} rows")
