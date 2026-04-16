# ============================================
# Velora Analytics — Gold Layer
# Notebook: 03_gold_build
# Purpose: Build star schema fact and dimension tables
# Input: Silver Delta tables
# Output: 4 Dimension tables + 3 Fact tables
# ============================================

from pyspark.sql.functions import (
    col, date_format, current_timestamp, lit,
    round as spark_round, explode, sequence,
    to_date, year, month, dayofmonth, quarter,
    dayofweek, weekofyear, row_number
)
from pyspark.sql.window import Window
import pyspark.sql.functions as F

SILVER = "abfss://E-commerce@onelake.dfs.fabric.microsoft.com/Velora_Silver.Lakehouse/Tables/dbo"
BRONZE = "abfss://E-commerce@onelake.dfs.fabric.microsoft.com/Velora_Bronze.Lakehouse/Files"

# ── Cell 1: Load All Silver Tables ───────────────────────────────
df_orders    = spark.read.format("delta").load(f"{SILVER}/silver_orders")
df_items     = spark.read.format("delta").load(f"{SILVER}/silver_order_items")
df_payments  = spark.read.format("delta").load(f"{SILVER}/silver_payments")
df_customers = spark.read.format("delta").load(f"{SILVER}/silver_customers")
df_products  = spark.read.format("delta").load(f"{SILVER}/silver_products")

print("All Silver tables loaded successfully")

# ── Cell 2: dim_date ─────────────────────────────────────────────
# Full date spine covering entire dataset range 2016-2019
df_date = spark.sql("""
    SELECT explode(sequence(
        to_date('2016-01-01'),
        to_date('2019-12-31'),
        interval 1 day
    )) as full_date
""")

df_dim_date = df_date \
    .withColumn("date_id",      date_format(col("full_date"), "yyyyMMdd").cast("int")) \
    .withColumn("day",          dayofmonth(col("full_date"))) \
    .withColumn("month",        month(col("full_date"))) \
    .withColumn("month_name",   date_format(col("full_date"), "MMMM")) \
    .withColumn("quarter",      quarter(col("full_date"))) \
    .withColumn("year",         year(col("full_date"))) \
    .withColumn("week_of_year", weekofyear(col("full_date"))) \
    .withColumn("day_of_week",  dayofweek(col("full_date"))) \
    .withColumn("day_name",     date_format(col("full_date"), "EEEE")) \
    .withColumn("is_weekend",
        F.when(dayofweek(col("full_date")).isin(1, 7), 1).otherwise(0)) \
    .withColumn("year_month",   date_format(col("full_date"), "yyyy-MM"))

df_dim_date.write.format("delta").mode("overwrite").saveAsTable("dim_date")
print("dim_date written:", df_dim_date.count(), "rows")

# ── Cell 3: dim_customer ─────────────────────────────────────────
# One row per unique real customer (customer_unique_id is true identity)
w = Window.partitionBy("customer_unique_id").orderBy("customer_id")

df_dim_customer = df_customers \
    .repartition(8) \
    .withColumn("rn", row_number().over(w)) \
    .filter(col("rn") == 1) \
    .drop("rn") \
    .select(
        col("customer_unique_id").alias("customer_id"),
        col("customer_city"),
        col("customer_state"),
        col("customer_zip_code_prefix").alias("zip_code"),
        col("is_repeat_customer"),
        col("ingested_at")
    )

df_dim_customer.write.format("delta").mode("overwrite").saveAsTable("dim_customer")
print("dim_customer written:", df_dim_customer.count(), "rows")

# ── Cell 4: dim_product ──────────────────────────────────────────
df_dim_product = df_products \
    .select(
        col("product_id"),
        col("product_category_name"),
        col("product_category_english").alias("category_english"),
        col("product_weight_g"),
        col("product_length_cm"),
        col("product_height_cm"),
        col("product_width_cm"),
        col("product_photos_qty"),
        col("ingested_at")
    )

df_dim_product.write.format("delta").mode("overwrite").saveAsTable("dim_product")
print("dim_product written:", df_dim_product.count(), "rows")

# ── Cell 5: dim_seller ───────────────────────────────────────────
df_sellers = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE}/olist_sellers_dataset.csv")

df_dim_seller = df_sellers \
    .withColumn("ingested_at", current_timestamp()) \
    .select(
        col("seller_id"),
        col("seller_zip_code_prefix").alias("zip_code"),
        col("seller_city"),
        col("seller_state"),
        col("ingested_at")
    )

df_dim_seller.write.format("delta").mode("overwrite").saveAsTable("dim_seller")
print("dim_seller written:", df_dim_seller.count(), "rows")

# ── Cell 6: fact_orders ──────────────────────────────────────────
# Grain: one row per order
# Joins payments summary to get total order value
df_fact_orders = df_orders \
    .join(df_customers.select("customer_id", "customer_unique_id"),
          on="customer_id", how="left") \
    .join(
        df_payments.groupBy("order_id").agg(
            F.sum("payment_value").alias("total_payment_value"),
            F.max("payment_installments").alias("max_installments"),
            F.countDistinct("payment_type").alias("payment_methods_used")
        ),
        on="order_id", how="left"
    ) \
    .select(
        col("order_id"),
        col("customer_unique_id").alias("customer_id"),
        col("order_status"),
        date_format(col("order_purchase_timestamp"), "yyyyMMdd").cast("int").alias("order_date_id"),
        date_format(col("order_approved_at"), "yyyyMMdd").cast("int").alias("approved_date_id"),
        date_format(col("order_delivered_customer_date"), "yyyyMMdd").cast("int").alias("delivered_date_id"),
        col("order_estimated_delivery_date"),
        col("is_approved"),
        col("is_delivered"),
        col("is_late_delivery"),
        spark_round(col("delivery_delay_days"), 2).alias("delivery_delay_days"),
        spark_round(col("total_payment_value"), 2).alias("total_order_value"),
        col("max_installments"),
        col("payment_methods_used"),
        current_timestamp().alias("ingested_at")
    )

df_fact_orders.write.format("delta").mode("overwrite").saveAsTable("fact_orders")
print("fact_orders written:", df_fact_orders.count(), "rows")

# ── Cell 7: fact_order_items ─────────────────────────────────────
# Grain: one row per order line item
df_fact_order_items = df_items \
    .select(
        col("order_id"),
        col("order_item_id"),
        col("product_id"),
        col("seller_id"),
        spark_round(col("price"), 2).alias("price"),
        spark_round(col("freight_value"), 2).alias("freight_value"),
        spark_round(col("total_item_value"), 2).alias("total_item_value"),
        col("shipping_limit_date"),
        current_timestamp().alias("ingested_at")
    )

df_fact_order_items.write.format("delta").mode("overwrite").saveAsTable("fact_order_items")
print("fact_order_items written:", df_fact_order_items.count(), "rows")

# ── Cell 8: fact_payments ────────────────────────────────────────
# Grain: one row per payment transaction
df_fact_payments = df_payments \
    .select(
        col("order_id"),
        col("payment_sequential"),
        col("payment_type"),
        col("payment_installments"),
        spark_round(col("payment_value"), 2).alias("payment_value"),
        col("is_installment"),
        current_timestamp().alias("ingested_at")
    )

df_fact_payments.write.format("delta").mode("overwrite").saveAsTable("fact_payments")
print("fact_payments written:", df_fact_payments.count(), "rows")

# ── Cell 9: Gold Layer Verification ──────────────────────────────
print("=== GOLD LAYER COMPLETE ===")
print("\n-- DIMENSIONS --")
for d in ["dim_date", "dim_customer", "dim_product", "dim_seller"]:
    print(f"{d}: {spark.table(d).count()} rows")

print("\n-- FACTS --")
for f in ["fact_orders", "fact_order_items", "fact_payments"]:
    print(f"{f}: {spark.table(f).count()} rows")
