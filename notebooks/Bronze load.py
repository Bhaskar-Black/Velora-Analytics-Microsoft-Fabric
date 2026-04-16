# ============================================
# Velora Analytics — Bronze Layer
# Notebook: 01_bronze_load
# Purpose: Land raw CSV files into Bronze lakehouse as Delta tables
# Input: Raw CSV files in Velora_Bronze/Files/
# Output: 8 Bronze Delta tables + bronze_load_log
# ============================================

from datetime import datetime
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

BRONZE_FILES = "abfss://E-commerce@onelake.dfs.fabric.microsoft.com/Velora_Bronze.Lakehouse/Files"

# ── Cell 1: Load Orders ──────────────────────────────────────────
df_orders = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_orders_dataset.csv")
df_orders.write.format("delta").mode("overwrite").saveAsTable("bronze_orders")
print("bronze_orders loaded:", df_orders.count())

# ── Cell 2: Load Order Items ─────────────────────────────────────
df_items = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_order_items_dataset.csv")
df_items.write.format("delta").mode("overwrite").saveAsTable("bronze_order_items")
print("bronze_order_items loaded:", df_items.count())

# ── Cell 3: Load Payments ────────────────────────────────────────
df_payments = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_order_payments_dataset.csv")
df_payments.write.format("delta").mode("overwrite").saveAsTable("bronze_payments")
print("bronze_payments loaded:", df_payments.count())

# ── Cell 4: Load Customers ───────────────────────────────────────
df_customers = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_customers_dataset.csv")
df_customers.write.format("delta").mode("overwrite").saveAsTable("bronze_customers")
print("bronze_customers loaded:", df_customers.count())

# ── Cell 5: Load Products ────────────────────────────────────────
df_products = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_products_dataset.csv")
df_products.write.format("delta").mode("overwrite").saveAsTable("bronze_products")
print("bronze_products loaded:", df_products.count())

# ── Cell 6: Load Sellers ─────────────────────────────────────────
df_sellers = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_sellers_dataset.csv")
df_sellers.write.format("delta").mode("overwrite").saveAsTable("bronze_sellers")
print("bronze_sellers loaded:", df_sellers.count())

# ── Cell 7: Load Reviews ─────────────────────────────────────────
df_reviews = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/olist_order_reviews_dataset.csv")
df_reviews.write.format("delta").mode("overwrite").saveAsTable("bronze_reviews")
print("bronze_reviews loaded:", df_reviews.count())

# ── Cell 8: Load Category Translation ───────────────────────────
df_category = spark.read.option("header", True).option("inferSchema", True)\
    .csv(f"{BRONZE_FILES}/product_category_name_translation.csv")
df_category.write.format("delta").mode("overwrite").saveAsTable("bronze_category_translation")
print("bronze_category_translation loaded:", df_category.count())

# ── Cell 9: Verify All Tables ────────────────────────────────────
tables = [
    "bronze_orders", "bronze_order_items", "bronze_payments",
    "bronze_customers", "bronze_products", "bronze_sellers",
    "bronze_reviews", "bronze_category_translation"
]
for t in tables:
    print(f"{t}: {spark.table(t).count()} rows")

# ── Cell 10: Write Load Log ──────────────────────────────────────
log_data = [
    ("bronze_orders",               99441,  "olist_orders_dataset.csv",                  datetime.now()),
    ("bronze_order_items",          112650, "olist_order_items_dataset.csv",             datetime.now()),
    ("bronze_payments",             103886, "olist_order_payments_dataset.csv",          datetime.now()),
    ("bronze_customers",            99441,  "olist_customers_dataset.csv",               datetime.now()),
    ("bronze_products",             32951,  "olist_products_dataset.csv",                datetime.now()),
    ("bronze_sellers",              3095,   "olist_sellers_dataset.csv",                 datetime.now()),
    ("bronze_reviews",              104162, "olist_order_reviews_dataset.csv",           datetime.now()),
    ("bronze_category_translation", 71,     "product_category_name_translation.csv",     datetime.now()),
]

schema = StructType([
    StructField("table_name",   StringType()),
    StructField("row_count",    IntegerType()),
    StructField("source_file",  StringType()),
    StructField("loaded_at",    TimestampType()),
])

df_log = spark.createDataFrame(log_data, schema)
df_log.write.format("delta").mode("overwrite").saveAsTable("bronze_load_log")
spark.table("bronze_load_log").show(truncate=False)
