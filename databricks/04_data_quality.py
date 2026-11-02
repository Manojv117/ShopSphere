# Databricks notebook source
from pyspark.sql import functions as F

bronze_orders = spark.table("workspace.shopsphere.bronze_orders")
silver_orders = spark.table("workspace.shopsphere.silver_orders")
silver_customers = spark.table("workspace.shopsphere.silver_customers")
silver_order_items = spark.table("workspace.shopsphere.silver_order_items")

gold_daily_revenue = spark.table("workspace.shopsphere.gold_daily_revenue")
gold_product_performance = spark.table("workspace.shopsphere.gold_product_performance")
gold_customer_summary = spark.table("workspace.shopsphere.gold_customer_summary")

print("All tables loaded successfully!")



tables = {
    "bronze_orders": bronze_orders,
    "silver_orders": silver_orders,
    "silver_customers": silver_customers,
    "silver_order_items": silver_order_items,
    "gold_daily_revenue": gold_daily_revenue,
    "gold_product_performance": gold_product_performance,
    "gold_customer_summary": gold_customer_summary
}

for name, df in tables.items():
    print(f"{name}: {df.count()} rows")

# COMMAND ----------

duplicate_checks = {
    "silver_orders": (silver_orders, ["order_id"]),
    "silver_customers": (silver_customers, ["customer_id"]),
    "silver_order_items": (silver_order_items, ["order_id", "order_item_id"])
}

for name, (df, keys) in duplicate_checks.items():
    duplicates = (
        df.groupBy(*keys)
        .count()
        .filter(F.col("count") > 1)
        .count()
    )

    status = "PASS" if duplicates == 0 else "FAIL"
    print(f"{name} duplicate check: {status} ({duplicates} duplicates)")

# COMMAND ----------

null_checks = {
    "silver_orders": (silver_orders, ["order_id", "order_purchase_timestamp"]),
    "silver_customers": (silver_customers, ["customer_id"]),
    "silver_order_items": (silver_order_items, ["order_id", "order_item_id", "product_id", "price"])
}

for name, (df, columns) in null_checks.items():
    for column in columns:
        null_count = df.filter(F.col(column).isNull()).count()

        status = "PASS" if null_count == 0 else "FAIL"
        print(f"{name}.{column}: {status} ({null_count} nulls)")

# COMMAND ----------

expected_revenue = (
    silver_orders
    .filter(F.col("order_status") == "delivered")
    .join(silver_order_items, on="order_id", how="inner")
    .agg(F.sum("price").alias("expected_revenue"))
    .first()["expected_revenue"]
)

actual_revenue = (
    gold_daily_revenue
    .agg(F.sum("total_revenue").alias("actual_revenue"))
    .first()["actual_revenue"]
)

print("Expected revenue:", expected_revenue)
print("Gold revenue:", actual_revenue)

if expected_revenue == actual_revenue:
    print("PASS: Revenue totals match")
else:
    print("FAIL: Revenue totals do not match")