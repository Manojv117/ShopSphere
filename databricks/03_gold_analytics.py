# Databricks notebook source
from pyspark.sql import functions as F

orders = spark.table("workspace.shopsphere.silver_orders")
order_items = spark.table("workspace.shopsphere.silver_order_items")

print("Orders:", orders.count())
print("Order items:", order_items.count())

# COMMAND ----------

daily_revenue = (
    orders
    .filter(F.col("order_status") == "delivered")
    .join(order_items, on="order_id", how="inner")
    .withColumn(
        "purchase_date",
        F.to_date("order_purchase_timestamp")
    )
    .groupBy("purchase_date")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("price").alias("total_revenue"),
        F.sum("freight_value").alias("total_freight")
    )
    .orderBy("purchase_date")
)

display(daily_revenue)

# COMMAND ----------

(
    daily_revenue.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.shopsphere.gold_daily_revenue")
)

print("Gold daily revenue table created successfully!")

# COMMAND ----------

from pyspark.sql import functions as F

order_items = spark.table(
    "workspace.shopsphere.silver_order_items"
)

display(order_items.limit(5))

# COMMAND ----------

gold_product_performance = (
    order_items
    .groupBy("product_id")
    .agg(
        F.count("*").alias("units_sold"),
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("price").alias("total_revenue"),
        F.avg("price").alias("average_selling_price"),
        F.sum("freight_value").alias("total_freight")
    )
    .orderBy(F.desc("total_revenue"))
)

display(gold_product_performance.limit(20))

# COMMAND ----------

(
    gold_product_performance.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.shopsphere.gold_product_performance")
)

print("Gold product performance table created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Find the top 10 products by revenue
# MAGIC

# COMMAND ----------

gold_products = spark.table(
    "workspace.shopsphere.gold_product_performance"
)

display(
    gold_products
    .orderBy(F.desc("total_revenue"))
    .limit(10)
)

# COMMAND ----------

from pyspark.sql import functions as F

orders = spark.table("workspace.shopsphere.silver_orders")
customers = spark.table("workspace.shopsphere.silver_customers")
order_items = spark.table("workspace.shopsphere.silver_order_items")

# COMMAND ----------

# MAGIC %md
# MAGIC Calculate spending for each order

# COMMAND ----------

order_totals = (
    order_items
    .groupBy("order_id")
    .agg(
        F.sum("price").alias("order_value"),
        F.sum("freight_value").alias("order_freight")
    )
)

display(order_totals.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC Build the customer summary

# COMMAND ----------

delivered_orders = (
    orders
    .filter(F.col("order_status") == "delivered")
)

gold_customer_summary = (
    delivered_orders
    .join(order_totals, on="order_id", how="inner")
    .groupBy("customer_id")
    .agg(
        F.countDistinct("order_id").alias("total_orders"),
        F.sum("order_value").alias("total_spent"),
        F.avg("order_value").alias("average_order_value"),
        F.sum("order_freight").alias("total_freight")
    )
    .join(customers, on="customer_id", how="left")
    .select(
        "customer_id",
        "customer_unique_id",
        "customer_city",
        "customer_state",
        "total_orders",
        "total_spent",
        "average_order_value",
        "total_freight"
    )
)

display(gold_customer_summary.limit(10))

# COMMAND ----------

(
    gold_customer_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.shopsphere.gold_customer_summary")
)

print("Gold customer summary created successfully!")

# COMMAND ----------

# MAGIC %md
# MAGIC Find the highest-spending customers

# COMMAND ----------

gold_customers = spark.table(
    "workspace.shopsphere.gold_customer_summary"
)

display(
    gold_customers
    .orderBy(F.desc("total_spent"))
    .limit(10)
)