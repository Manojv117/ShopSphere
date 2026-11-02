# Databricks notebook source
from pyspark.sql import *
orders_bronze = spark.table(
    "workspace.shopsphere.bronze_orders"
)

display(orders_bronze.limit(10))

# COMMAND ----------

import pyspark.sql.functions as F

print("Total records:", orders_bronze.count())

print(
    "Distinct order IDs:",
    orders_bronze.select("order_id").distinct().count()
)

print(
    "Null order IDs:",
    orders_bronze.filter(
        F.col("order_id").isNull()
    ).count()
)

# COMMAND ----------

duplicate_orders = (
    orders_bronze
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

display(duplicate_orders)

# COMMAND ----------

orders_silver = (
    orders_bronze
    .filter(F.col("order_id").isNotNull())
    .dropDuplicates(["order_id"])
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
)

# COMMAND ----------

# DBTITLE 1,Write silver_orders
orders_silver = orders_silver.withColumn(
    "order_delivered_carrier_date",
    F.to_timestamp("order_delivered_carrier_date")
)

(
    orders_silver.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.shopsphere.silver_orders")
)

# COMMAND ----------

silver_orders = spark.table(
    "workspace.shopsphere.silver_orders"
)

print("Silver row count:", silver_orders.count())

print(
    "Distinct order IDs:",
    silver_orders.select("order_id").distinct().count()
)

silver_orders.printSchema()

# COMMAND ----------

from pyspark.sql import functions as F

# Read Bronze data
orders_bronze = spark.table(
    "workspace.shopsphere.bronze_orders"
)

# Convert timestamps and standardize the schema
orders_silver = (
    orders_bronze
    .withColumn(
        "order_purchase_timestamp",
        F.to_timestamp("order_purchase_timestamp")
    )
    .withColumn(
        "order_approved_at",
        F.to_timestamp("order_approved_at")
    )
    .withColumn(
        "order_delivered_carrier_date",
        F.to_timestamp("order_delivered_carrier_date")
    )
    .withColumn(
        "order_delivered_customer_date",
        F.to_timestamp("order_delivered_customer_date")
    )
    .withColumn(
        "order_estimated_delivery_date",
        F.to_timestamp("order_estimated_delivery_date")
    )
)

print("Silver transformation completed!")

# COMMAND ----------

(
    orders_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable("workspace.shopsphere.silver_orders")
)

print("silver_orders created successfully!")

# COMMAND ----------

from pyspark.sql import functions as F

silver_orders = spark.table("workspace.shopsphere.silver_orders")

# 1. Check total rows
print("Total rows:", silver_orders.count())

# 2. Check duplicate order IDs
duplicate_orders = (
    silver_orders
    .groupBy("order_id")
    .count()
    .filter(F.col("count") > 1)
)

print("Duplicate order IDs:", duplicate_orders.count())

# 3. Check null order IDs
print(
    "Null order IDs:",
    silver_orders.filter(F.col("order_id").isNull()).count()
)

# 4. Check missing purchase timestamps
print(
    "Null purchase timestamps:",
    silver_orders.filter(
        F.col("order_purchase_timestamp").isNull()
    ).count()
)

# 5. Check order statuses
display(
    silver_orders.groupBy("order_status")
    .count()
    .orderBy(F.desc("count"))
)

# COMMAND ----------

from pyspark.sql import functions as F

customers_bronze = spark.table("workspace.shopsphere.bronze_customers")

display(customers_bronze.limit(5))

# COMMAND ----------

customers_silver = (
    customers_bronze
    .dropDuplicates(["customer_id"])
    .filter(F.col("customer_id").isNotNull())
    .withColumn(
        "customer_zip_code_prefix",
        F.col("customer_zip_code_prefix").cast("int")
    )
    .withColumn(
        "customer_city",
        F.trim(F.lower(F.col("customer_city")))
    )
    .withColumn(
        "customer_state",
        F.upper(F.trim(F.col("customer_state")))
    )
)

display(customers_silver.limit(10))

# COMMAND ----------

(
    customers_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.shopsphere.silver_customers")
)

print("silver_customers created successfully!")

# COMMAND ----------

from pyspark.sql import functions as F

order_items_bronze = spark.table(
    "workspace.shopsphere.bronze_order_items"
)

display(order_items_bronze.limit(10))



# COMMAND ----------

order_items_silver = (
    order_items_bronze

    # Remove duplicate order-item records
    .dropDuplicates(["order_id", "order_item_id"])

    # Remove records without essential identifiers
    .filter(F.col("order_id").isNotNull())
    .filter(F.col("order_item_id").isNotNull())
    .filter(F.col("product_id").isNotNull())

    # Convert numeric columns
    .withColumn("order_item_id", F.col("order_item_id").cast("int"))
    .withColumn("price", F.col("price").cast("decimal(10,2)"))
    .withColumn("freight_value", F.col("freight_value").cast("decimal(10,2)"))

    # Standardize product IDs
    .withColumn("product_id", F.trim(F.col("product_id")))
)

display(order_items_silver.limit(10))

# COMMAND ----------

date_columns = [
    "order_purchase_timestamp",
    "order_approved_at",
    "order_delivered_carrier_date",
    "order_delivered_customer_date",
    "order_estimated_delivery_date"
]

for col_name in date_columns:
    orders_silver = orders_silver.withColumn(
        col_name,
        F.to_timestamp(F.col(col_name))
    )

# COMMAND ----------

(
    order_items_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("workspace.shopsphere.silver_order_items")
)

print("silver_order_items created successfully!")