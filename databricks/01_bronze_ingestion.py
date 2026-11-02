# Databricks notebook source
print("ShopSphere Data Engineering Pipeline")
print("Environment: Databricks")
print("Processing engine: Apache Spark")

# COMMAND ----------

print("Spark version:", spark.version)

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %pip install kaggle

# COMMAND ----------

dbutils.secrets.listScopes()
scopes = dbutils.secrets.listScopes()

if scopes:
    print("Available secret scopes:")
    for scope in scopes:
        print(scope.name)
else:
    print("No secret scopes are available.")

print("Databricks utilities available:", dbutils is not None)

# COMMAND ----------

# MAGIC %pip install kagglehub
# MAGIC
# MAGIC
# MAGIC import kagglehub
# MAGIC
# MAGIC # Download latest version
# MAGIC path = kagglehub.dataset_download("olistbr/brazilian-ecommerce")
# MAGIC
# MAGIC print("Path to dataset files:", )

# COMMAND ----------

import os
import kagglehub

# COMMAND ----------

path = kagglehub.dataset_download(
    "olistbr/brazilian-ecommerce"
)

print("Dataset path:", path)

print("\nFiles available:")
for file in os.listdir(path):
    print(file)

# COMMAND ----------

import pandas as pd

orders_path = os.path.join(
    path,
    "olist_orders_dataset.csv"
)

orders_df = spark.createDataFrame(pd.read_csv(orders_path))

display(orders_df.limit(10))

# COMMAND ----------

spark.sql("""
CREATE SCHEMA IF NOT EXISTS workspace.shopsphere
""")


# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

bronze_orders = (
    orders_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit(orders_path))
)

(
    bronze_orders.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.shopsphere.bronze_orders")
)

print("Bronze orders table created successfully!")

# COMMAND ----------

display(
    spark.table("workspace.shopsphere.bronze_orders").limit(10)
)
print(
    "Total Bronze orders:",
    spark.table("workspace.shopsphere.bronze_orders").count()
)
spark.table("workspace.shopsphere.bronze_orders").printSchema()

# COMMAND ----------

customers_path = os.path.join(
    path,
    "olist_customers_dataset.csv"
)

customers_df = spark.createDataFrame(pd.read_csv(customers_path))

print("Customers:", customers_df.count())
display(customers_df.limit(5))

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

bronze_customers = (
    customers_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("olist_customers_dataset.csv"))
)

(
    bronze_customers.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.shopsphere.bronze_customers")
)

print("Bronze customers table created!")

# COMMAND ----------

order_items_path = os.path.join(
    path,
    "olist_order_items_dataset.csv"
)

order_items_df = spark.createDataFrame(pd.read_csv(order_items_path))

print("Order items:", order_items_df.count())
display(order_items_df.limit(5))

# COMMAND ----------

bronze_order_items = (
    order_items_df
    .withColumn("_ingested_at", current_timestamp())
    .withColumn("_source_file", lit("olist_order_items_dataset.csv"))
)

(
    bronze_order_items.write
    .format("delta")
    .mode("overwrite")
    .saveAsTable("workspace.shopsphere.bronze_order_items")
)

print("Bronze order items table created!")

# COMMAND ----------

tables = [
    "bronze_orders",
    "bronze_customers",
    "bronze_order_items"
]

for table_name in tables:
    df = spark.table(f"workspace.shopsphere.{table_name}")

    print(f"{table_name}: {df.count()} rows")