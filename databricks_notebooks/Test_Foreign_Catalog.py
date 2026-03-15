# Databricks notebook source
# MAGIC %md
# MAGIC # Test Snowflake Foreign Catalog
# MAGIC 
# MAGIC Verify connectivity to Snowflake Iceberg tables via Unity Catalog Foreign Catalog.
# MAGIC 
# MAGIC **Foreign Catalog:** `snowflake_iceberg`  
# MAGIC **Snowflake Database:** `ICEBERG_POC`  
# MAGIC **Connection:** `snowflake_iceberg_conn` (PAT auth)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. List Schemas

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN snowflake_iceberg;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. List Tables in EXTERNAL_ICEBERG Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN snowflake_iceberg.external_iceberg;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Query CUSTOMERS Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM snowflake_iceberg.external_iceberg.customers LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Query ORDERS Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM snowflake_iceberg.external_iceberg.orders LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Query EVENTS Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM snowflake_iceberg.external_iceberg.events LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Aggregation Query

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     COUNT(*) as total_customers
# MAGIC FROM snowflake_iceberg.external_iceberg.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Join Query (Customers + Orders)

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     c.customer_id,
# MAGIC     COUNT(o.order_id) as order_count
# MAGIC FROM snowflake_iceberg.external_iceberg.customers c
# MAGIC LEFT JOIN snowflake_iceberg.external_iceberg.orders o
# MAGIC     ON c.customer_id = o.customer_id
# MAGIC GROUP BY c.customer_id
# MAGIC ORDER BY order_count DESC
# MAGIC LIMIT 10;

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. PySpark DataFrame Access

# COMMAND ----------

df_customers = spark.table("snowflake_iceberg.external_iceberg.customers")
print(f"Customers row count: {df_customers.count():,}")
df_customers.printSchema()

# COMMAND ----------

df_orders = spark.table("snowflake_iceberg.external_iceberg.orders")
print(f"Orders row count: {df_orders.count():,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

results = []

tables = ["customers", "events", "orders", "products", "transactions"]
for table in tables:
    try:
        count = spark.table(f"snowflake_iceberg.external_iceberg.{table}").count()
        results.append((table, count, "PASS"))
    except Exception as e:
        results.append((table, 0, f"FAIL: {str(e)[:50]}"))

print("=" * 60)
print("FOREIGN CATALOG TEST RESULTS")
print("=" * 60)
for table, count, status in results:
    print(f"  {table:15} | {count:>8,} rows | {status}")
print("=" * 60)
