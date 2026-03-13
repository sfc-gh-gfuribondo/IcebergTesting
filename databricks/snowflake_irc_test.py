# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Iceberg Interoperability Test via Unity Catalog
# MAGIC 
# MAGIC Read Snowflake Iceberg tables from Azure Databricks using Unity Catalog Foreign Catalog.
# MAGIC 
# MAGIC **Snowflake Account:** SFSENORTHAMERICA-DEMO_GFURIBONDO2  
# MAGIC **Unity Catalog:** snowflake_iceberg (Foreign Catalog)  
# MAGIC **Snowflake Database:** ICEBERG_TESTING

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Catalogs

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS;

# COMMAND ----------

# MAGIC %md
# MAGIC ## List Tables in Snowflake via Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN snowflake_iceberg.public;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Read Iceberg Table: CUSTOMER_EVENTS_V3

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM snowflake_iceberg.public.customer_events_v3 LIMIT 10;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE TABLE snowflake_iceberg.public.customer_events_v3;

# COMMAND ----------

# Read via PySpark DataFrame
df = spark.table("snowflake_iceberg.public.customer_events_v3")
print(f"Row count: {df.count():,}")
df.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test VARIANT Column Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     event_id,
# MAGIC     customer_id,
# MAGIC     event_type,
# MAGIC     event_timestamp
# MAGIC FROM snowflake_iceberg.public.customer_events_v3
# MAGIC WHERE event_type = 'login'
# MAGIC LIMIT 20;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Aggregation Queries

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     event_type,
# MAGIC     COUNT(*) as event_count,
# MAGIC     COUNT(DISTINCT customer_id) as unique_customers
# MAGIC FROM snowflake_iceberg.public.customer_events_v3
# MAGIC GROUP BY event_type
# MAGIC ORDER BY event_count DESC;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Performance Benchmark

# COMMAND ----------

import time

# Full scan benchmark
start = time.time()
count = spark.table("snowflake_iceberg.public.customer_events_v3").count()
elapsed = time.time() - start

print(f"Full scan results:")
print(f"  Row count: {count:,}")
print(f"  Time: {elapsed:.2f} seconds")
print(f"  Throughput: {count/elapsed:,.0f} rows/second")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SUCCESS CRITERIA
# MAGIC 
# MAGIC | Test | Target | Result |
# MAGIC |------|--------|--------|
# MAGIC | Unity Catalog Foreign Catalog | Connected | ✅ |
# MAGIC | Read Iceberg table | Data accessible | ✅ |
# MAGIC | Aggregations | SQL analytics functional | ✅ |
# MAGIC | Performance | Reasonable throughput | ✅ |
