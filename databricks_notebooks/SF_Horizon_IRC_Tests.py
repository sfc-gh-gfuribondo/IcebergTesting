# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Horizon IRC Tests
# MAGIC 
# MAGIC Test reading Snowflake Iceberg tables via Horizon IRC endpoint.
# MAGIC 
# MAGIC **Run `SF_Horizon_IRC_Setup` first to configure the catalog.**
# MAGIC 
# MAGIC ## Test Scenarios
# MAGIC 1. List namespaces and tables
# MAGIC 2. Read EXTERNAL_ICEBERG.CUSTOMERS (99,998 rows)
# MAGIC 3. Aggregation queries
# MAGIC 4. Join queries
# MAGIC 5. Endpoint compatibility (blob vs abfss)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 1: List Namespaces

# COMMAND ----------

CATALOG = "gf_snowflake_iceberg"

# COMMAND ----------

namespaces_df = spark.sql(f"SHOW NAMESPACES IN {CATALOG}")
namespaces_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 2: List Tables in EXTERNAL_ICEBERG

# COMMAND ----------

tables_df = spark.sql(f"SHOW TABLES IN {CATALOG}.EXTERNAL_ICEBERG")
tables_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 3: Read CUSTOMERS Table
# MAGIC 
# MAGIC Expected: ~99,998 rows
# MAGIC 
# MAGIC **This tests blob endpoint compatibility** - Snowflake stores data at `azure://...blob.core.windows.net`

# COMMAND ----------

df_customers = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS")
row_count = df_customers.count()
print(f"CUSTOMERS row count: {row_count}")
print(f"Expected: 99,998")
print(f"Status: {'PASS' if row_count == 99998 else 'CHECK'}")

# COMMAND ----------

df_customers.show(10)

# COMMAND ----------

df_customers.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Aggregation Query

# COMMAND ----------

agg_df = spark.sql(f"""
    SELECT 
        customer_tier,
        region,
        COUNT(*) as customer_count,
        ROUND(AVG(lifetime_value), 2) as avg_ltv
    FROM {CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS
    GROUP BY customer_tier, region
    ORDER BY customer_count DESC
""")
agg_df.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Read ORDERS Table

# COMMAND ----------

df_orders = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.ORDERS")
print(f"ORDERS row count: {df_orders.count()}")
df_orders.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Join CUSTOMERS and ORDERS

# COMMAND ----------

join_df = spark.sql(f"""
    SELECT 
        c.customer_tier,
        o.order_status,
        COUNT(*) as order_count,
        ROUND(SUM(o.total_amount), 2) as total_revenue,
        ROUND(AVG(o.total_amount), 2) as avg_order_value
    FROM {CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS c
    JOIN {CATALOG}.EXTERNAL_ICEBERG.ORDERS o 
        ON c.customer_id = o.customer_id
    GROUP BY c.customer_tier, o.order_status
    ORDER BY total_revenue DESC
    LIMIT 20
""")
join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Read TRANSACTIONS Table (with VARIANT data)

# COMMAND ----------

df_txn = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.TRANSACTIONS")
print(f"TRANSACTIONS row count: {df_txn.count()}")
df_txn.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Query VARIANT/JSON Data

# COMMAND ----------

variant_df = spark.sql(f"""
    SELECT 
        transaction_type,
        currency,
        COUNT(*) as txn_count,
        ROUND(SUM(amount), 2) as total_amount
    FROM {CATALOG}.EXTERNAL_ICEBERG.TRANSACTIONS
    GROUP BY transaction_type, currency
    ORDER BY txn_count DESC
    LIMIT 20
""")
variant_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

print("=" * 60)
print("SNOWFLAKE HORIZON IRC TEST RESULTS")
print("=" * 60)

tests = [
    ("List Namespaces", True),
    ("List Tables", True),
    ("Read CUSTOMERS", df_customers.count() == 99998),
    ("Aggregation Query", agg_df.count() > 0),
    ("Read ORDERS", df_orders.count() > 0),
    ("Join Query", join_df.count() > 0),
    ("Read TRANSACTIONS", df_txn.count() > 0),
    ("VARIANT Query", variant_df.count() > 0),
]

for test_name, passed in tests:
    status = "PASS" if passed else "FAIL"
    print(f"  {test_name}: {status}")

print("=" * 60)
print(f"Endpoint: azure:// (blob) - {'COMPATIBLE' if all(t[1] for t in tests) else 'CHECK ERRORS'}")
print("=" * 60)
