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
# MAGIC ## 9. Double Compute Verification
# MAGIC 
# MAGIC Verify whether queries use Databricks-only compute (reading Iceberg files directly) 
# MAGIC or double compute (routing through Snowflake).
# MAGIC 
# MAGIC **Expected behavior with Iceberg REST Catalog:** Databricks reads Parquet files directly 
# MAGIC from cloud storage using vended credentials - NO Snowflake compute should be used.

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9a. Check Physical Plan
# MAGIC 
# MAGIC If reading Iceberg directly, the plan will show `BatchScan` or `FileScan` with Parquet.
# MAGIC If using Snowflake connector (double compute), it will show `JDBCScan` or `SnowflakeScan`.

# COMMAND ----------

df_customers = spark.table("snowflake_iceberg.external_iceberg.customers")
print("=== PHYSICAL PLAN ===")
df_customers.explain(mode="simple")

# COMMAND ----------

print("=== EXTENDED PLAN (for detailed analysis) ===")
df_customers.explain(mode="extended")

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9b. Verify Table Format is Iceberg

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED snowflake_iceberg.external_iceberg.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9c. Check Table Properties for Iceberg Metadata

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES snowflake_iceberg.external_iceberg.customers;

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9d. Programmatic Double Compute Detection

# COMMAND ----------

import re

def check_double_compute(table_name):
    """
    Analyze query plan to determine if double compute is being used.
    
    Returns:
        dict with 'uses_double_compute' (bool), 'compute_type' (str), 'evidence' (str)
    """
    df = spark.table(table_name)
    plan = df._jdf.queryExecution().executedPlan().toString()
    
    double_compute_indicators = [
        "JDBCScan", "SnowflakeScan", "SnowflakeRelation", 
        "PushDownAggregate", "snowflake.jdbc"
    ]
    
    iceberg_direct_indicators = [
        "BatchScan", "FileScan parquet", "IcebergScan", 
        "DataSourceV2ScanRelation", "parquet"
    ]
    
    found_double = [ind for ind in double_compute_indicators if ind.lower() in plan.lower()]
    found_iceberg = [ind for ind in iceberg_direct_indicators if ind.lower() in plan.lower()]
    
    uses_double_compute = len(found_double) > 0
    
    if uses_double_compute:
        compute_type = "DOUBLE COMPUTE (Snowflake + Databricks)"
        evidence = f"Found in plan: {found_double}"
    elif found_iceberg:
        compute_type = "SINGLE COMPUTE (Databricks only - direct Iceberg read)"
        evidence = f"Found in plan: {found_iceberg}"
    else:
        compute_type = "UNKNOWN"
        evidence = "Could not determine from plan"
    
    return {
        "uses_double_compute": uses_double_compute,
        "compute_type": compute_type,
        "evidence": evidence,
        "plan_snippet": plan[:500]
    }

tables_to_check = ["customers", "orders", "events"]
print("=" * 80)
print("DOUBLE COMPUTE VERIFICATION RESULTS")
print("=" * 80)

for table in tables_to_check:
    full_name = f"snowflake_iceberg.external_iceberg.{table}"
    try:
        result = check_double_compute(full_name)
        status = "⚠️ DOUBLE" if result["uses_double_compute"] else "✅ SINGLE"
        print(f"\n{table.upper()}:")
        print(f"  Status: {status}")
        print(f"  Compute: {result['compute_type']}")
        print(f"  Evidence: {result['evidence']}")
    except Exception as e:
        print(f"\n{table.upper()}: ❌ ERROR - {str(e)[:60]}")

print("\n" + "=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### 9e. Snowflake Query History Check (Run in Snowflake)
# MAGIC 
# MAGIC To fully verify no Snowflake compute is used, run this query in Snowflake 
# MAGIC **before and after** executing Databricks queries:
# MAGIC 
# MAGIC ```sql
# MAGIC -- Run in Snowflake to check for any queries from Databricks
# MAGIC SELECT 
# MAGIC     query_id,
# MAGIC     query_text,
# MAGIC     start_time,
# MAGIC     total_elapsed_time,
# MAGIC     warehouse_name,
# MAGIC     user_name
# MAGIC FROM snowflake.account_usage.query_history
# MAGIC WHERE start_time > DATEADD(minute, -10, CURRENT_TIMESTAMP())
# MAGIC   AND query_type NOT IN ('DESCRIBE', 'SHOW')
# MAGIC ORDER BY start_time DESC
# MAGIC LIMIT 20;
# MAGIC ```
# MAGIC 
# MAGIC **If no SELECT queries appear during Databricks test execution, double compute is NOT being used.**

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

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary: Double Compute Analysis
# MAGIC 
# MAGIC | Method | What It Shows | Expected for Iceberg REST |
# MAGIC |--------|---------------|---------------------------|
# MAGIC | Physical Plan | `BatchScan`/`FileScan parquet` = direct read | ✅ No JDBC/Snowflake scan |
# MAGIC | DESCRIBE EXTENDED | Provider = `iceberg` | ✅ Iceberg format |
# MAGIC | TBLPROPERTIES | Metadata location in cloud storage | ✅ Points to Parquet files |
# MAGIC | Snowflake Query History | No SELECT queries during test | ✅ No warehouse usage |
# MAGIC 
# MAGIC **Conclusion:** If all checks pass, Databricks is reading Iceberg files directly 
# MAGIC from cloud storage with vended credentials - **single compute only**.
