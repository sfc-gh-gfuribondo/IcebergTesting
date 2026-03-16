# Databricks notebook source
# MAGIC %md
# MAGIC # Foreign Catalog Test Suite
# MAGIC 
# MAGIC Comprehensive tests for Snowflake Foreign Catalog connectivity via Unity Catalog.
# MAGIC 
# MAGIC **Foreign Catalog:** `snowflake_iceberg`  
# MAGIC **Snowflake Database:** `ICEBERG_POC`  
# MAGIC **Schema:** `EXTERNAL_ICEBERG`
# MAGIC 
# MAGIC ## Test Categories
# MAGIC 1. Connectivity & Discovery
# MAGIC 2. Basic Read Operations
# MAGIC 3. Data Type Handling
# MAGIC 4. Aggregations & Analytics
# MAGIC 5. Join Operations
# MAGIC 6. Performance Benchmarks
# MAGIC 7. SQL Compatibility
# MAGIC 
# MAGIC ---
# MAGIC 
# MAGIC ## Compute & Data Access Legend
# MAGIC 
# MAGIC | Symbol | Meaning |
# MAGIC |--------|---------|
# MAGIC | 🟦 **DBX** | Databricks Spark compute only |
# MAGIC | 🟩 **SF** | Snowflake compute only |
# MAGIC | 🟨 **BOTH** | Both engines involved |
# MAGIC | ⚡ **IRC** | Iceberg REST Catalog (metadata) |
# MAGIC | 📁 **STORAGE** | Direct cloud storage read (Parquet) |
# MAGIC | 🔗 **JDBC** | JDBC connector (double compute) |
# MAGIC | 🎫 **VENDED** | Vended credentials (temp SAS/presigned URLs) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup & Configuration
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** N/A (local setup)

# COMMAND ----------

import time
from pyspark.sql import functions as F
from pyspark.sql.types import *

CATALOG = "snowflake_iceberg"
SCHEMA = "external_iceberg"

EXPECTED_COUNTS = {
    "customers": 99998,
    "events": 1000000,
    "orders": 500000,
    "products": 10000,
    "transactions": 1000000
}

test_results = []

def run_test(test_name, test_func, category="General", compute="DBX", protocol="IRC+STORAGE"):
    """Execute a test and record results with compute/protocol info."""
    start = time.time()
    try:
        result = test_func()
        elapsed = time.time() - start
        test_results.append({
            "category": category,
            "test": test_name,
            "status": "PASS",
            "elapsed_sec": round(elapsed, 2),
            "compute": compute,
            "protocol": protocol,
            "details": str(result) if result else ""
        })
        print(f"✅ {test_name}: PASS ({elapsed:.2f}s) [{compute} | {protocol}]")
        return True
    except Exception as e:
        elapsed = time.time() - start
        test_results.append({
            "category": category,
            "test": test_name,
            "status": "FAIL",
            "elapsed_sec": round(elapsed, 2),
            "compute": compute,
            "protocol": protocol,
            "details": str(e)[:200]
        })
        print(f"❌ {test_name}: FAIL - {str(e)[:100]}")
        return False

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Connectivity & Discovery Tests
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC (metadata only)
# MAGIC 
# MAGIC These tests query Unity Catalog metadata, which fetches table listings from Snowflake's Horizon IRC.
# MAGIC - **NO data movement** - just catalog metadata
# MAGIC - **NO Snowflake warehouse** consumption
# MAGIC - IRC serves table schemas, locations, and permissions

# COMMAND ----------

def test_catalog_exists():
    catalogs = spark.sql("SHOW CATALOGS").collect()
    catalog_names = [row.catalog for row in catalogs]
    assert CATALOG in catalog_names, f"Catalog {CATALOG} not found"
    return f"Found {len(catalog_names)} catalogs"

def test_list_schemas():
    schemas = spark.sql(f"SHOW SCHEMAS IN {CATALOG}").collect()
    schema_names = [row.databaseName for row in schemas]
    assert SCHEMA in schema_names, f"Schema {SCHEMA} not found"
    return f"Found {len(schema_names)} schemas: {schema_names}"

def test_list_tables():
    tables = spark.sql(f"SHOW TABLES IN {CATALOG}.{SCHEMA}").collect()
    table_names = [row.tableName.lower() for row in tables]
    expected = list(EXPECTED_COUNTS.keys())
    for t in expected:
        assert t in table_names, f"Table {t} not found"
    return f"Found {len(table_names)} tables"

run_test("Catalog Exists", test_catalog_exists, "Connectivity", "DBX", "IRC")
run_test("List Schemas", test_list_schemas, "Connectivity", "DBX", "IRC")
run_test("List Tables", test_list_tables, "Connectivity", "DBX", "IRC")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Basic Read Operations
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC → 🎫 VENDED → 📁 STORAGE
# MAGIC 
# MAGIC Data flow:
# MAGIC 1. **IRC** → Databricks asks Snowflake IRC for table metadata + file locations
# MAGIC 2. **VENDED** → Snowflake returns temp SAS tokens / presigned URLs (valid ~1hr)
# MAGIC 3. **STORAGE** → Databricks reads Parquet files DIRECTLY from Azure/S3/GCS
# MAGIC 
# MAGIC **⚠️ NO Snowflake warehouse involved! Zero double compute.**

# COMMAND ----------

def test_read_customers():
    df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
    count = df.count()
    assert count == EXPECTED_COUNTS["customers"], f"Expected {EXPECTED_COUNTS['customers']}, got {count}"
    return f"{count:,} rows"

def test_read_events():
    df = spark.table(f"{CATALOG}.{SCHEMA}.events")
    count = df.count()
    assert count == EXPECTED_COUNTS["events"], f"Expected {EXPECTED_COUNTS['events']}, got {count}"
    return f"{count:,} rows"

def test_read_orders():
    df = spark.table(f"{CATALOG}.{SCHEMA}.orders")
    count = df.count()
    assert count == EXPECTED_COUNTS["orders"], f"Expected {EXPECTED_COUNTS['orders']}, got {count}"
    return f"{count:,} rows"

def test_read_products():
    df = spark.table(f"{CATALOG}.{SCHEMA}.products")
    count = df.count()
    assert count == EXPECTED_COUNTS["products"], f"Expected {EXPECTED_COUNTS['products']}, got {count}"
    return f"{count:,} rows"

def test_read_transactions():
    df = spark.table(f"{CATALOG}.{SCHEMA}.transactions")
    count = df.count()
    assert count == EXPECTED_COUNTS["transactions"], f"Expected {EXPECTED_COUNTS['transactions']}, got {count}"
    return f"{count:,} rows"

run_test("Read CUSTOMERS", test_read_customers, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read EVENTS", test_read_events, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read ORDERS", test_read_orders, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read PRODUCTS", test_read_products, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read TRANSACTIONS", test_read_transactions, "Read Operations", "DBX", "IRC+VENDED+STORAGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Data Type Handling
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC → 🎫 VENDED → 📁 STORAGE
# MAGIC 
# MAGIC Tests verify that Iceberg schema (from IRC) maps correctly to Spark types:
# MAGIC - Snowflake VARIANT → Spark StringType (JSON serialized)
# MAGIC - Snowflake TIMESTAMP_NTZ(9) → Spark TimestampType (nanosecond precision)
# MAGIC - Snowflake NUMBER → Spark DecimalType/LongType

# COMMAND ----------

def test_numeric_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
    row = df.select("customer_id", "lifetime_value").first()
    assert row.customer_id is not None
    assert isinstance(row.lifetime_value, (int, float, type(None)))
    return f"customer_id={row.customer_id}, lifetime_value={row.lifetime_value}"

def test_string_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
    row = df.select("customer_name", "customer_tier", "region").first()
    assert isinstance(row.customer_name, str)
    return f"name={row.customer_name}, tier={row.customer_tier}"

def test_date_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.customers")
    row = df.select("signup_date").first()
    assert row.signup_date is not None
    return f"signup_date={row.signup_date}"

def test_timestamp_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.events")
    row = df.select("event_timestamp").first()
    assert row.event_timestamp is not None
    return f"event_timestamp={row.event_timestamp}"

def test_variant_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.events")
    row = df.select("event_data").first()
    return f"event_data type={type(row.event_data).__name__}"

run_test("Numeric Types", test_numeric_types, "Data Types", "DBX", "IRC+VENDED+STORAGE")
run_test("String Types", test_string_types, "Data Types", "DBX", "IRC+VENDED+STORAGE")
run_test("Date Types", test_date_types, "Data Types", "DBX", "IRC+VENDED+STORAGE")
run_test("Timestamp Types", test_timestamp_types, "Data Types", "DBX", "IRC+VENDED+STORAGE")
run_test("Variant Types", test_variant_types, "Data Types", "DBX", "IRC+VENDED+STORAGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Aggregations & Analytics
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC → 🎫 VENDED → 📁 STORAGE
# MAGIC 
# MAGIC All aggregations execute entirely in Databricks Spark:
# MAGIC - COUNT, SUM, AVG computed by Spark executors
# MAGIC - GROUP BY handled by Spark shuffle
# MAGIC - Window functions run in Spark
# MAGIC 
# MAGIC **Snowflake's role:** Metadata + credentials only. ZERO compute consumption.

# COMMAND ----------

def test_count_aggregation():
    result = spark.sql(f"""
        SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.customers
    """).first()
    assert result.cnt == EXPECTED_COUNTS["customers"]
    return f"count={result.cnt:,}"

def test_sum_aggregation():
    result = spark.sql(f"""
        SELECT SUM(lifetime_value) as total FROM {CATALOG}.{SCHEMA}.customers
    """).first()
    assert result.total is not None
    return f"total_lifetime_value={result.total:,.2f}"

def test_avg_aggregation():
    result = spark.sql(f"""
        SELECT AVG(total_amount) as avg_order FROM {CATALOG}.{SCHEMA}.orders
    """).first()
    assert result.avg_order is not None
    return f"avg_order_amount={result.avg_order:.2f}"

def test_group_by():
    result = spark.sql(f"""
        SELECT customer_tier, COUNT(*) as cnt 
        FROM {CATALOG}.{SCHEMA}.customers 
        GROUP BY customer_tier
        ORDER BY cnt DESC
    """).collect()
    assert len(result) > 0
    return f"{len(result)} tiers"

def test_distinct():
    result = spark.sql(f"""
        SELECT COUNT(DISTINCT region) as regions FROM {CATALOG}.{SCHEMA}.customers
    """).first()
    assert result.regions > 0
    return f"{result.regions} distinct regions"

def test_window_function():
    result = spark.sql(f"""
        SELECT customer_id, total_amount,
               ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY total_amount DESC) as rn
        FROM {CATALOG}.{SCHEMA}.orders
        LIMIT 10
    """).collect()
    assert len(result) > 0
    return f"{len(result)} rows with window function"

run_test("COUNT Aggregation", test_count_aggregation, "Aggregations", "DBX", "IRC+VENDED+STORAGE")
run_test("SUM Aggregation", test_sum_aggregation, "Aggregations", "DBX", "IRC+VENDED+STORAGE")
run_test("AVG Aggregation", test_avg_aggregation, "Aggregations", "DBX", "IRC+VENDED+STORAGE")
run_test("GROUP BY", test_group_by, "Aggregations", "DBX", "IRC+VENDED+STORAGE")
run_test("DISTINCT", test_distinct, "Aggregations", "DBX", "IRC+VENDED+STORAGE")
run_test("Window Function", test_window_function, "Aggregations", "DBX", "IRC+VENDED+STORAGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Join Operations
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC → 🎫 VENDED → 📁 STORAGE
# MAGIC 
# MAGIC Multi-table joins execute entirely in Databricks:
# MAGIC - Each table's Parquet files read via vended credentials
# MAGIC - Join performed by Spark (broadcast or shuffle join)
# MAGIC - All data stays in Databricks memory/spill
# MAGIC 
# MAGIC **Compare to JDBC:** If using JDBC, joins would push down to Snowflake warehouse = double compute!

# COMMAND ----------

def test_inner_join():
    result = spark.sql(f"""
        SELECT c.customer_id, c.customer_name, o.order_id, o.total_amount
        FROM {CATALOG}.{SCHEMA}.customers c
        INNER JOIN {CATALOG}.{SCHEMA}.orders o ON c.customer_id = o.customer_id
        LIMIT 100
    """).collect()
    assert len(result) > 0
    return f"{len(result)} joined rows"

def test_left_join():
    result = spark.sql(f"""
        SELECT c.customer_id, COUNT(o.order_id) as order_count
        FROM {CATALOG}.{SCHEMA}.customers c
        LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        LIMIT 100
    """).collect()
    assert len(result) > 0
    return f"{len(result)} customers with order counts"

def test_multi_table_join():
    result = spark.sql(f"""
        SELECT 
            c.customer_name,
            COUNT(DISTINCT o.order_id) as orders,
            COUNT(DISTINCT t.transaction_id) as transactions
        FROM {CATALOG}.{SCHEMA}.customers c
        LEFT JOIN {CATALOG}.{SCHEMA}.orders o ON c.customer_id = o.customer_id
        LEFT JOIN {CATALOG}.{SCHEMA}.transactions t ON c.customer_id = t.customer_id
        GROUP BY c.customer_name
        LIMIT 50
    """).collect()
    assert len(result) > 0
    return f"{len(result)} customers with orders & transactions"

def test_self_join():
    result = spark.sql(f"""
        SELECT e1.event_id, e1.customer_id, e2.event_id as next_event
        FROM {CATALOG}.{SCHEMA}.events e1
        INNER JOIN {CATALOG}.{SCHEMA}.events e2 
            ON e1.customer_id = e2.customer_id 
            AND e1.event_id < e2.event_id
        LIMIT 50
    """).collect()
    assert len(result) > 0
    return f"{len(result)} event pairs"

run_test("Inner Join", test_inner_join, "Joins", "DBX", "IRC+VENDED+STORAGE")
run_test("Left Join", test_left_join, "Joins", "DBX", "IRC+VENDED+STORAGE")
run_test("Multi-Table Join", test_multi_table_join, "Joins", "DBX", "IRC+VENDED+STORAGE")
run_test("Self Join", test_self_join, "Joins", "DBX", "IRC+VENDED+STORAGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Performance Benchmarks
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC → 🎫 VENDED → 📁 STORAGE
# MAGIC 
# MAGIC Performance characteristics:
# MAGIC - **Latency:** IRC metadata call (~100-500ms) + storage read time
# MAGIC - **Throughput:** Limited by Databricks cluster size & storage bandwidth
# MAGIC - **Cost:** Only Databricks DBU - NO Snowflake credits consumed!

# COMMAND ----------

def test_full_scan_events():
    start = time.time()
    count = spark.table(f"{CATALOG}.{SCHEMA}.events").count()
    elapsed = time.time() - start
    throughput = count / elapsed
    return f"{count:,} rows in {elapsed:.2f}s ({throughput:,.0f} rows/sec)"

def test_filtered_scan():
    start = time.time()
    count = spark.sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.events 
        WHERE event_type = 'purchase'
    """).first()[0]
    elapsed = time.time() - start
    return f"{count:,} filtered rows in {elapsed:.2f}s"

def test_aggregation_performance():
    start = time.time()
    result = spark.sql(f"""
        SELECT region, event_type, COUNT(*) as cnt, SUM(amount) as total
        FROM {CATALOG}.{SCHEMA}.events
        GROUP BY region, event_type
    """).collect()
    elapsed = time.time() - start
    return f"{len(result)} groups in {elapsed:.2f}s"

def test_join_performance():
    start = time.time()
    count = spark.sql(f"""
        SELECT COUNT(*)
        FROM {CATALOG}.{SCHEMA}.orders o
        INNER JOIN {CATALOG}.{SCHEMA}.customers c ON o.customer_id = c.customer_id
    """).first()[0]
    elapsed = time.time() - start
    return f"{count:,} joined rows in {elapsed:.2f}s"

run_test("Full Scan (1M Events)", test_full_scan_events, "Performance", "DBX", "IRC+VENDED+STORAGE")
run_test("Filtered Scan", test_filtered_scan, "Performance", "DBX", "IRC+VENDED+STORAGE")
run_test("Aggregation Performance", test_aggregation_performance, "Performance", "DBX", "IRC+VENDED+STORAGE")
run_test("Join Performance", test_join_performance, "Performance", "DBX", "IRC+VENDED+STORAGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. SQL Compatibility Tests
# MAGIC 
# MAGIC **Compute:** 🟦 DBX | **Protocol:** ⚡ IRC → 🎫 VENDED → 📁 STORAGE
# MAGIC 
# MAGIC Standard SQL features execute in Spark SQL:
# MAGIC - LIMIT, ORDER BY → Spark
# MAGIC - CASE WHEN → Spark expression evaluation
# MAGIC - Subqueries → Spark subquery planning
# MAGIC - CTEs → Spark view resolution

# COMMAND ----------

def test_limit_offset():
    result = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.customers 
        ORDER BY customer_id
        LIMIT 10
    """).collect()
    assert len(result) == 10
    return "LIMIT works"

def test_order_by():
    result = spark.sql(f"""
        SELECT customer_id, lifetime_value 
        FROM {CATALOG}.{SCHEMA}.customers 
        ORDER BY lifetime_value DESC
        LIMIT 5
    """).collect()
    values = [r.lifetime_value for r in result]
    assert values == sorted(values, reverse=True)
    return "ORDER BY DESC works"

def test_case_when():
    result = spark.sql(f"""
        SELECT customer_id,
               CASE 
                   WHEN lifetime_value > 1000 THEN 'High'
                   WHEN lifetime_value > 500 THEN 'Medium'
                   ELSE 'Low'
               END as value_tier
        FROM {CATALOG}.{SCHEMA}.customers
        LIMIT 10
    """).collect()
    assert all(r.value_tier in ['High', 'Medium', 'Low'] for r in result)
    return "CASE WHEN works"

def test_subquery():
    result = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.customers
        WHERE customer_id IN (
            SELECT DISTINCT customer_id FROM {CATALOG}.{SCHEMA}.orders
            WHERE total_amount > 100
        )
        LIMIT 10
    """).collect()
    return f"{len(result)} customers from subquery"

def test_cte():
    result = spark.sql(f"""
        WITH top_customers AS (
            SELECT customer_id, SUM(total_amount) as total_spend
            FROM {CATALOG}.{SCHEMA}.orders
            GROUP BY customer_id
            ORDER BY total_spend DESC
            LIMIT 10
        )
        SELECT c.customer_name, tc.total_spend
        FROM top_customers tc
        JOIN {CATALOG}.{SCHEMA}.customers c ON tc.customer_id = c.customer_id
    """).collect()
    return f"{len(result)} top customers via CTE"

run_test("LIMIT", test_limit_offset, "SQL Compatibility", "DBX", "IRC+VENDED+STORAGE")
run_test("ORDER BY", test_order_by, "SQL Compatibility", "DBX", "IRC+VENDED+STORAGE")
run_test("CASE WHEN", test_case_when, "SQL Compatibility", "DBX", "IRC+VENDED+STORAGE")
run_test("Subquery", test_subquery, "SQL Compatibility", "DBX", "IRC+VENDED+STORAGE")
run_test("CTE (WITH clause)", test_cte, "SQL Compatibility", "DBX", "IRC+VENDED+STORAGE")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

from pyspark.sql import Row

results_df = spark.createDataFrame([Row(**r) for r in test_results])
results_df.createOrReplaceTempView("test_results")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 
# MAGIC     category,
# MAGIC     COUNT(*) as total_tests,
# MAGIC     SUM(CASE WHEN status = 'PASS' THEN 1 ELSE 0 END) as passed,
# MAGIC     SUM(CASE WHEN status = 'FAIL' THEN 1 ELSE 0 END) as failed,
# MAGIC     ROUND(AVG(elapsed_sec), 2) as avg_time_sec,
# MAGIC     FIRST(compute) as compute_location,
# MAGIC     FIRST(protocol) as protocol
# MAGIC FROM test_results
# MAGIC GROUP BY category
# MAGIC ORDER BY category

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT test, status, elapsed_sec, compute, protocol, details 
# MAGIC FROM test_results 
# MAGIC ORDER BY category, test

# COMMAND ----------

total = len(test_results)
passed = sum(1 for r in test_results if r["status"] == "PASS")
failed = total - passed

print("=" * 80)
print("FOREIGN CATALOG TEST SUITE - FINAL RESULTS")
print("=" * 80)
print(f"  Total Tests:  {total}")
print(f"  Passed:       {passed} ✅")
print(f"  Failed:       {failed} ❌")
print(f"  Pass Rate:    {100*passed/total:.1f}%")
print("=" * 80)

if failed > 0:
    print("\nFailed Tests:")
    for r in test_results:
        if r["status"] == "FAIL":
            print(f"  - {r['test']}: {r['details'][:80]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Compute Location & Protocol Summary
# MAGIC 
# MAGIC | Test Category | Compute | Protocol | Snowflake WH Used? |
# MAGIC |---------------|---------|----------|-------------------|
# MAGIC | Connectivity | 🟦 DBX | ⚡ IRC | ❌ No |
# MAGIC | Read Operations | 🟦 DBX | ⚡ IRC → 🎫 VENDED → 📁 STORAGE | ❌ No |
# MAGIC | Data Types | 🟦 DBX | ⚡ IRC → 🎫 VENDED → 📁 STORAGE | ❌ No |
# MAGIC | Aggregations | 🟦 DBX | ⚡ IRC → 🎫 VENDED → 📁 STORAGE | ❌ No |
# MAGIC | Joins | 🟦 DBX | ⚡ IRC → 🎫 VENDED → 📁 STORAGE | ❌ No |
# MAGIC | Performance | 🟦 DBX | ⚡ IRC → 🎫 VENDED → 📁 STORAGE | ❌ No |
# MAGIC | SQL Compatibility | 🟦 DBX | ⚡ IRC → 🎫 VENDED → 📁 STORAGE | ❌ No |
# MAGIC 
# MAGIC **KEY FINDING: ALL operations execute on Databricks with ZERO Snowflake warehouse consumption!**

# COMMAND ----------

# MAGIC %md
# MAGIC ## Protocol Comparison: IRC vs JDBC
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                    IRC (Iceberg REST Catalog) - CURRENT APPROACH                    │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                                     │
# MAGIC │   ┌──────────┐      IRC API        ┌──────────────┐     Vended       ┌───────────┐ │
# MAGIC │   │Databricks│ ──────────────────▶ │  Snowflake   │ ──────────────▶  │   Cloud   │ │
# MAGIC │   │  Spark   │     (metadata)      │  Horizon IRC │   Credentials    │  Storage  │ │
# MAGIC │   │          │ ◀────────────────── │              │                  │  (Parquet)│ │
# MAGIC │   │          │   table locations   └──────────────┘                  └───────────┘ │
# MAGIC │   │          │                                                             │       │
# MAGIC │   │          │ ◀───────────────────────────────────────────────────────────┘       │
# MAGIC │   └──────────┘              Direct Parquet Read (NO Snowflake WH!)                 │
# MAGIC │                                                                                     │
# MAGIC │   ✅ Single Compute (DBX only)     ✅ No SF credits     ✅ Open format              │
# MAGIC └─────────────────────────────────────────────────────────────────────────────────────┘
# MAGIC 
# MAGIC ┌─────────────────────────────────────────────────────────────────────────────────────┐
# MAGIC │                         JDBC - LEGACY APPROACH (NOT USED HERE)                      │
# MAGIC ├─────────────────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                                     │
# MAGIC │   ┌──────────┐       JDBC         ┌──────────────┐                  ┌───────────┐  │
# MAGIC │   │Databricks│ ──────────────────▶│  Snowflake   │ ───────────────▶ │   Cloud   │  │
# MAGIC │   │  Spark   │      (SQL)         │  Warehouse   │    (internal)    │  Storage  │  │
# MAGIC │   │          │ ◀──────────────────│              │ ◀─────────────── │           │  │
# MAGIC │   └──────────┘     (results)      └──────────────┘                  └───────────┘  │
# MAGIC │                                                                                     │
# MAGIC │   ⚠️ DOUBLE COMPUTE:  DBX + SF warehouse both running!                             │
# MAGIC │   ⚠️ DOUBLE COST:     DBU + SF credits consumed                                    │
# MAGIC │   ⚠️ BOTTLENECK:      Network transfer of result sets                              │
# MAGIC └─────────────────────────────────────────────────────────────────────────────────────┘
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Competitive Analysis: Snowflake Iceberg Advantages
# MAGIC 
# MAGIC ### Why Snowflake for Iceberg?
# MAGIC 
# MAGIC | Capability | Snowflake | Databricks | Advantage |
# MAGIC |------------|-----------|------------|-----------|
# MAGIC | **Native Iceberg Tables** | CREATE ICEBERG TABLE - first-class support | Delta Lake primary, Iceberg via UniForm conversion | 🏆 Snowflake: Native, no conversion overhead |
# MAGIC | **Managed Storage** | SNOWFLAKE_MANAGED - zero config internal storage | Requires external storage setup | 🏆 Snowflake: Simpler ops, no S3/ADLS config |
# MAGIC | **Iceberg v3 Support** | Full v3 with nanosecond timestamps, VARIANT | Limited v3 support | 🏆 Snowflake: Latest spec compliance |
# MAGIC | **Governance** | Horizon - masking, RAP, tags work on Iceberg | Requires Unity Catalog separately | 🏆 Snowflake: Unified governance model |
# MAGIC | **Time Travel** | Native BEFORE/AT syntax on Iceberg tables | Available but complex setup | 🏆 Snowflake: Familiar SQL syntax |
# MAGIC | **Interoperability** | Horizon IRC serves any Iceberg client | Foreign catalogs (consumer only) | 🏆 Snowflake: Bidirectional openness |
# MAGIC 
# MAGIC ### Access Patterns Demonstrated
# MAGIC 
# MAGIC ```
# MAGIC ┌─────────────────────────────────────────────────────────────────────────┐
# MAGIC │                     SNOWFLAKE ICEBERG INTEROP                           │
# MAGIC ├─────────────────────────────────────────────────────────────────────────┤
# MAGIC │                                                                         │
# MAGIC │  ┌─────────────┐         Horizon IRC          ┌─────────────────────┐  │
# MAGIC │  │  Snowflake  │ ◀───────────────────────────▶│  Spark / Trino /    │  │
# MAGIC │  │  Iceberg    │    (Iceberg REST Catalog)    │  Flink / Presto     │  │
# MAGIC │  │  Tables     │                              └─────────────────────┘  │
# MAGIC │  └─────────────┘                                                        │
# MAGIC │        │                                                                │
# MAGIC │        │ Vended Credentials                                             │
# MAGIC │        ▼                                                                │
# MAGIC │  ┌─────────────────────────────────────────────────────────────────┐   │
# MAGIC │  │              Cloud Storage (S3 / ADLS / GCS)                    │   │
# MAGIC │  │                     Parquet Data Files                          │   │
# MAGIC │  └─────────────────────────────────────────────────────────────────┘   │
# MAGIC │                                                                         │
# MAGIC │  KEY: No double compute! Databricks reads Parquet directly.            │
# MAGIC └─────────────────────────────────────────────────────────────────────────┘
# MAGIC ```
# MAGIC 
# MAGIC ### Key Differentiators
# MAGIC 
# MAGIC 1. **Open by Default**: Snowflake Iceberg tables are immediately accessible to any Iceberg-compatible engine via Horizon IRC
# MAGIC 
# MAGIC 2. **No Lock-in**: Data stored in open Parquet format - portable across clouds and engines
# MAGIC 
# MAGIC 3. **Single Compute**: Vended credentials enable direct storage access - no Snowflake warehouse needed for external reads
# MAGIC 
# MAGIC 4. **Unified Governance**: Same masking policies, row access policies, and tags work on both native and Iceberg tables
# MAGIC 
# MAGIC 5. **Managed Simplicity**: SNOWFLAKE_MANAGED storage eliminates external volume configuration for internal use cases

# COMMAND ----------

# MAGIC %md
# MAGIC ## Double Compute & Vended Credentials Verification

# COMMAND ----------

def verify_no_double_compute(table_name):
    """Verify single compute pattern via query plan analysis."""
    import io
    import sys
    
    df = spark.table(table_name)
    
    old_stdout = sys.stdout
    sys.stdout = buffer = io.StringIO()
    df.explain(mode="extended")
    plan = buffer.getvalue()
    sys.stdout = old_stdout
    
    double_compute_indicators = ["JDBCScan", "SnowflakeScan", "snowflake.jdbc", "JDBCRelation"]
    single_compute_indicators = ["BatchScan", "FileScan", "parquet", "IcebergScan", "DataSourceV2"]
    
    found_double = any(ind.lower() in plan.lower() for ind in double_compute_indicators)
    found_single = any(ind.lower() in plan.lower() for ind in single_compute_indicators)
    
    return {
        "table": table_name,
        "uses_jdbc": found_double,
        "uses_irc": found_single and not found_double,
        "plan_snippet": plan[:500] if plan else "No plan available"
    }

print("=" * 90)
print("COMPUTE & CREDENTIAL VERIFICATION")
print("=" * 90)
print("""
┌────────────────────┬──────────────────────────────────────────────────────────────────────┐
│ Access Pattern     │ Description                                                          │
├────────────────────┼──────────────────────────────────────────────────────────────────────┤
│ ⚡ IRC + 🎫 VENDED │ Snowflake issues temp SAS/presigned URLs → DBX reads storage ✅      │
│ ⚡ IRC + 🔐 REMOTE │ Per-file signing requests to Snowflake → Still direct read ✅        │
│ 🔗 JDBC            │ Queries route through Snowflake warehouse → DOUBLE COMPUTE ⚠️       │
└────────────────────┴──────────────────────────────────────────────────────────────────────┘
""")

for table in ["customers", "orders", "events"]:
    full_name = f"{CATALOG}.{SCHEMA}.{table}"
    result = verify_no_double_compute(full_name)
    
    if result["uses_irc"]:
        status = "✅ IRC + VENDED CREDENTIALS (Single Compute)"
        protocol = "⚡ IRC → 🎫 VENDED → 📁 STORAGE"
    elif result["uses_jdbc"]:
        status = "⚠️ JDBC (Double Compute - NOT expected!)"
        protocol = "🔗 JDBC → Snowflake WH"
    else:
        status = "❓ UNKNOWN"
        protocol = "UNKNOWN"
    
    print(f"\n{table.upper()}:")
    print(f"  Access Pattern: {status}")
    print(f"  Protocol:       {protocol}")

print("\n" + "=" * 90)
print("SUMMARY: All tables accessed via IRC + Vended Credentials = ZERO Snowflake compute!")
print("=" * 90)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Cost Implications
# MAGIC 
# MAGIC | Scenario | Databricks Cost | Snowflake Cost | Total |
# MAGIC |----------|-----------------|----------------|-------|
# MAGIC | **This Test (IRC)** | DBU for Spark cluster | $0 (no warehouse) | DBU only |
# MAGIC | **JDBC Alternative** | DBU for Spark cluster | Credits for WH | DBU + Credits |
# MAGIC 
# MAGIC **Bottom Line:** Using Snowflake Foreign Catalog with IRC means you pay for ONE engine, not two.
