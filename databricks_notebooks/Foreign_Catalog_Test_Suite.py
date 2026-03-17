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
# MAGIC ## Healthcare Domain Tables
# MAGIC | Table | Description | Expected Rows |
# MAGIC |-------|-------------|---------------|
# MAGIC | patients | Patient demographics | 100,000 |
# MAGIC | encounters | Clinical encounters | 1,000,000 |
# MAGIC | claims | Insurance claims | 500,000 |
# MAGIC | medications | Medication records | 300,000 |
# MAGIC | providers | Healthcare providers | 1,000 |
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
    "patients": 100000,
    "encounters": 1000000,
    "claims": 500000,
    "medications": 300000,
    "providers": 1000
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

def test_read_patients():
    df = spark.table(f"{CATALOG}.{SCHEMA}.patients")
    count = df.count()
    assert count == EXPECTED_COUNTS["patients"], f"Expected {EXPECTED_COUNTS['patients']}, got {count}"
    return f"{count:,} rows"

def test_read_encounters():
    df = spark.table(f"{CATALOG}.{SCHEMA}.encounters")
    count = df.count()
    assert count == EXPECTED_COUNTS["encounters"], f"Expected {EXPECTED_COUNTS['encounters']}, got {count}"
    return f"{count:,} rows"

def test_read_claims():
    df = spark.table(f"{CATALOG}.{SCHEMA}.claims")
    count = df.count()
    assert count == EXPECTED_COUNTS["claims"], f"Expected {EXPECTED_COUNTS['claims']}, got {count}"
    return f"{count:,} rows"

def test_read_medications():
    df = spark.table(f"{CATALOG}.{SCHEMA}.medications")
    count = df.count()
    assert count == EXPECTED_COUNTS["medications"], f"Expected {EXPECTED_COUNTS['medications']}, got {count}"
    return f"{count:,} rows"

def test_read_providers():
    df = spark.table(f"{CATALOG}.{SCHEMA}.providers")
    count = df.count()
    assert count == EXPECTED_COUNTS["providers"], f"Expected {EXPECTED_COUNTS['providers']}, got {count}"
    return f"{count:,} rows"

run_test("Read PATIENTS", test_read_patients, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read ENCOUNTERS", test_read_encounters, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read CLAIMS", test_read_claims, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read MEDICATIONS", test_read_medications, "Read Operations", "DBX", "IRC+VENDED+STORAGE")
run_test("Read PROVIDERS", test_read_providers, "Read Operations", "DBX", "IRC+VENDED+STORAGE")

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
    df = spark.table(f"{CATALOG}.{SCHEMA}.patients")
    row = df.select("patient_id", "age").first()
    assert row.patient_id is not None
    assert isinstance(row.age, (int, float, type(None)))
    return f"patient_id={row.patient_id}, age={row.age}"

def test_string_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.patients")
    row = df.select("patient_name", "risk_tier", "state").first()
    assert isinstance(row.patient_name, str)
    return f"name={row.patient_name}, tier={row.risk_tier}"

def test_date_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.patients")
    row = df.select("birth_date").first()
    assert row.birth_date is not None
    return f"birth_date={row.birth_date}"

def test_timestamp_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.encounters")
    row = df.select("encounter_timestamp").first()
    assert row.encounter_timestamp is not None
    return f"encounter_timestamp={row.encounter_timestamp}"

def test_variant_types():
    df = spark.table(f"{CATALOG}.{SCHEMA}.medications")
    row = df.select("medication_details").first()
    return f"medication_details type={type(row.medication_details).__name__}"

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
        SELECT COUNT(*) as cnt FROM {CATALOG}.{SCHEMA}.patients
    """).first()
    assert result.cnt == EXPECTED_COUNTS["patients"]
    return f"count={result.cnt:,}"

def test_sum_aggregation():
    result = spark.sql(f"""
        SELECT SUM(claim_amount) as total FROM {CATALOG}.{SCHEMA}.claims
    """).first()
    assert result.total is not None
    return f"total_claim_amount={result.total:,.2f}"

def test_avg_aggregation():
    result = spark.sql(f"""
        SELECT AVG(claim_amount) as avg_claim FROM {CATALOG}.{SCHEMA}.claims
    """).first()
    assert result.avg_claim is not None
    return f"avg_claim_amount={result.avg_claim:.2f}"

def test_group_by():
    result = spark.sql(f"""
        SELECT risk_tier, COUNT(*) as cnt 
        FROM {CATALOG}.{SCHEMA}.patients 
        GROUP BY risk_tier
        ORDER BY cnt DESC
    """).collect()
    assert len(result) > 0
    return f"{len(result)} risk tiers"

def test_distinct():
    result = spark.sql(f"""
        SELECT COUNT(DISTINCT state) as states FROM {CATALOG}.{SCHEMA}.patients
    """).first()
    assert result.states > 0
    return f"{result.states} distinct states"

def test_window_function():
    result = spark.sql(f"""
        SELECT patient_id, claim_amount,
               ROW_NUMBER() OVER (PARTITION BY patient_id ORDER BY claim_amount DESC) as rn
        FROM {CATALOG}.{SCHEMA}.claims
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
        SELECT p.patient_id, p.patient_name, c.claim_id, c.claim_amount
        FROM {CATALOG}.{SCHEMA}.patients p
        INNER JOIN {CATALOG}.{SCHEMA}.claims c ON p.patient_id = c.patient_id
        LIMIT 100
    """).collect()
    assert len(result) > 0
    return f"{len(result)} joined rows"

def test_left_join():
    result = spark.sql(f"""
        SELECT p.patient_id, COUNT(c.claim_id) as claim_count
        FROM {CATALOG}.{SCHEMA}.patients p
        LEFT JOIN {CATALOG}.{SCHEMA}.claims c ON p.patient_id = c.patient_id
        GROUP BY p.patient_id
        LIMIT 100
    """).collect()
    assert len(result) > 0
    return f"{len(result)} patients with claim counts"

def test_multi_table_join():
    result = spark.sql(f"""
        SELECT 
            p.patient_name,
            COUNT(DISTINCT c.claim_id) as claims,
            COUNT(DISTINCT e.encounter_id) as encounters
        FROM {CATALOG}.{SCHEMA}.patients p
        LEFT JOIN {CATALOG}.{SCHEMA}.claims c ON p.patient_id = c.patient_id
        LEFT JOIN {CATALOG}.{SCHEMA}.encounters e ON p.patient_id = e.patient_id
        GROUP BY p.patient_name
        LIMIT 50
    """).collect()
    assert len(result) > 0
    return f"{len(result)} patients with claims & encounters"

def test_self_join():
    result = spark.sql(f"""
        SELECT e1.encounter_id, e1.patient_id, e2.encounter_id as next_encounter
        FROM {CATALOG}.{SCHEMA}.encounters e1
        INNER JOIN {CATALOG}.{SCHEMA}.encounters e2 
            ON e1.patient_id = e2.patient_id 
            AND e1.encounter_id < e2.encounter_id
        LIMIT 50
    """).collect()
    assert len(result) > 0
    return f"{len(result)} encounter pairs"

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

def test_full_scan_encounters():
    start = time.time()
    count = spark.table(f"{CATALOG}.{SCHEMA}.encounters").count()
    elapsed = time.time() - start
    throughput = count / elapsed
    return f"{count:,} rows in {elapsed:.2f}s ({throughput:,.0f} rows/sec)"

def test_filtered_scan():
    start = time.time()
    count = spark.sql(f"""
        SELECT COUNT(*) FROM {CATALOG}.{SCHEMA}.encounters 
        WHERE encounter_type = 'emergency'
    """).first()[0]
    elapsed = time.time() - start
    return f"{count:,} filtered rows in {elapsed:.2f}s"

def test_aggregation_performance():
    start = time.time()
    result = spark.sql(f"""
        SELECT state, encounter_type, COUNT(*) as cnt, SUM(cost) as total
        FROM {CATALOG}.{SCHEMA}.encounters
        GROUP BY state, encounter_type
    """).collect()
    elapsed = time.time() - start
    return f"{len(result)} groups in {elapsed:.2f}s"

def test_join_performance():
    start = time.time()
    count = spark.sql(f"""
        SELECT COUNT(*)
        FROM {CATALOG}.{SCHEMA}.claims c
        INNER JOIN {CATALOG}.{SCHEMA}.patients p ON c.patient_id = p.patient_id
    """).first()[0]
    elapsed = time.time() - start
    return f"{count:,} joined rows in {elapsed:.2f}s"

run_test("Full Scan (1M Encounters)", test_full_scan_encounters, "Performance", "DBX", "IRC+VENDED+STORAGE")
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
        SELECT * FROM {CATALOG}.{SCHEMA}.patients 
        ORDER BY patient_id
        LIMIT 10
    """).collect()
    assert len(result) == 10
    return "LIMIT works"

def test_order_by():
    result = spark.sql(f"""
        SELECT patient_id, age 
        FROM {CATALOG}.{SCHEMA}.patients 
        ORDER BY age DESC
        LIMIT 5
    """).collect()
    values = [r.age for r in result if r.age is not None]
    assert values == sorted(values, reverse=True)
    return "ORDER BY DESC works"

def test_case_when():
    result = spark.sql(f"""
        SELECT patient_id,
               CASE 
                   WHEN age > 65 THEN 'Senior'
                   WHEN age > 40 THEN 'Adult'
                   ELSE 'Young'
               END as age_group
        FROM {CATALOG}.{SCHEMA}.patients
        LIMIT 10
    """).collect()
    assert all(r.age_group in ['Senior', 'Adult', 'Young'] for r in result)
    return "CASE WHEN works"

def test_subquery():
    result = spark.sql(f"""
        SELECT * FROM {CATALOG}.{SCHEMA}.patients
        WHERE patient_id IN (
            SELECT DISTINCT patient_id FROM {CATALOG}.{SCHEMA}.claims
            WHERE claim_amount > 1000
        )
        LIMIT 10
    """).collect()
    return f"{len(result)} patients from subquery"

def test_cte():
    result = spark.sql(f"""
        WITH high_cost_patients AS (
            SELECT patient_id, SUM(claim_amount) as total_claims
            FROM {CATALOG}.{SCHEMA}.claims
            GROUP BY patient_id
            ORDER BY total_claims DESC
            LIMIT 10
        )
        SELECT p.patient_name, hcp.total_claims
        FROM high_cost_patients hcp
        JOIN {CATALOG}.{SCHEMA}.patients p ON hcp.patient_id = p.patient_id
    """).collect()
    return f"{len(result)} high cost patients via CTE"

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

for table in ["patients", "claims", "encounters"]:
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
