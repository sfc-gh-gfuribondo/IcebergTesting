# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake-Databricks Bidirectional Iceberg Interop
# MAGIC 
# MAGIC This notebook demonstrates **bidirectional** Iceberg interoperability:
# MAGIC 
# MAGIC | Direction | Source | Target | Method |
# MAGIC |-----------|--------|--------|--------|
# MAGIC | DBX → SF | Databricks Unity Catalog | Snowflake | CLD (Catalog-Linked Database) |
# MAGIC | SF → DBX | Snowflake Iceberg | Databricks | Horizon IRC (REST Catalog) |
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
# MAGIC ## Architecture
# MAGIC ```
# MAGIC ┌─────────────────┐                    ┌─────────────────┐
# MAGIC │   Databricks    │◄───── CLD ────────│   Snowflake     │
# MAGIC │  Unity Catalog  │                    │  gf_dbx_cld     │
# MAGIC │   (UniForm)     │                    │                 │
# MAGIC │                 │───── IRC ─────────►│  Horizon IRC    │
# MAGIC │                 │                    │  ICEBERG_POC    │
# MAGIC └─────────────────┘                    └─────────────────┘
# MAGIC         │                                      │
# MAGIC         └──────────── Azure ADLS Gen2 ─────────┘
# MAGIC              gfstorageaccountwest2
# MAGIC ```

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part A: Databricks Local Tables (Unity Catalog - UniForm)
# MAGIC 
# MAGIC These tables are stored in Databricks Unity Catalog with UniForm enabled,
# MAGIC making them accessible from Snowflake via CLD (Catalog-Linked Database).

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in gf_interop.uniform (UniForm-enabled healthcare tables)
# MAGIC SHOW TABLES IN gf_interop.uniform

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query local Databricks patients table
# MAGIC SELECT * FROM gf_interop.uniform.patients LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregation on local patients table
# MAGIC SELECT state, COUNT(*) as patient_count 
# MAGIC FROM gf_interop.uniform.patients 
# MAGIC GROUP BY state 
# MAGIC ORDER BY patient_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: Snowflake Tables via Horizon IRC
# MAGIC 
# MAGIC These tables are stored in Snowflake ICEBERG_POC.EXTERNAL_ICEBERG schema
# MAGIC and accessed via Horizon IRC (Iceberg REST Catalog).

# COMMAND ----------

CATALOG = "snowflake_horizon"

try:
    spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show()
    print(f"Catalog {CATALOG} is configured")
except:
    SF_PAT = dbutils.secrets.get(scope="snowflake-irc", key="pat")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.type", "rest")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.uri", 
        "https://sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com/polaris/api/catalog")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.credential", SF_PAT)
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", "ICEBERG_POC")
    print(f"Catalog {CATALOG} configured")

# COMMAND ----------

sf_patients = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.PATIENTS")
print(f"Snowflake PATIENTS: {sf_patients.count()} rows")
sf_patients.show(5)

# COMMAND ----------

spark.sql(f"""
    SELECT risk_tier, COUNT(*) as count
    FROM {CATALOG}.EXTERNAL_ICEBERG.PATIENTS
    GROUP BY risk_tier
    ORDER BY count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C: DBX Local vs Snowflake Foreign Catalog Comparison
# MAGIC 
# MAGIC Compare reading the same healthcare data from two sources:
# MAGIC 1. **DBX Local (UniForm)**: `gf_interop.uniform.patients`
# MAGIC 2. **Snowflake IRC**: `snowflake_horizon.EXTERNAL_ICEBERG.PATIENTS`

# COMMAND ----------

import time

# COMMAND ----------

# MAGIC %md
# MAGIC ### C1: Row Count Comparison

# COMMAND ----------

dbx_patients = spark.table("gf_interop.uniform.patients")
sf_patients = spark.table(f"{CATALOG}.EXTERNAL_ICEBERG.PATIENTS")

dbx_count = dbx_patients.count()
sf_count = sf_patients.count()

print("=" * 70)
print("ROW COUNT COMPARISON: DBX Local vs Snowflake Foreign Catalog")
print("=" * 70)
print(f"  DBX Local (UniForm):     {dbx_count:>10,} rows")
print(f"  Snowflake (IRC):         {sf_count:>10,} rows")
print(f"  Match:                   {'✅ YES' if dbx_count == sf_count else '⚠️ NO'}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C2: Read Performance Comparison

# COMMAND ----------

def benchmark_read(source_name, table_ref, iterations=3):
    """Benchmark read performance for a table."""
    times = []
    for i in range(iterations):
        start = time.time()
        count = spark.table(table_ref).count()
        elapsed = time.time() - start
        times.append(elapsed)
    
    avg_time = sum(times) / len(times)
    return {
        "source": source_name,
        "avg_time_sec": round(avg_time, 3),
        "min_time_sec": round(min(times), 3),
        "max_time_sec": round(max(times), 3),
        "row_count": count
    }

dbx_perf = benchmark_read("DBX Local (UniForm)", "gf_interop.uniform.patients")
sf_perf = benchmark_read("Snowflake (IRC)", f"{CATALOG}.EXTERNAL_ICEBERG.PATIENTS")

print("=" * 80)
print("READ PERFORMANCE COMPARISON: PATIENTS TABLE")
print("=" * 80)
print(f"""
  Source                 | Avg Time | Min Time | Max Time | Rows
  -----------------------|----------|----------|----------|----------
  DBX Local (UniForm)    | {dbx_perf['avg_time_sec']:>6.3f}s  | {dbx_perf['min_time_sec']:>6.3f}s  | {dbx_perf['max_time_sec']:>6.3f}s  | {dbx_perf['row_count']:>8,}
  Snowflake (IRC)        | {sf_perf['avg_time_sec']:>6.3f}s  | {sf_perf['min_time_sec']:>6.3f}s  | {sf_perf['max_time_sec']:>6.3f}s  | {sf_perf['row_count']:>8,}
""")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C3: Aggregation Performance Comparison

# COMMAND ----------

def benchmark_aggregation(source_name, sql_query, iterations=3):
    """Benchmark aggregation query performance."""
    times = []
    for i in range(iterations):
        start = time.time()
        result = spark.sql(sql_query).collect()
        elapsed = time.time() - start
        times.append(elapsed)
    
    avg_time = sum(times) / len(times)
    return {
        "source": source_name,
        "avg_time_sec": round(avg_time, 3),
        "result_rows": len(result)
    }

dbx_agg = benchmark_aggregation(
    "DBX Local",
    "SELECT state, COUNT(*) as cnt FROM gf_interop.uniform.patients GROUP BY state"
)

sf_agg = benchmark_aggregation(
    "Snowflake IRC",
    f"SELECT state, COUNT(*) as cnt FROM {CATALOG}.EXTERNAL_ICEBERG.PATIENTS GROUP BY state"
)

print("=" * 70)
print("AGGREGATION PERFORMANCE: GROUP BY STATE")
print("=" * 70)
print(f"  DBX Local (UniForm):     {dbx_agg['avg_time_sec']:>6.3f}s  ({dbx_agg['result_rows']} groups)")
print(f"  Snowflake (IRC):         {sf_agg['avg_time_sec']:>6.3f}s  ({sf_agg['result_rows']} groups)")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ### C4: Schema Comparison

# COMMAND ----------

print("=" * 70)
print("SCHEMA COMPARISON")
print("=" * 70)
print("\n--- DBX Local (UniForm) Schema ---")
dbx_patients.printSchema()
print("\n--- Snowflake (IRC) Schema ---")
sf_patients.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ### C5: Data Sample Comparison

# COMMAND ----------

print("=" * 70)
print("DATA SAMPLE COMPARISON (First 5 rows)")
print("=" * 70)
print("\n--- DBX Local (UniForm) ---")
dbx_patients.orderBy("patient_id").limit(5).show()

print("\n--- Snowflake (IRC) ---")
sf_patients.orderBy("patient_id").limit(5).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D: Full Healthcare Domain Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### D1: All Tables Count Comparison

# COMMAND ----------

healthcare_tables = ["patients", "encounters", "claims", "medications", "providers"]
comparison_results = []

for table in healthcare_tables:
    try:
        dbx_count = spark.table(f"gf_interop.uniform.{table}").count()
    except:
        dbx_count = -1
    
    try:
        sf_count = spark.table(f"{CATALOG}.EXTERNAL_ICEBERG.{table.upper()}").count()
    except:
        sf_count = -1
    
    comparison_results.append({
        "table": table,
        "dbx_local": dbx_count,
        "sf_irc": sf_count,
        "match": dbx_count == sf_count and dbx_count > 0
    })

print("=" * 80)
print("HEALTHCARE DOMAIN - ROW COUNT COMPARISON")
print("=" * 80)
print(f"""
  Table          | DBX Local     | Snowflake IRC | Match
  ---------------|---------------|---------------|-------""")
for r in comparison_results:
    dbx = f"{r['dbx_local']:>10,}" if r['dbx_local'] > 0 else "    N/A   "
    sf = f"{r['sf_irc']:>10,}" if r['sf_irc'] > 0 else "    N/A   "
    match = "✅" if r['match'] else "⚠️"
    print(f"  {r['table']:<14} | {dbx}    | {sf}    | {match}")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part E: Access Pattern Analysis

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage Endpoint Analysis

# COMMAND ----------

print("=" * 80)
print("ACCESS PATTERN ANALYSIS")
print("=" * 80)
print("""
┌────────────────────────────────────────────────────────────────────────────────┐
│                     DATABRICKS LOCAL (UniForm)                                  │
├────────────────────────────────────────────────────────────────────────────────┤
│  Storage:    abfss://gf-unity@gfstorageaccountwest2.dfs.core.windows.net       │
│  Protocol:   ABFS (Azure Blob File System) - native Databricks                 │
│  Format:     Delta Lake + Iceberg metadata (UniForm)                           │
│  Access:     Direct storage read via Unity Catalog credentials                 │
│  Compute:    🟦 Databricks only                                                │
└────────────────────────────────────────────────────────────────────────────────┘

┌────────────────────────────────────────────────────────────────────────────────┐
│                     SNOWFLAKE (Horizon IRC)                                     │
├────────────────────────────────────────────────────────────────────────────────┤
│  Storage:    azure://gfstorageaccountwest2.blob.core.windows.net/gfwestcontainer1 │
│  Protocol:   Azure Blob - via Snowflake External Volume                        │
│  Format:     Native Iceberg (Parquet data files)                               │
│  Access:     IRC metadata → Vended credentials → Direct storage read           │
│  Compute:    🟦 Databricks only (NO Snowflake warehouse!)                      │
└────────────────────────────────────────────────────────────────────────────────┘

Both access patterns result in SINGLE COMPUTE (Databricks only):
  • DBX Local: Native Unity Catalog access
  • SF IRC: Vended credentials enable direct Parquet reads
""")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part F: Bidirectional Interop Summary

# COMMAND ----------

print("=" * 80)
print("BIDIRECTIONAL ICEBERG INTEROP - SUMMARY")
print("=" * 80)
print("""
┌──────────────────────────────────────────────────────────────────────────────┐
│  DIRECTION: Databricks → Snowflake (CLD)                                     │
├──────────────────────────────────────────────────────────────────────────────┤
│  Method:          Catalog-Linked Database (gf_dbx_cld)                       │
│  Catalog Int:     gf_interop_unity_int                                       │
│  Tables:          patients, encounters, claims, medications, providers       │
│  Format:          UniForm (Delta + Iceberg metadata)                         │
│  Snowflake SQL:   SELECT * FROM gf_dbx_cld.uniform.patients                  │
└──────────────────────────────────────────────────────────────────────────────┘

┌──────────────────────────────────────────────────────────────────────────────┐
│  DIRECTION: Snowflake → Databricks (Horizon IRC)                             │
├──────────────────────────────────────────────────────────────────────────────┤
│  Method:          Iceberg REST Catalog (Foreign Catalog)                     │
│  IRC Endpoint:    https://sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com/polaris/api/catalog │
│  Tables:          PATIENTS, ENCOUNTERS, CLAIMS, MEDICATIONS, PROVIDERS       │
│  Format:          Native Iceberg (Parquet)                                   │
│  Spark SQL:       SELECT * FROM snowflake_horizon.EXTERNAL_ICEBERG.PATIENTS  │
└──────────────────────────────────────────────────────────────────────────────┘

KEY BENEFITS:
  ✅ Same healthcare data accessible from both platforms
  ✅ Open Iceberg format - no proprietary lock-in
  ✅ Single compute for reads (no double compute penalty)
  ✅ Unified governance via Snowflake Horizon / Databricks Unity Catalog
""")
print("=" * 80)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results

# COMMAND ----------

results = []

try:
    dbx_count = spark.sql("SELECT COUNT(*) FROM gf_interop.uniform.patients").collect()[0][0]
    results.append(("DBX Local (patients)", dbx_count, "✅ PASS"))
except Exception as e:
    results.append(("DBX Local (patients)", 0, f"❌ FAIL: {str(e)[:40]}"))

try:
    sf_count = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.PATIENTS").count()
    results.append(("SF via IRC (patients)", sf_count, "✅ PASS"))
except Exception as e:
    results.append(("SF via IRC (patients)", 0, f"❌ FAIL: {str(e)[:40]}"))

try:
    dbx_enc = spark.sql("SELECT COUNT(*) FROM gf_interop.uniform.encounters").collect()[0][0]
    results.append(("DBX Local (encounters)", dbx_enc, "✅ PASS"))
except Exception as e:
    results.append(("DBX Local (encounters)", 0, f"❌ FAIL: {str(e)[:40]}"))

try:
    sf_enc = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.ENCOUNTERS").count()
    results.append(("SF via IRC (encounters)", sf_enc, "✅ PASS"))
except Exception as e:
    results.append(("SF via IRC (encounters)", 0, f"❌ FAIL: {str(e)[:40]}"))

print("=" * 70)
print("BIDIRECTIONAL INTEROP TEST RESULTS - HEALTHCARE DOMAIN")
print("=" * 70)
for test, count, status in results:
    print(f"  {test:<25}: {count:>10,} rows - {status}")
print("=" * 70)
