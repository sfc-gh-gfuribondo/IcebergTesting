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
# MAGIC ## Architecture
# MAGIC ```
# MAGIC ┌─────────────────┐                    ┌─────────────────┐
# MAGIC │   Databricks    │◄───── CLD ────────│   Snowflake     │
# MAGIC │  Unity Catalog  │                    │  gf_interop_cld │
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
# MAGIC ## Part A: Databricks Local Tables (Unity Catalog)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- List tables in gf_interop.uniform (UniForm-enabled)
# MAGIC SHOW TABLES IN gf_interop.uniform

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Query local Databricks table
# MAGIC SELECT * FROM gf_interop.uniform.patients LIMIT 5

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Aggregation on local table
# MAGIC SELECT state, COUNT(*) as patient_count 
# MAGIC FROM gf_interop.uniform.patients 
# MAGIC GROUP BY state 
# MAGIC ORDER BY patient_count DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part B: Snowflake Tables via Horizon IRC

# COMMAND ----------

# Configure Snowflake Horizon IRC (run SF_Horizon_IRC_Setup first, or configure here)
CATALOG = "snowflake_horizon"

try:
    # Check if catalog is already configured
    spark.sql(f"SHOW NAMESPACES IN {CATALOG}").show()
    print(f"Catalog {CATALOG} is configured")
except:
    # Configure if not already done
    SF_PAT = dbutils.secrets.get(scope="snowflake-irc", key="pat")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}", "org.apache.iceberg.spark.SparkCatalog")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.type", "rest")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.uri", 
        "https://sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com/polaris/api/catalog")
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.credential", SF_PAT)
    spark.conf.set(f"spark.sql.catalog.{CATALOG}.warehouse", "ICEBERG_POC")
    print(f"Catalog {CATALOG} configured")

# COMMAND ----------

# Query Snowflake Iceberg table via IRC
sf_customers = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS")
print(f"Snowflake CUSTOMERS: {sf_customers.count()} rows")
sf_customers.show(5)

# COMMAND ----------

# Aggregation on Snowflake table
spark.sql(f"""
    SELECT customer_tier, COUNT(*) as count
    FROM {CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS
    GROUP BY customer_tier
    ORDER BY count DESC
""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part C: Cross-Platform Comparison

# COMMAND ----------

# MAGIC %md
# MAGIC ### Compare Databricks vs Snowflake Data

# COMMAND ----------

# Databricks local patient count by state
dbx_states = spark.sql("""
    SELECT 'Databricks' as source, state, COUNT(*) as count
    FROM gf_interop.uniform.patients
    GROUP BY state
    ORDER BY count DESC
""")
dbx_states.show()

# COMMAND ----------

# Snowflake customer count by region
sf_regions = spark.sql(f"""
    SELECT 'Snowflake' as source, region, COUNT(*) as count
    FROM {CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS
    GROUP BY region
    ORDER BY count DESC
""")
sf_regions.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Part D: Endpoint Compatibility Summary

# COMMAND ----------

# MAGIC %md
# MAGIC ### Storage Endpoint Analysis

# COMMAND ----------

print("=" * 70)
print("ENDPOINT COMPATIBILITY ANALYSIS")
print("=" * 70)
print()
print("Databricks Unity Catalog (UniForm):")
print("  - Uses: abfss://gf-unity@gfstorageaccountwest2.blob.core.windows.net")
print("  - Protocol: ABFS (Azure Blob File System)")
print("  - Snowflake CLD: WORKS (reads abfss:// paths)")
print()
print("Snowflake Iceberg (EXVOL):")
print("  - Uses: azure://gfstorageaccountwest2.blob.core.windows.net")
print("  - Protocol: Azure Blob")
print("  - Databricks IRC: TBD (may need storage key or path translation)")
print()
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results

# COMMAND ----------

results = []

# Test DBX local
try:
    dbx_count = spark.sql("SELECT COUNT(*) FROM gf_interop.uniform.patients").collect()[0][0]
    results.append(("DBX Local (patients)", dbx_count, "PASS"))
except Exception as e:
    results.append(("DBX Local (patients)", 0, f"FAIL: {e}"))

# Test Snowflake via IRC
try:
    sf_count = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.CUSTOMERS").count()
    results.append(("SF via IRC (customers)", sf_count, "PASS"))
except Exception as e:
    results.append(("SF via IRC (customers)", 0, f"FAIL: {e}"))

print("=" * 70)
print("BIDIRECTIONAL INTEROP TEST RESULTS")
print("=" * 70)
for test, count, status in results:
    print(f"  {test}: {count} rows - {status}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Notes
# MAGIC 
# MAGIC **Snowflake CLD (Catalog-Linked Database):**
# MAGIC - Snowflake reads Databricks UniForm tables via `gf_interop_unity_int` catalog integration
# MAGIC - 5 tables synced: patients, providers, encounters, claims, medications
# MAGIC - All queries work including joins and aggregations
# MAGIC 
# MAGIC **Databricks Horizon IRC:**
# MAGIC - Databricks reads Snowflake Iceberg tables via REST Catalog
# MAGIC - Endpoint: `https://sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com/polaris/api/catalog`
# MAGIC - 5 tables available: CUSTOMERS, EVENTS, ORDERS, PRODUCTS, TRANSACTIONS
# MAGIC - May require storage key configuration if vended credentials don't work with blob endpoint
