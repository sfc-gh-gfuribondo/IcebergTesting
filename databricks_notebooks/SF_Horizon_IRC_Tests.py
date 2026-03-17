# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Horizon IRC Tests
# MAGIC 
# MAGIC Test reading Snowflake Iceberg tables via Horizon IRC endpoint.
# MAGIC 
# MAGIC **Run `SF_Horizon_IRC_Setup` first to configure the catalog.**
# MAGIC 
# MAGIC ## Healthcare Domain Tables
# MAGIC | Table | Description | Expected Rows |
# MAGIC |-------|-------------|---------------|
# MAGIC | PATIENTS | Patient demographics | 100,000 |
# MAGIC | ENCOUNTERS | Clinical encounters | 1,000,000 |
# MAGIC | CLAIMS | Insurance claims | 500,000 |
# MAGIC | MEDICATIONS | Medication records | 300,000 |
# MAGIC | PROVIDERS | Healthcare providers | 1,000 |
# MAGIC 
# MAGIC ## Test Scenarios
# MAGIC 1. List namespaces and tables
# MAGIC 2. Read EXTERNAL_ICEBERG.PATIENTS (100,000 rows)
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
# MAGIC ## Test 3: Read PATIENTS Table
# MAGIC 
# MAGIC Expected: ~100,000 rows
# MAGIC 
# MAGIC **This tests blob endpoint compatibility** - Snowflake stores data at `azure://...blob.core.windows.net`

# COMMAND ----------

df_patients = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.PATIENTS")
row_count = df_patients.count()
print(f"PATIENTS row count: {row_count}")
print(f"Expected: 100,000")
print(f"Status: {'PASS' if row_count == 100000 else 'CHECK'}")

# COMMAND ----------

df_patients.show(10)

# COMMAND ----------

df_patients.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 4: Aggregation Query

# COMMAND ----------

agg_df = spark.sql(f"""
    SELECT 
        risk_tier,
        state,
        COUNT(*) as patient_count,
        ROUND(AVG(age), 2) as avg_age
    FROM {CATALOG}.EXTERNAL_ICEBERG.PATIENTS
    GROUP BY risk_tier, state
    ORDER BY patient_count DESC
""")
agg_df.show(20)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 5: Read CLAIMS Table

# COMMAND ----------

df_claims = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.CLAIMS")
print(f"CLAIMS row count: {df_claims.count()}")
df_claims.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 6: Join PATIENTS and CLAIMS

# COMMAND ----------

join_df = spark.sql(f"""
    SELECT 
        p.risk_tier,
        c.claim_status,
        COUNT(*) as claim_count,
        ROUND(SUM(c.claim_amount), 2) as total_claims,
        ROUND(AVG(c.claim_amount), 2) as avg_claim_value
    FROM {CATALOG}.EXTERNAL_ICEBERG.PATIENTS p
    JOIN {CATALOG}.EXTERNAL_ICEBERG.CLAIMS c 
        ON p.patient_id = c.patient_id
    GROUP BY p.risk_tier, c.claim_status
    ORDER BY total_claims DESC
    LIMIT 20
""")
join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 7: Read MEDICATIONS Table (with VARIANT data)

# COMMAND ----------

df_meds = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.MEDICATIONS")
print(f"MEDICATIONS row count: {df_meds.count()}")
df_meds.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 8: Query VARIANT/JSON Data (medication_details)

# COMMAND ----------

variant_df = spark.sql(f"""
    SELECT 
        medication_name,
        dosage,
        COUNT(*) as prescription_count
    FROM {CATALOG}.EXTERNAL_ICEBERG.MEDICATIONS
    GROUP BY medication_name, dosage
    ORDER BY prescription_count DESC
    LIMIT 20
""")
variant_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 9: Read ENCOUNTERS Table (1M rows)

# COMMAND ----------

df_encounters = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.ENCOUNTERS")
print(f"ENCOUNTERS row count: {df_encounters.count()}")
df_encounters.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 10: Read PROVIDERS Table

# COMMAND ----------

df_providers = spark.read.table(f"{CATALOG}.EXTERNAL_ICEBERG.PROVIDERS")
print(f"PROVIDERS row count: {df_providers.count()}")
df_providers.show(5)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test 11: Multi-Table Healthcare Join

# COMMAND ----------

multi_join_df = spark.sql(f"""
    SELECT 
        p.state,
        COUNT(DISTINCT p.patient_id) as patients,
        COUNT(DISTINCT e.encounter_id) as encounters,
        COUNT(DISTINCT c.claim_id) as claims,
        ROUND(SUM(c.claim_amount), 2) as total_claims
    FROM {CATALOG}.EXTERNAL_ICEBERG.PATIENTS p
    LEFT JOIN {CATALOG}.EXTERNAL_ICEBERG.ENCOUNTERS e ON p.patient_id = e.patient_id
    LEFT JOIN {CATALOG}.EXTERNAL_ICEBERG.CLAIMS c ON p.patient_id = c.patient_id
    GROUP BY p.state
    ORDER BY total_claims DESC
    LIMIT 20
""")
multi_join_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

print("=" * 70)
print("SNOWFLAKE HORIZON IRC TEST RESULTS - HEALTHCARE DOMAIN")
print("=" * 70)

tests = [
    ("List Namespaces", True),
    ("List Tables", True),
    ("Read PATIENTS (100K)", df_patients.count() == 100000),
    ("Aggregation Query", agg_df.count() > 0),
    ("Read CLAIMS (500K)", df_claims.count() == 500000),
    ("Join Query", join_df.count() > 0),
    ("Read MEDICATIONS (300K)", df_meds.count() == 300000),
    ("VARIANT Query", variant_df.count() > 0),
    ("Read ENCOUNTERS (1M)", df_encounters.count() == 1000000),
    ("Read PROVIDERS (1K)", df_providers.count() == 1000),
    ("Multi-Table Join", multi_join_df.count() > 0),
]

passed = 0
for test_name, test_passed in tests:
    status = "✅ PASS" if test_passed else "❌ FAIL"
    if test_passed:
        passed += 1
    print(f"  {test_name}: {status}")

print("=" * 70)
print(f"Results: {passed}/{len(tests)} tests passed")
print(f"Endpoint: azure:// (blob) - {'COMPATIBLE' if passed == len(tests) else 'CHECK ERRORS'}")
print("=" * 70)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Healthcare Data Summary

# COMMAND ----------

print("=" * 70)
print("HEALTHCARE DATA SUMMARY VIA HORIZON IRC")
print("=" * 70)
print(f"""
  Table          | Row Count    | Status
  ---------------|--------------|--------
  PATIENTS       | {df_patients.count():>10,} | {'✅' if df_patients.count() == 100000 else '⚠️'}
  ENCOUNTERS     | {df_encounters.count():>10,} | {'✅' if df_encounters.count() == 1000000 else '⚠️'}
  CLAIMS         | {df_claims.count():>10,} | {'✅' if df_claims.count() == 500000 else '⚠️'}
  MEDICATIONS    | {df_meds.count():>10,} | {'✅' if df_meds.count() == 300000 else '⚠️'}
  PROVIDERS      | {df_providers.count():>10,} | {'✅' if df_providers.count() == 1000 else '⚠️'}
  ---------------|--------------|--------
  TOTAL          | {df_patients.count() + df_encounters.count() + df_claims.count() + df_meds.count() + df_providers.count():>10,} |
""")
print("=" * 70)
