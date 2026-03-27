# Databricks notebook source
# MAGIC %md
# MAGIC # Create Iceberg v3 Healthcare Tables
# MAGIC 
# MAGIC This notebook creates healthcare tables in `gf_dbx.icebergv3` schema with **Iceberg v3** enabled.
# MAGIC 
# MAGIC **Requirements:**
# MAGIC - Databricks Runtime 17.3+ (using 18.0)
# MAGIC - Iceberg v3 Beta feature enabled
# MAGIC 
# MAGIC **Key Difference from v2:**
# MAGIC - Uses `delta.enableIcebergCompatV3 = 'true'` instead of V2
# MAGIC - Supports VARIANT data type and deletion vectors

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG gf_dbx;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS icebergv3
# MAGIC COMMENT 'Healthcare tables with Iceberg v3 UniForm enabled';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA icebergv3;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Healthcare Tables with Iceberg v3

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PATIENTS table
# MAGIC CREATE OR REPLACE TABLE patients (
# MAGIC   patient_id BIGINT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   date_of_birth DATE,
# MAGIC   gender STRING,
# MAGIC   blood_type STRING,
# MAGIC   primary_phone STRING,
# MAGIC   city STRING,
# MAGIC   state STRING,
# MAGIC   insurance_plan STRING
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV3' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC )
# MAGIC COMMENT 'Patient demographics - Iceberg v3';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- ENCOUNTERS table
# MAGIC CREATE OR REPLACE TABLE encounters (
# MAGIC   encounter_id BIGINT,
# MAGIC   patient_id BIGINT,
# MAGIC   provider_id BIGINT,
# MAGIC   encounter_date DATE,
# MAGIC   encounter_type STRING,
# MAGIC   primary_diagnosis_code STRING,
# MAGIC   primary_diagnosis_desc STRING,
# MAGIC   disposition STRING,
# MAGIC   total_charge DECIMAL(10,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV3' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC )
# MAGIC COMMENT 'Clinical encounters - Iceberg v3';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- CLAIMS table
# MAGIC CREATE OR REPLACE TABLE claims (
# MAGIC   claim_id BIGINT,
# MAGIC   encounter_id BIGINT,
# MAGIC   patient_id BIGINT,
# MAGIC   payer_name STRING,
# MAGIC   claim_status STRING,
# MAGIC   submitted_date DATE,
# MAGIC   paid_date DATE,
# MAGIC   billed_amount DECIMAL(10,2),
# MAGIC   allowed_amount DECIMAL(10,2),
# MAGIC   paid_amount DECIMAL(10,2)
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV3' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC )
# MAGIC COMMENT 'Insurance claims - Iceberg v3';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- MEDICATIONS table
# MAGIC CREATE OR REPLACE TABLE medications (
# MAGIC   medication_id BIGINT,
# MAGIC   patient_id BIGINT,
# MAGIC   encounter_id BIGINT,
# MAGIC   drug_name STRING,
# MAGIC   ndc_code STRING,
# MAGIC   dosage STRING,
# MAGIC   frequency STRING,
# MAGIC   prescribed_date DATE,
# MAGIC   end_date DATE,
# MAGIC   prescribing_provider_id BIGINT
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV3' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC )
# MAGIC COMMENT 'Medication records - Iceberg v3';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- PROVIDERS table
# MAGIC CREATE OR REPLACE TABLE providers (
# MAGIC   provider_id BIGINT,
# MAGIC   first_name STRING,
# MAGIC   last_name STRING,
# MAGIC   specialty STRING,
# MAGIC   npi_number STRING,
# MAGIC   facility_name STRING,
# MAGIC   facility_city STRING,
# MAGIC   facility_state STRING,
# MAGIC   accepting_patients BOOLEAN
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC   'delta.enableIcebergCompatV3' = 'true',
# MAGIC   'delta.universalFormat.enabledFormats' = 'iceberg'
# MAGIC )
# MAGIC COMMENT 'Healthcare providers - Iceberg v3';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Populate from Existing Uniform Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gf_dbx.icebergv3.patients 
# MAGIC SELECT * FROM gf_dbx.uniform.patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gf_dbx.icebergv3.encounters 
# MAGIC SELECT * FROM gf_dbx.uniform.encounters;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gf_dbx.icebergv3.claims 
# MAGIC SELECT * FROM gf_dbx.uniform.claims;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gf_dbx.icebergv3.medications 
# MAGIC SELECT * FROM gf_dbx.uniform.medications;

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO gf_dbx.icebergv3.providers 
# MAGIC SELECT * FROM gf_dbx.uniform.providers;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Verify Row Counts

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT 'patients' AS table_name, COUNT(*) AS row_count FROM gf_dbx.icebergv3.patients
# MAGIC UNION ALL
# MAGIC SELECT 'encounters', COUNT(*) FROM gf_dbx.icebergv3.encounters
# MAGIC UNION ALL
# MAGIC SELECT 'claims', COUNT(*) FROM gf_dbx.icebergv3.claims
# MAGIC UNION ALL
# MAGIC SELECT 'medications', COUNT(*) FROM gf_dbx.icebergv3.medications
# MAGIC UNION ALL
# MAGIC SELECT 'providers', COUNT(*) FROM gf_dbx.icebergv3.providers
# MAGIC ORDER BY table_name;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Iceberg v3 Properties

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES gf_dbx.icebergv3.patients;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED gf_dbx.icebergv3.patients;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Compare v2 vs v3 Properties

# COMMAND ----------

print("=== Iceberg v2 (uniform schema) ===")
v2_props = spark.sql("SHOW TBLPROPERTIES gf_dbx.uniform.patients").filter("key LIKE '%iceberg%' OR key LIKE '%universal%'")
v2_props.show(truncate=False)

print("\n=== Iceberg v3 (icebergv3 schema) ===")
v3_props = spark.sql("SHOW TBLPROPERTIES gf_dbx.icebergv3.patients").filter("key LIKE '%iceberg%' OR key LIKE '%universal%'")
v3_props.show(truncate=False)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Summary
# MAGIC 
# MAGIC | Schema | Iceberg Version | Key Property |
# MAGIC |--------|-----------------|--------------|
# MAGIC | `gf_dbx.uniform` | v2 | `delta.enableIcebergCompatV2 = true` |
# MAGIC | `gf_dbx.icebergv3` | v3 | `delta.enableIcebergCompatV3 = true` |
# MAGIC 
# MAGIC **Iceberg v3 Features:**
# MAGIC - Deletion vectors support
# MAGIC - VARIANT data type support
# MAGIC - Row lineage tracking
