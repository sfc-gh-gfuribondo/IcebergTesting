---
name: "create-dbx-icebergv3-schema"
created: "2026-03-17T15:11:48.402Z"
status: pending
---

# Plan: Create Databricks Iceberg v3 Schema

## Overview

Create a new `gf_dbx.icebergv3` schema with 5 healthcare tables using Delta UniForm with Iceberg v3 enabled (`delta.enableIcebergCompatV3 = 'true'`).

## Prerequisites

- Databricks Runtime 17.3+ cluster (required for Iceberg v3)
- Iceberg v3 Beta feature enabled in workspace

## Approach

Use Databricks CLI to execute SQL statements via `databricks sql execute` or create a notebook to run on a DBR 17.3+ cluster.

---

## Task 1: Create Schema

```
CREATE SCHEMA IF NOT EXISTS gf_dbx.icebergv3
COMMENT 'Healthcare tables with Iceberg v3 UniForm enabled';
```

---

## Task 2: Create Tables with Iceberg v3

### patients

```
CREATE OR REPLACE TABLE gf_dbx.icebergv3.patients (
  patient_id BIGINT,
  first_name STRING,
  last_name STRING,
  date_of_birth DATE,
  gender STRING,
  blood_type STRING,
  primary_phone STRING,
  city STRING,
  state STRING,
  insurance_plan STRING
)
TBLPROPERTIES (
  'delta.enableIcebergCompatV3' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

### encounters

```
CREATE OR REPLACE TABLE gf_dbx.icebergv3.encounters (
  encounter_id BIGINT,
  patient_id BIGINT,
  provider_id BIGINT,
  encounter_date DATE,
  encounter_type STRING,
  primary_diagnosis_code STRING,
  primary_diagnosis_desc STRING,
  disposition STRING,
  total_charge DECIMAL(10,2)
)
TBLPROPERTIES (
  'delta.enableIcebergCompatV3' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

### claims

```
CREATE OR REPLACE TABLE gf_dbx.icebergv3.claims (
  claim_id BIGINT,
  encounter_id BIGINT,
  patient_id BIGINT,
  payer_name STRING,
  claim_status STRING,
  submitted_date DATE,
  paid_date DATE,
  billed_amount DECIMAL(10,2),
  allowed_amount DECIMAL(10,2),
  paid_amount DECIMAL(10,2)
)
TBLPROPERTIES (
  'delta.enableIcebergCompatV3' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

### medications

```
CREATE OR REPLACE TABLE gf_dbx.icebergv3.medications (
  medication_id BIGINT,
  patient_id BIGINT,
  encounter_id BIGINT,
  drug_name STRING,
  ndc_code STRING,
  dosage STRING,
  frequency STRING,
  prescribed_date DATE,
  end_date DATE,
  prescribing_provider_id BIGINT
)
TBLPROPERTIES (
  'delta.enableIcebergCompatV3' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

### providers

```
CREATE OR REPLACE TABLE gf_dbx.icebergv3.providers (
  provider_id BIGINT,
  first_name STRING,
  last_name STRING,
  specialty STRING,
  npi_number STRING,
  facility_name STRING,
  facility_city STRING,
  facility_state STRING,
  accepting_patients BOOLEAN
)
TBLPROPERTIES (
  'delta.enableIcebergCompatV3' = 'true',
  'delta.universalFormat.enabledFormats' = 'iceberg'
);
```

---

## Task 3: Populate Tables

Copy data from existing `gf_dbx.uniform` schema:

```
INSERT INTO gf_dbx.icebergv3.patients SELECT * FROM gf_dbx.uniform.patients;
INSERT INTO gf_dbx.icebergv3.encounters SELECT * FROM gf_dbx.uniform.encounters;
INSERT INTO gf_dbx.icebergv3.claims SELECT * FROM gf_dbx.uniform.claims;
INSERT INTO gf_dbx.icebergv3.medications SELECT * FROM gf_dbx.uniform.medications;
INSERT INTO gf_dbx.icebergv3.providers SELECT * FROM gf_dbx.uniform.providers;
```

---

## Task 4: Verify Iceberg v3 Properties

```
SHOW TBLPROPERTIES gf_dbx.icebergv3.patients;
-- Should show: delta.enableIcebergCompatV3 = true
```

Or via CLI:

```
databricks tables get gf_dbx.icebergv3.patients -o json | grep -i iceberg
```

---

## Execution Options

**Option A: Databricks Notebook** (Recommended)

- Requires DBR 17.3+ cluster
- Create notebook with SQL cells and execute

**Option B: Databricks SQL Warehouse**

- Execute via `databricks sql execute --statement "..."`
- Requires SQL warehouse with appropriate DBR version

---

## Expected Result

| Schema             | Tables | Iceberg Version | Key Properties               |
| ------------------ | ------ | --------------- | ---------------------------- |
| `gf_dbx.uniform`   | 5      | v2              | `enableIcebergCompatV2=true` |
| `gf_dbx.icebergv3` | 5      | v3              | `enableIcebergCompatV3=true` |
