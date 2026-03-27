# Plan: Refactor POC Notebooks to Healthcare Domain Data

## Overview

Replace all synthetic generic data (customers, events, orders, transactions) across all 10 POC notebooks with healthcare domain data that mirrors the real tables in `gf_dbx_cld.uniform`. This makes the Databricks interop story in NB08 feel native — the Snowflake-managed Iceberg tables and the CLD tables are now the same domain.

## Source of Truth: gf_dbx_cld.uniform Schema

| Table | Key Columns | Rows (CLD) | Target Scale (Snowflake) |
|---|---|---|---|
| `patients` | patient_id, first_name, last_name, date_of_birth, gender, blood_type, primary_phone, city, state, insurance_plan | 8 | 100K–1M |
| `encounters` | encounter_id, patient_id, provider_id, encounter_date, encounter_type, primary_diagnosis_code, primary_diagnosis_desc, disposition, total_charge | 10 | 500K–1M |
| `claims` | claim_id, encounter_id, patient_id, payer_name, claim_status, submitted_date, paid_date, billed_amount, allowed_amount, paid_amount | 10 | 500K |
| `medications` | medication_id, patient_id, encounter_id, drug_name, ndc_code, dosage, frequency, prescribed_date, end_date, prescribing_provider_id | 8 | 200K |
| `providers` | provider_id, first_name, last_name, specialty, npi_number, facility_name, facility_city, facility_state, accepting_patients | 5 | 10K |

ID ranges will be kept consistent with CLD: patients 1001+, encounters 3001+, claims 4001+, medications 5001+, providers 2001+.

## What Changes Per Notebook

### [NB00: 00_Setup_Environment.ipynb](00_Setup_Environment.ipynb)

The setup notebook creates the baseline tables across all 3 schemas (TESTS, EXTERNAL_ICEBERG, NATIVE_BASELINE). Currently creates 7 unrelated synthetic tables.

**Replace with 5 paired healthcare tables** (each as both a Snowflake-managed Iceberg + Native Baseline pair):

| Old Table | New Table | Scale | Key Change |
|---|---|---|---|
| `EVENTS` (event_id, customer_id, event_type, region, amount) | `ENCOUNTERS` (encounter_id, patient_id, provider_id, encounter_date, encounter_type, diagnosis_code, total_charge) | 1M | GENERATOR-based synthetic encounters |
| `DML_TEST` (id, name, value, status) | `CLAIMS` (claim_id, encounter_id, patient_id, payer_name, claim_status, billed_amount, allowed_amount, paid_amount) | 500K | DML target |
| `CDC_SOURCE` (id, customer_id, order_amount, status) | `CLAIMS_STAGING` (claim_id, encounter_id, patient_id, payer_name, claim_status, submitted_date, billed_amount) | 50K | CDC/stream source |
| `CUSTOMERS_PII` (customer_id, first_name, last_name, email, ssn) | `PATIENTS` (patient_id, first_name, last_name, date_of_birth, ssn, phone, city, state, insurance_plan) | 100K | PII/governance target |
| `REPLICATION_TEST` (id, data, region) | `ENCOUNTERS` at HA scale (same schema as above) | 1M | HA/DR target |
| `CONCURRENCY_TEST` (id, thread_id, value) | `CLAIMS_AUDIT` (claim_id, patient_id, action, payload VARIANT, created_at) | 100K | Concurrency target |
| `V3_VARIANT_TEST` (id, event_time, metadata VARIANT) | `PATIENTS` (patient_id, record_ts TIMESTAMP_NTZ(9), demographics VARIANT, clinical_notes VARIANT) | 1M | V3/VARIANT showcase |

External Iceberg tables (`EXTERNAL_ICEBERG` schema) become:
- `ENCOUNTERS` — replaces EVENTS
- `PATIENTS` — replaces CUSTOMERS
- `CLAIMS` — replaces ORDERS
- `MEDICATIONS` — replaces TRANSACTIONS (retains VARIANT via `medication_details VARIANT`)
- `PROVIDERS` — replaces PRODUCTS (retains VARIANT via `provider_attributes VARIANT`)

### [NB01: 01_Iceberg_V3_Basics.ipynb](01_Iceberg_V3_Basics.ipynb)

Currently creates `V3_VARIANT_TEST` with generic `payload` and `metadata` columns.

**Replace with**: `PATIENTS_V3` table showcasing:
- `record_ts TIMESTAMP_NTZ(9)` — nanosecond admission timestamp
- `demographics VARIANT` — JSON blob: `{"blood_type": "O+", "allergies": [...], "emergency_contact": {...}}`
- `clinical_notes VARIANT` — nested: `{"diagnoses": [...], "vitals": {...}, "provider_id": 2001}`

This makes the V3 VARIANT story concrete: "here's why VARIANT matters for patient records."

### [NB02: 02_Performance_Benchmarks.ipynb](02_Performance_Benchmarks.ipynb)

Currently benchmarks queries against EVENTS/CUSTOMERS/ORDERS/TRANSACTIONS/PRODUCTS.

**Replace benchmark queries** with clinically meaningful ones:
- SC-01 query 1: `SELECT encounter_type, COUNT(*), AVG(total_charge) FROM ENCOUNTERS GROUP BY encounter_type` — on all 3 storage types
- SC-01 query 2: `SELECT payer_name, SUM(billed_amount), SUM(paid_amount) FROM CLAIMS WHERE claim_status = 'Paid' GROUP BY payer_name`
- SC-01 query 3: `SELECT specialty, COUNT(DISTINCT patient_id) FROM PROVIDERS p JOIN ENCOUNTERS e ON p.provider_id = e.provider_id GROUP BY specialty`
- Range scan: claims by submitted_date range
- VARIANT query: `demographics:blood_type::STRING` on PATIENTS

### [NB03: 03_DML_Operations.ipynb](03_DML_Operations.ipynb)

Currently does DML on generic DML_TEST/CUSTOMERS/TRANSACTIONS/ORDERS.

**Replace with healthcare DML scenarios**:
- `INSERT` into `CLAIMS` — new claim submissions via GENERATOR (SEQ4() fix already documented)
- `UPDATE` — `claim_status = 'Paid'` where `claim_status = 'Submitted'`
- `DELETE` — `DELETE FROM CLAIMS WHERE claim_status = 'Denied'`
- `MERGE` — upsert new encounters from `CLAIMS_STAGING` source; use `SEQ4() + 2000000 AS claim_id` (not `id + 2000000`)
- Time travel: `SELECT * FROM CLAIMS AT(TIMESTAMP => $before_ts)` before/after DML

### [NB04: 04_Streams_Tasks_DynamicTables.ipynb](04_Streams_Tasks_DynamicTables.ipynb)

Currently uses CDC_SOURCE (fake customer orders) feeding into CDC_TARGET.

**Replace with**:
- `CLAIMS_STAGING` Iceberg table as the stream source (new claim submissions arrive)
- `CLAIMS_STREAM` (APPEND_ONLY = TRUE) — same fix already confirmed
- `CLAIMS_PROCESSED` as the CDC target (stream consumer)
- `ENCOUNTER_SUMMARY` Dynamic Table: `SELECT encounter_type, state, COUNT(*) AS encounter_count, SUM(total_charge) AS total_charge FROM ENCOUNTERS JOIN PATIENTS ON ... GROUP BY encounter_type, state` with 1-minute lag

### [NB05: 05_Governance_Security.ipynb](05_Governance_Security.ipynb)

Currently uses a generic CUSTOMERS_PII table. Healthcare data makes the HIPAA/governance story much more compelling.

**Replace with `PATIENTS_PII`**:
- Columns: `patient_id, first_name, last_name, date_of_birth, ssn, primary_phone, city, state, insurance_plan`
- Masking policies: `MASK_SSN` (***-**-XXXX), `MASK_DOB` (show only year), `MASK_PHONE` (555-***-****)
- Tags: `PII_TYPE` = SSN / DATE_OF_BIRTH / PHONE, `DATA_CLASSIFICATION` = HIPAA_PHI
- Row Access Policy `STATE_RAP`: restrict by patient state (mirrors real payer regional restrictions)
- Query reference: "HIPAA requires masking of 18 PHI identifiers" framing

### [NB06: 06_HA_DR_Replication.ipynb](06_HA_DR_Replication.ipynb)

Currently uses a featureless `REPLICATION_TEST (id, data, region)`.

**Replace with `ENCOUNTERS` at 1M scale** — same schema as production table. Validates that encounter records with diagnosis codes and charges replicate cleanly. Replication metadata check: `SYSTEM$GET_ICEBERG_TABLE_INFORMATION` on ENCOUNTERS.

### [NB07: 07_Concurrency_Stress.ipynb](07_Concurrency_Stress.ipynb)

Currently uses `CONCURRENCY_TEST (id, thread_id, operation, payload VARIANT)`.

**Replace with `CLAIMS_AUDIT`**:
- Columns: `claim_id, patient_id, action VARCHAR (Submit/Review/Approve/Deny), audit_payload VARIANT, created_at`
- VARIANT payload: `{"payer": "Blue Cross", "reviewer": "system", "reason_code": "AUTH_OK"}`
- Makes the concurrency story real: "100K simultaneous claim audit log writes"

### [NB09: 09_POC_Results_Summary.ipynb](09_POC_Results_Summary.ipynb)

- Update the infrastructure inventory queries to reference new healthcare table names
- Update the feature matrix notes column to mention healthcare examples (e.g., "VARIANT for patient demographics, clinical notes")
- Keep SC-01/02/03/04/05/INTEROP/EXTERNAL criteria unchanged — just update example names

## Data Generation Strategy

All large tables use `TABLE(GENERATOR(ROWCOUNT => N))` with `SEQ4()` for IDs:

```sql
-- Example: synthetic PATIENTS at 100K
INSERT INTO ICEBERG_POC.TESTS.PATIENTS
SELECT
    SEQ4() + 1001                                          AS patient_id,
    'Patient_' || SEQ4()                                   AS first_name,
    'Last_' || (SEQ4() % 1000)                             AS last_name,
    DATEADD('day', -(SEQ4() % 36500), '2000-01-01')       AS date_of_birth,
    CASE SEQ4() % 2 WHEN 0 THEN 'M' ELSE 'F' END          AS gender,
    ARRAY_CONSTRUCT('A+','A-','B+','O+','O-','AB+')[SEQ4() % 6]::STRING AS blood_type,
    '555-' || LPAD(SEQ4() % 9999, 4, '0')                 AS primary_phone,
    ARRAY_CONSTRUCT('Phoenix','Denver','Seattle','Austin','Chicago')[SEQ4() % 5]::STRING AS city,
    ARRAY_CONSTRUCT('AZ','CO','WA','TX','IL','OR','FL','MA')[SEQ4() % 8]::STRING AS state,
    ARRAY_CONSTRUCT('Blue Cross PPO','Aetna HMO','United Healthcare','Cigna EPO','Humana Gold')[SEQ4() % 5]::STRING AS insurance_plan
FROM TABLE(GENERATOR(ROWCOUNT => 100000));
```

## Column Name Alignment

Columns across Snowflake-managed tables will exactly match the CLD column names so NB08 joins work identically on either side:

| CLD Column | Snowflake TESTS Column | Notes |
|---|---|---|
| `patient_id` | `patient_id` | exact match |
| `encounter_id` | `encounter_id` | exact match |
| `claim_id` | `claim_id` | exact match |
| `provider_id` | `provider_id` | exact match |
| `medication_id` | `medication_id` | exact match |
| `encounter_type` | `encounter_type` | exact match |
| `claim_status` | `claim_status` | exact match |
| `total_charge` | `total_charge` | exact match |
| `billed_amount` | `billed_amount` | exact match |

## Files to Modify

- [`poc_notebooks/00_Setup_Environment.ipynb`](poc_notebooks/00_Setup_Environment.ipynb) — all table DDL + INSERT cells
- [`poc_notebooks/01_Iceberg_V3_Basics.ipynb`](poc_notebooks/01_Iceberg_V3_Basics.ipynb) — V3_VARIANT_TEST → PATIENTS_V3
- [`poc_notebooks/02_Performance_Benchmarks.ipynb`](poc_notebooks/02_Performance_Benchmarks.ipynb) — all benchmark query cells
- [`poc_notebooks/03_DML_Operations.ipynb`](poc_notebooks/03_DML_Operations.ipynb) — DML cells + MERGE source
- [`poc_notebooks/04_Streams_Tasks_DynamicTables.ipynb`](poc_notebooks/04_Streams_Tasks_DynamicTables.ipynb) — CDC_SOURCE → CLAIMS_STAGING, DT → ENCOUNTER_SUMMARY
- [`poc_notebooks/05_Governance_Security.ipynb`](poc_notebooks/05_Governance_Security.ipynb) — CUSTOMERS_PII → PATIENTS_PII, new masking policies
- [`poc_notebooks/06_HA_DR_Replication.ipynb`](poc_notebooks/06_HA_DR_Replication.ipynb) — REPLICATION_TEST → ENCOUNTERS
- [`poc_notebooks/07_Concurrency_Stress.ipynb`](poc_notebooks/07_Concurrency_Stress.ipynb) — CONCURRENCY_TEST → CLAIMS_AUDIT
- [`poc_notebooks/09_POC_Results_Summary.ipynb`](poc_notebooks/09_POC_Results_Summary.ipynb) — table name references + notes

NB08 (`08_Databricks_IRC_Interop.ipynb`) is **unchanged** — it already uses the CLD healthcare tables directly.
