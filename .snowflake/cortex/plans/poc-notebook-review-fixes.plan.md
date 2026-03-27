---
name: "poc-notebook-review-fixes"
created: "2026-03-17T01:19:14.698Z"
status: pending
---

# POC Notebooks Review - Findings and Fixes

## Summary

Reviewed all 10 POC notebooks for consistency with the new 4-way storage comparison (Native, Managed, Customer, External). Found several issues that need to be addressed.

---

## Issue 1: Data Flow Dependency in 00\_Setup\_Environment

**File:** poc\_notebooks/00\_Setup\_Environment.ipynb

**Problem:** Cell 18 seeds EXTERNAL\_ICEBERG tables from TESTS schema, but TESTS tables are populated in cell 14 (after NATIVE\_BASELINE is created in cell 13). This creates a dependency chain that could fail if cells run out of order.

**Current (Cell 18):**

```
INSERT INTO ICEBERG_POC.EXTERNAL_ICEBERG.PATIENTS
SELECT ... FROM ICEBERG_POC.TESTS.PATIENTS;
```

**Fix:** Change to seed from NATIVE\_BASELINE for consistency:

```
INSERT INTO ICEBERG_POC.EXTERNAL_ICEBERG.PATIENTS
SELECT ... FROM ICEBERG_POC.NATIVE_BASELINE.PATIENTS;
```

---

## Issue 2: Missing Managed Storage in 04\_Streams\_Tasks\_DynamicTables

**File:** poc\_notebooks/04\_Streams\_Tasks\_DynamicTables.ipynb

**Problem:** Only tests streams/CDC on customer storage (TESTS schema). Should include managed storage for comparison.

**Current Coverage:**

- CLAIMS\_STAGING (TESTS - customer storage)
- CLAIMS\_STREAM on TESTS.CLAIMS\_STAGING

**Fix:** Add managed storage equivalents:

```
CREATE OR REPLACE ICEBERG TABLE ICEBERG_POC.MANAGED_ICEBERG.CLAIMS_STAGING (...)
CATALOG = 'SNOWFLAKE'
ICEBERG_VERSION = 3;

CREATE OR REPLACE STREAM ICEBERG_POC.MANAGED_ICEBERG.CLAIMS_STREAM
  ON TABLE ICEBERG_POC.MANAGED_ICEBERG.CLAIMS_STAGING APPEND_ONLY = TRUE;
```

---

## Issue 3: Missing Managed Storage in 05\_Governance\_Security

**File:** poc\_notebooks/05\_Governance\_Security.ipynb

**Problem:** Only tests governance policies on customer storage (TESTS.PATIENTS\_PHI). Should verify policies work identically on managed storage.

**Current Coverage:**

- PATIENTS\_PHI in TESTS schema only
- Masking policies (MASK\_SSN, MASK\_DOB, MASK\_PHONE)
- Row Access Policy (STATE\_RAP)
- Tags (HIPAA\_PHI)

**Fix:** Add managed storage PATIENTS\_PHI:

```
CREATE OR REPLACE ICEBERG TABLE ICEBERG_POC.MANAGED_ICEBERG.PATIENTS_PHI (...)
CATALOG = 'SNOWFLAKE'
ICEBERG_VERSION = 3;

-- Apply same policies to managed table
ALTER TABLE ICEBERG_POC.MANAGED_ICEBERG.PATIENTS_PHI
  ALTER COLUMN ssn SET MASKING POLICY MASK_SSN;
```

---

## Issue 4: Missing Managed Storage in 07\_Concurrency\_Stress

**File:** poc\_notebooks/07\_Concurrency\_Stress.ipynb

**Problem:** Only tests concurrency on customer storage (TESTS.CLAIMS\_AUDIT). Should compare managed vs customer storage under concurrent load.

**Current Coverage:**

- CLAIMS\_AUDIT in TESTS schema only

**Fix:** Add managed storage table:

```
CREATE OR REPLACE ICEBERG TABLE ICEBERG_POC.MANAGED_ICEBERG.CLAIMS_AUDIT (...)
CATALOG = 'SNOWFLAKE'
ICEBERG_VERSION = 3;
```

---

## Issue 5: 09\_POC\_Results\_Summary Needs Managed Storage Queries

**File:** poc\_notebooks/09\_POC\_Results\_Summary.ipynb

**Problem:** Feature test matrix queries (cells 11-12) only check Internal and External Iceberg. Should include Managed storage.

**Current (Cell 11):**

```
-- Feature test matrix - Internal Iceberg
```

**Fix:** Add cell for Managed Iceberg feature matrix:

```
-- Feature test matrix - Managed Storage Iceberg
SELECT 
    'Managed Storage' AS table_type,
    'CLAIMS' AS table_name,
    ...
```

---

## Notebooks Already Updated Correctly

| Notebook                     | Status  | Notes                                       |
| ---------------------------- | ------- | ------------------------------------------- |
| 00\_Setup\_Environment       | Partial | Has managed tables, but data flow issue     |
| 01\_Iceberg\_V3\_Basics      | OK      | Tests V3 features (VARIANT, timestamps)     |
| 02\_Performance\_Benchmarks  | OK      | 4-way comparison implemented                |
| 03\_DML\_Operations          | OK      | Managed storage DML added                   |
| 06\_HA\_DR\_Replication      | OK      | Uses ENCOUNTERS which exists in all schemas |
| 08\_Databricks\_IRC\_Interop | OK      | CLD/IRC testing (external focus is correct) |

---

## Execution Order

```
flowchart TD
    A[00_Setup_Environment] --> B[01_Iceberg_V3_Basics]
    A --> C[02_Performance_Benchmarks]
    A --> D[03_DML_Operations]
    A --> E[04_Streams_Tasks]
    A --> F[05_Governance]
    A --> G[06_HA_DR]
    A --> H[07_Concurrency]
    A --> I[08_Databricks_IRC]
    B --> J[09_Results_Summary]
    C --> J
    D --> J
    E --> J
    F --> J
    G --> J
    H --> J
    I --> J
```

---

## Recommended Priority

1. **High:** Fix cell 18 in 00\_Setup (data flow dependency)
2. **Medium:** Add managed storage to 04, 05, 07 notebooks
3. **Low:** Update 09 summary queries for completeness
