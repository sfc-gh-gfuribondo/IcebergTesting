# Plan: Fix icebergv3 Credential Vending

## Problem
The `icebergv3` tables in the CLD show "not initialized" errors with message:
> "Failed to retrieve credentials from the Catalog for table X. Please ensure that the catalog vends credentials"

## Root Cause
Missing `EXTERNAL USE SCHEMA` privilege on `gf_dbx.icebergv3` schema in Databricks Unity Catalog.

## Solution

### Task 1: Grant EXTERNAL USE SCHEMA in Databricks

Run these SQL statements in **Databricks SQL** (not Snowflake):

```sql
-- Grant credential vending permission (REQUIRED for vended credentials)
GRANT EXTERNAL USE SCHEMA ON SCHEMA gf_dbx.icebergv3 TO `gene.furibondo@snowflake.com`;

-- Verify other required privileges exist
GRANT USE CATALOG ON CATALOG gf_dbx TO `gene.furibondo@snowflake.com`;
GRANT USE SCHEMA ON SCHEMA gf_dbx.icebergv3 TO `gene.furibondo@snowflake.com`;
GRANT SELECT ON SCHEMA gf_dbx.icebergv3 TO `gene.furibondo@snowflake.com`;
```

**Note:** Replace `gene.furibondo@snowflake.com` with the actual Databricks user/principal associated with the PAT used in the catalog integration.

### Task 2: Refresh CLD in Snowflake

After granting permissions, refresh the CLD to pick up the new credentials:

```sql
-- In Snowflake
CREATE OR REPLACE DATABASE gf_dbx_cld
  LINKED_CATALOG = ( CATALOG = 'gf_interop_unity_int' );
```

### Task 3: Verify icebergv3 Tables Accessible

Test that icebergv3 tables now work:

```sql
-- Check sync status
SELECT SYSTEM$CATALOG_LINK_STATUS('GF_DBX_CLD');

-- List tables
SHOW TABLES IN gf_dbx_cld.icebergv3;

-- Query a table
SELECT COUNT(*) FROM gf_dbx_cld.icebergv3.patients;
```

## Expected Outcome
- All 5 icebergv3 tables (patients, providers, encounters, diagnoses, medications) should be queryable
- No more "Failed to retrieve credentials" errors
