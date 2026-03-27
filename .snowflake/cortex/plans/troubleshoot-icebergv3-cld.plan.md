# Plan: Troubleshoot Iceberg v3 CLD Tables

## Problem
Databricks `icebergv3` schema tables show "not initialized" error in Snowflake CLD, while `uniform` (v2) tables work correctly.

## Diagnosis Steps

### 1. Check Iceberg metadata folder exists in Databricks storage
Run in Databricks to verify Iceberg metadata was generated:
```python
# List table storage location
dbutils.fs.ls("abfss://gf-unity@gfstorageaccountwest2.dfs.core.windows.net/gf_interop/__unitystorage/catalogs/d1a2794b-7517-4e24-8dec-a75f22c239e3/tables/4eed77b1-7556-4ed2-8a9e-71416f289c7f/")
```
Look for `metadata/` subfolder with `.metadata.json` files.

### 2. Compare v2 vs v3 storage structure
Check if the working v2 table has different folder structure:
```sql
-- Get v2 table storage location from Databricks
DESCRIBE EXTENDED gf_dbx.uniform.patients;
```
Then list both locations and compare.

### 3. Force Iceberg metadata sync in Databricks
Try forcing metadata generation:
```sql
-- Option A: OPTIMIZE to trigger metadata write
OPTIMIZE gf_dbx.icebergv3.patients;

-- Option B: VACUUM to trigger cleanup/metadata refresh  
VACUUM gf_dbx.icebergv3.patients;

-- Option C: Small update to trigger new commit
UPDATE gf_dbx.icebergv3.patients SET first_name = first_name WHERE 1=0;
```

### 4. Check Unity Catalog REST API response
Use Databricks CLI to query the Iceberg REST endpoint directly:
```bash
curl -X GET "https://adb-xxx.azuredatabricks.net/api/2.1/unity-catalog/iceberg-rest/v1/gf_dbx/icebergv3/patients" \
  -H "Authorization: Bearer $DBX_TOKEN"
```

### 5. Verify Snowflake CLD refresh
After any Databricks changes, refresh in Snowflake:
```sql
ALTER ICEBERG TABLE gf_dbx_cld.icebergv3.patients REFRESH;
```

## Expected Outcome
- If metadata folder is missing: Iceberg v3 async generation issue (may need to wait or file Databricks support ticket)
- If metadata exists but CLD fails: Snowflake CLD may not support Iceberg v3 format yet (file Snowflake support ticket)
