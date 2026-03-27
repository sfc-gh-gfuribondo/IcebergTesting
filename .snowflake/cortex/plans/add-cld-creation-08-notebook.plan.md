# Plan: Add CLD Creation to 08_Databricks_IRC_Interop.ipynb

## Summary
Add an executable `CREATE OR REPLACE DATABASE` statement for the CLD using the correct `LINKED_CATALOG` syntax discovered during testing.

## Changes

### 1. Add new code cell after cell 5 (before current cell 6)

Insert a new cell with the working CLD creation syntax:

```sql
-- Create Catalog-Linked Database (CLD) from Databricks Unity Catalog
CREATE OR REPLACE DATABASE gf_dbx_cld
  LINKED_CATALOG = ( CATALOG = 'gf_interop_unity_int' );
```

### 2. Update markdown cell 5

Change header from:
```markdown
## Step 1.2: Verify Catalog-Linked Database (CLD)
```
To:
```markdown
## Step 1.2: Create/Verify Catalog-Linked Database (CLD)
```

### 3. Fix reference cell 25 syntax

Update the commented reference from:
```sql
/*
CREATE OR REPLACE DATABASE gf_dbx_cld
  FROM LINKED_CATALOG
  CATALOG_INTEGRATION = 'gf_interop_unity_int';
*/
```
To:
```sql
/*
CREATE OR REPLACE DATABASE gf_dbx_cld
  LINKED_CATALOG = ( CATALOG = 'gf_interop_unity_int' );
*/
```

## File
- [08_Databricks_IRC_Interop.ipynb](poc_notebooks/08_Databricks_IRC_Interop.ipynb)
