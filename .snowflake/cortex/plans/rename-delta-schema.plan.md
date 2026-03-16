# Plan: Rename Delta Schema

## Overview
Reorganize Unity Catalog schemas in `gf_interop` catalog:
- Delete `delta_only` schema (cleanup)
- Rename `default` schema to `delta` for clarity

## Current State
```
gf_interop
├── default          (Delta tables)
├── delta_only       (test Delta table - to delete)
├── uniform          (UniForm tables - visible to Snowflake)
└── information_schema
```

## Target State
```
gf_interop
├── delta            (Delta tables - NOT visible to Snowflake CLD)
├── uniform          (UniForm tables - visible to Snowflake CLD)
└── information_schema
```

## Implementation

### Step 1: Delete delta_only schema
```sql
DROP SCHEMA gf_interop.delta_only CASCADE;
```

### Step 2: Rename default to delta
```sql
ALTER SCHEMA gf_interop.default RENAME TO delta;
```

### Step 3: Verify
```sql
SHOW SCHEMAS IN gf_interop;
```

## Notes
- The `default` schema cannot be deleted in Unity Catalog, but it can be renamed
- After renaming, Delta tables will be in `gf_interop.delta` 
- These will remain invisible to Snowflake CLD (no Iceberg metadata)
- `uniform` schema tables will continue to be visible to Snowflake
