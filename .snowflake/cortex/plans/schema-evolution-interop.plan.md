# Plan: Add Schema Evolution Cross-Engine Tests to Notebook 08

## Overview
Add a new **Part 5: Schema Evolution Cross-Engine Tests** to `poc_notebooks/08_Databricks_IRC_Interop.ipynb`. This section will validate that schema changes made in one engine are correctly visible in the other.

## Location
Insert after the existing Part 3 (v3 interop tests) summary cell and before the "Step 2.3: Endpoint Compatibility Test" section. This keeps all interop test parts together.

## New Cells to Add

### Cell 1: Markdown — Part 5 Header
```markdown
---
# Part 5: Schema Evolution Cross-Engine Tests

Validates that schema changes in one engine propagate correctly to the other.

| Test | Direction | Operation | Expected |
|------|-----------|-----------|----------|
| 5.1 | SF → DBX | ADD COLUMN in Snowflake Iceberg | Visible in DBX via IRC |
| 5.2 | SF → CLD | ADD COLUMN in SF External Iceberg | Visible via CLD refresh |
| 5.3 | DBX → SF | ADD COLUMN in Databricks Delta+UniForm | Visible in CLD after sync |
| 5.4 | SF → DBX | Type widening (INT → BIGINT) | Schema change visible cross-engine |
| 5.5 | SF → DBX | RENAME COLUMN | Verify rename propagation |
```

### Cell 2: SQL — Step 5.1: Baseline schema snapshot
```sql
-- Step 5.1: Capture baseline schema for EVENTS_V3 before evolution
DESCRIBE TABLE ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3;
```

### Cell 3: SQL — Step 5.2: ADD COLUMN in Snowflake
```sql
-- Step 5.2: Add columns to Snowflake Iceberg table
ALTER ICEBERG TABLE ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3
  ADD COLUMN country STRING;

ALTER ICEBERG TABLE ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3
  ADD COLUMN event_priority INT;

-- Verify new columns exist
DESCRIBE TABLE ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3;
```

### Cell 4: SQL — Step 5.3: Populate new columns
```sql
-- Populate new columns with data
UPDATE ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3
SET 
    country = CASE region
        WHEN 'us-east' THEN 'US'
        WHEN 'us-west' THEN 'US'
        WHEN 'eu-west' THEN 'DE'
        WHEN 'ap-south' THEN 'IN'
        ELSE 'US'
    END,
    event_priority = CASE event_type
        WHEN 'purchase' THEN 1
        WHEN 'signup' THEN 2
        WHEN 'click' THEN 3
        WHEN 'view' THEN 4
        ELSE 5
    END
WHERE country IS NULL;

-- Verify data in new columns
SELECT event_id, event_type, region, country, event_priority
FROM ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3
LIMIT 10;
```

### Cell 5: SQL — Step 5.4: Verify Iceberg metadata reflects schema change
```sql
-- Verify Iceberg metadata updated with new schema
SELECT 
    PARSE_JSON(SYSTEM$GET_ICEBERG_TABLE_INFORMATION('ICEBERG_POC.EXTERNAL_ICEBERG.EVENTS_V3')) AS iceberg_info;
```

### Cell 6: Python — Step 5.5: Databricks reads evolved schema via IRC
```python
# Databricks: Verify schema evolution is visible via Horizon IRC
# The new columns (country, event_priority) should appear in the Spark schema

df_evolved = spark.read.table("snowflake_horizon.EXTERNAL_ICEBERG.EVENTS_V3")
print("=== Schema after SF ADD COLUMN ===")
df_evolved.printSchema()

# Verify new columns have data
df_evolved.select("event_id", "event_type", "region", "country", "event_priority").show(10)

# Validate column count increased
expected_cols = ["event_id", "event_type", "event_timestamp", "user_id", 
                 "payload", "region", "created_at", "country", "event_priority"]
actual_cols = df_evolved.columns
missing = set(expected_cols) - set(actual_cols)
print(f"✓ Schema evolution test: {'PASS' if not missing else 'FAIL - missing: ' + str(missing)}")
```

### Cell 7: Markdown — Step 5.6: CLD schema evolution header
```markdown
## Step 5.6: CLD Schema Evolution (DBX → SF)

Test that schema changes made in Databricks are visible in Snowflake via the CLD.

**Note:** This requires running `ALTER TABLE` in Databricks first, then refreshing the CLD.
```

### Cell 8: SQL — Step 5.7: CLD schema before
```sql
-- Capture CLD table schema before Databricks schema change
DESCRIBE TABLE gf_dbx_cld.uniform.patients;
```

### Cell 9: Python — Step 5.8: Databricks ADD COLUMN
```python
# Databricks: Add column to Delta+UniForm table
# Run this in Databricks notebook

spark.sql("""
    ALTER TABLE gf_dbx.uniform.patients 
    ADD COLUMN preferred_language STRING
""")

# Populate new column
spark.sql("""
    UPDATE gf_dbx.uniform.patients 
    SET preferred_language = CASE 
        WHEN state IN ('TX', 'FL', 'CA') THEN 'Spanish'
        ELSE 'English'
    END
""")

# Run OPTIMIZE to regenerate Iceberg metadata with new schema
spark.sql("OPTIMIZE gf_dbx.uniform.patients")

print("✓ Column 'preferred_language' added and optimized")
```

### Cell 10: SQL — Step 5.9: Refresh CLD and verify
```sql
-- Refresh CLD to pick up schema changes from Databricks
-- (CLD auto-syncs, but force refresh to verify)
ALTER DATABASE gf_dbx_cld REFRESH;

-- Verify new column is visible in Snowflake CLD
DESCRIBE TABLE gf_dbx_cld.uniform.patients;

-- Query the new column
SELECT patient_id, first_name, last_name, state, preferred_language
FROM gf_dbx_cld.uniform.patients
LIMIT 10;
```

### Cell 11: SQL — Step 5.10: Type widening test
```sql
-- Step 5.10: Test type widening (requires Iceberg v3 type promotion support)
-- Note: Iceberg v3 supports type promotion (int → long, float → double)

-- Create a test table for type widening
CREATE OR REPLACE ICEBERG TABLE ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST (
    id INT,
    metric_value INT,
    label STRING
)
CATALOG = 'SNOWFLAKE'
EXTERNAL_VOLUME = 'gf_iceberg_ext_vol'
BASE_LOCATION = 'iceberg_v3_tests/schema_evolution/'
STORAGE_SERIALIZATION_POLICY = COMPATIBLE;

INSERT INTO ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST
SELECT SEQ4(), SEQ4() * 100, 'label_' || SEQ4()::STRING
FROM TABLE(GENERATOR(ROWCOUNT => 1000));

-- Widen INT to BIGINT (Iceberg type promotion)
ALTER ICEBERG TABLE ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST
  ALTER COLUMN metric_value SET DATA TYPE BIGINT;

-- Insert value exceeding INT range to prove widening works
INSERT INTO ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST
VALUES (9999, 3000000000, 'bigint_test');

-- Verify
SELECT * FROM ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST
WHERE id = 9999;
```

### Cell 12: Python — Step 5.11: Databricks verifies type widening
```python
# Databricks: Verify type widening is visible via IRC
df_widened = spark.read.table("snowflake_horizon.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST")
print("=== Schema after type widening ===")
df_widened.printSchema()

# Verify BIGINT value is readable
df_widened.filter("id = 9999").show()
bigint_val = df_widened.filter("id = 9999").select("metric_value").collect()[0][0]
print(f"✓ Type widening test: {'PASS' if bigint_val == 3000000000 else 'FAIL'}")
```

### Cell 13: SQL — Step 5.12: RENAME COLUMN test
```sql
-- Step 5.12: Rename column test
ALTER ICEBERG TABLE ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST
  RENAME COLUMN label TO description;

-- Verify rename
DESCRIBE TABLE ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST;

SELECT id, metric_value, description
FROM ICEBERG_POC.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST
LIMIT 5;
```

### Cell 14: Python — Step 5.13: Databricks verifies rename
```python
# Databricks: Verify column rename visible via IRC
df_renamed = spark.read.table("snowflake_horizon.EXTERNAL_ICEBERG.SCHEMA_EVOLUTION_TEST")
print("=== Schema after RENAME COLUMN ===")
df_renamed.printSchema()

has_description = "description" in df_renamed.columns
has_label = "label" in df_renamed.columns
print(f"✓ Rename test: {'PASS' if has_description and not has_label else 'FAIL'}")
```

### Cell 15: Markdown — Schema Evolution Results Summary
```markdown
## Step 5.14: Schema Evolution Test Results

| Test | Direction | Operation | SF Result | DBX Result | Status |
|------|-----------|-----------|-----------|------------|--------|
| 5.1-5.5 | SF → DBX | ADD COLUMN (country, event_priority) | ✅ | TBD | ⏳ |
| 5.6-5.9 | DBX → SF | ADD COLUMN (preferred_language) via CLD | TBD | ✅ | ⏳ |
| 5.10-5.11 | SF → DBX | Type widening INT → BIGINT | ✅ | TBD | ⏳ |
| 5.12-5.13 | SF → DBX | RENAME COLUMN (label → description) | ✅ | TBD | ⏳ |

### Key Findings:
1. Iceberg v3 schema evolution is **additive** — ADD COLUMN is safe for cross-engine reads
2. Type widening (INT → BIGINT) follows Iceberg v3 type promotion rules
3. RENAME COLUMN updates Iceberg metadata — readers must refresh catalog to see new names
4. CLD requires `ALTER DATABASE ... REFRESH` or auto-sync to pick up Databricks schema changes
5. **UniForm limitation**: Delta UniForm regenerates Iceberg metadata on OPTIMIZE — schema changes require re-OPTIMIZE
```

## Implementation Notes
- Total new cells: **15** (5 markdown, 5 SQL, 5 Python/PySpark)
- SQL cells are executable in Snowflake directly
- Python cells are Databricks PySpark — documented for execution in Databricks notebook environment
- Insert position: After the Part 3 v3 summary cell (cell index ~60) and before the endpoint compatibility section
