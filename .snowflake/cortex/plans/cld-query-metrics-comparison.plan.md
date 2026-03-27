# Plan: CLD vs Native Query Metrics Comparison

## Context

[Notebook 08](poc_notebooks/08_Databricks_IRC_Interop.ipynb) already has basic CLD benchmarks (cells 18-20) that check `QUERY_HISTORY` for CLD queries only. There is **no side-by-side comparison** with native table metrics to confirm that bytes_scanned, rows_produced, and other observability metrics are captured equivalently.

## Approach

Add a new section (Part 6 or extend Part 1.5) to notebook 08 with 3-4 cells that:

1. Run identical queries against **CLD** (`gf_dbx_cld.uniform.patients`) and **native** (`ICEBERG_POC.NATIVE_BASELINE.PATIENTS`) tables, using tagged comments for easy identification in query history.
2. Query `SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY` to pull metrics for both and display them side-by-side.

**Note:** `ACCOUNT_USAGE.QUERY_HISTORY` has up to 45 minutes of latency. As an alternative, we can use `INFORMATION_SCHEMA.QUERY_HISTORY()` for near-real-time results (last 7 days, current account only).

## New Cells

### Cell A: Tagged CLD queries
```sql
-- Part 6: Query Metrics Comparison (CLD vs Native)
-- CLD query tag: metrics_test_cld
SELECT /* metrics_test_cld_scan */ COUNT(*) AS cnt FROM gf_dbx_cld.uniform.patients;

SELECT /* metrics_test_cld_agg */
    gender, COUNT(*) AS cnt, AVG(YEAR(date_of_birth)) AS avg_birth_year
FROM gf_dbx_cld.uniform.patients
GROUP BY gender;
```

### Cell B: Tagged native queries
```sql
SELECT /* metrics_test_native_scan */ COUNT(*) AS cnt FROM ICEBERG_POC.NATIVE_BASELINE.PATIENTS;

SELECT /* metrics_test_native_agg */
    gender, COUNT(*) AS cnt, AVG(YEAR(date_of_birth)) AS avg_birth_year
FROM ICEBERG_POC.NATIVE_BASELINE.PATIENTS
GROUP BY gender;
```

### Cell C: Side-by-side metrics comparison
```sql
-- Uses INFORMATION_SCHEMA for near-real-time results
SELECT
    CASE
        WHEN QUERY_TEXT ILIKE '%metrics_test_cld%' THEN 'CLD'
        WHEN QUERY_TEXT ILIKE '%metrics_test_native%' THEN 'NATIVE'
    END AS source_type,
    CASE
        WHEN QUERY_TEXT ILIKE '%_scan%' THEN 'FULL_SCAN'
        WHEN QUERY_TEXT ILIKE '%_agg%' THEN 'AGGREGATION'
    END AS query_type,
    TOTAL_ELAPSED_TIME AS elapsed_ms,
    COMPILATION_TIME AS compile_ms,
    EXECUTION_TIME AS exec_ms,
    BYTES_SCANNED,
    ROWS_PRODUCED,
    PARTITIONS_SCANNED,
    PARTITIONS_TOTAL,
    BYTES_SPILLED_TO_LOCAL_STORAGE AS local_spill,
    BYTES_SPILLED_TO_REMOTE_STORAGE AS remote_spill,
    QUERY_TEXT
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER())
WHERE QUERY_TEXT ILIKE '%metrics_test_%'
    AND QUERY_TYPE = 'SELECT'
ORDER BY source_type, query_type;
```

### Cell D: NULL field audit
```sql
-- Check which metrics columns are NULL for CLD vs Native
SELECT
    CASE WHEN QUERY_TEXT ILIKE '%metrics_test_cld%' THEN 'CLD' ELSE 'NATIVE' END AS source,
    COUNT(*) AS query_count,
    COUNT(BYTES_SCANNED) AS has_bytes_scanned,
    COUNT(ROWS_PRODUCED) AS has_rows_produced,
    COUNT(PARTITIONS_SCANNED) AS has_partitions_scanned,
    COUNT(PARTITIONS_TOTAL) AS has_partitions_total,
    COUNT(BYTES_WRITTEN) AS has_bytes_written,
    COUNT(BYTES_WRITTEN_TO_RESULT) AS has_bytes_written_to_result
FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY_BY_USER())
WHERE QUERY_TEXT ILIKE '%metrics_test_%'
    AND QUERY_TYPE = 'SELECT'
GROUP BY 1;
```

## Key Metrics to Compare

| Metric | Expected for CLD | Notes |
|---|---|---|
| BYTES_SCANNED | Populated | Should reflect actual Iceberg file reads |
| ROWS_PRODUCED | Populated | Row count returned |
| TOTAL_ELAPSED_TIME | Populated | End-to-end time |
| COMPILATION_TIME | Populated | May differ due to CLD metadata resolution |
| PARTITIONS_SCANNED | May be NULL | CLD may not report Iceberg partition stats |
| BYTES_SPILLED | Populated if applicable | Should behave same as native |

## Files Modified

- [poc_notebooks/08_Databricks_IRC_Interop.ipynb](poc_notebooks/08_Databricks_IRC_Interop.ipynb): Add 5 new cells (1 markdown header + 4 code cells) after existing performance benchmark section (after cell 20).
