# Plan: Create Databricks Notebooks for Horizon IRC Testing

## Overview
Create Python source format notebooks that can be imported directly into Databricks workspace to test:
1. Reading Snowflake Iceberg tables via Horizon IRC
2. Blob vs abfss endpoint compatibility
3. Bidirectional interop validation

## Notebooks to Create

### 1. `SF_Horizon_IRC_Setup.py`
Configure Spark catalog for Snowflake Horizon IRC endpoint:
```python
# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Horizon IRC Setup
# MAGIC Configure Spark to connect to Snowflake Iceberg tables via REST Catalog

# COMMAND ----------
# Configure Horizon IRC catalog
SF_PAT = dbutils.secrets.get(scope="snowflake-irc", key="pat")
spark.conf.set("spark.sql.catalog.snowflake_horizon", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.snowflake_horizon.type", "rest")
spark.conf.set("spark.sql.catalog.snowflake_horizon.uri", 
    "https://sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com/polaris/api/catalog")
spark.conf.set("spark.sql.catalog.snowflake_horizon.credential", SF_PAT)
spark.conf.set("spark.sql.catalog.snowflake_horizon.warehouse", "ICEBERG_POC")
```

### 2. `SF_Horizon_IRC_Tests.py`
Test queries against Snowflake Iceberg tables:
- List namespaces and tables
- Read EXTERNAL_ICEBERG.CUSTOMERS (99998 rows)
- Aggregation queries
- Join queries
- Endpoint compatibility validation

### 3. `SF_DBX_Bidirectional_Interop.py`
Combined test showing bidirectional access:
- Part A: Read Databricks tables via Unity Catalog (local)
- Part B: Read Snowflake tables via Horizon IRC
- Part C: Cross-platform join (if supported)

## File Structure
```
/Users/gfuribondo/CoCo/IcebergTesting/
  databricks_notebooks/
    SF_Horizon_IRC_Setup.py
    SF_Horizon_IRC_Tests.py
    SF_DBX_Bidirectional_Interop.py
```

## Import Instructions
After creation, user can import via:
1. Databricks Workspace > Import > File
2. Select .py files
3. Notebooks appear ready to run

## Prerequisites in Databricks
- Secret scope `snowflake-irc` with key `pat` containing Snowflake PAT
- Cluster with Iceberg runtime (Spark 3.x + Iceberg 1.4+)
- Network access to Snowflake endpoint
