# Plan: Update Databricks Notebooks to Healthcare Domain

## Overview
Update 4 Databricks notebooks to use healthcare domain while preserving catalog connectivity and adding comparison tests.

## Key Mappings

### Table Names
| Old (Generic) | New (Healthcare) | Row Count |
|---------------|------------------|-----------|
| customers | patients | 100,000 |
| events | encounters | 1,000,000 |
| orders | claims | 500,000 |
| products | medications | 300,000 |
| transactions | providers | 1,000 |

### Column Mappings
| Old | New |
|-----|-----|
| customer_id | patient_id |
| customer_name | patient_name |
| customer_tier | risk_tier |
| order_id | claim_id |
| order_status | claim_status |
| total_amount | claim_amount |
| event_id | encounter_id |
| event_type | encounter_type |
| product_id | medication_id |
| transaction_id | provider_id |

## Connectivity Preservation
- **Keep**: Foreign catalog name `snowflake_iceberg`
- **Keep**: Schema `external_iceberg` 
- **Keep**: IRC endpoint configuration
- **Keep**: Vended credentials flow
- **Change**: Only table/column references

## Files to Update

### 1. Test_Foreign_Catalog.py
- Update queries from `customers/orders/events` to `patients/claims/encounters`
- Update column references in SELECT statements
- Update join keys (customer_id → patient_id)
- Update aggregation queries for healthcare metrics

### 2. Foreign_Catalog_Test_Suite.py
```python
# OLD
EXPECTED_COUNTS = {
    "customers": 99998,
    "events": 1000000,
    "orders": 500000,
    "products": 10000,
    "transactions": 1000000
}

# NEW
EXPECTED_COUNTS = {
    "patients": 100000,
    "encounters": 1000000,
    "claims": 500000,
    "medications": 300000,
    "providers": 1000
}
```
- Update all test functions for healthcare schema
- Update data type tests for healthcare columns
- Update join tests (patients ↔ claims ↔ encounters)

### 3. SF_Horizon_IRC_Tests.py
- Change CUSTOMERS (99,998) → PATIENTS (100,000)
- Change ORDERS → CLAIMS
- Change TRANSACTIONS → PROVIDERS
- Update aggregation and join queries

### 4. SF_DBX_Bidirectional_Interop.py
- Align Snowflake IRC side to use PATIENTS (currently uses CUSTOMERS)
- Add comparison section: DBX local vs Foreign Catalog
  - Same table (patients) from two sources
  - Performance comparison
  - Data consistency validation

## New Comparison Tests

### DBX Local vs Foreign Catalog Read
```python
# DBX Local (UniForm)
dbx_patients = spark.table("gf_interop.uniform.patients")

# Snowflake via Foreign Catalog (IRC)
sf_patients = spark.table("snowflake_iceberg.external_iceberg.patients")

# Compare counts, schema, sample data
# Measure read performance for each source
```

## Risk Mitigation
1. **Backup current files** before editing
2. **Test catalog connectivity** remains unchanged
3. **Verify table existence** in Snowflake EXTERNAL_ICEBERG schema
4. **Validate column names** match actual healthcare schema
