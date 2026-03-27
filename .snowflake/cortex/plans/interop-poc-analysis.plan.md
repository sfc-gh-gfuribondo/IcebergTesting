# Snowflake-Databricks Iceberg Interoperability POC Analysis

## Executive Summary

The POC consists of **10 notebooks** (00-09) testing Snowflake Iceberg tables across 4 storage types with a healthcare domain (PATIENTS, ENCOUNTERS, CLAIMS, MEDICATIONS, PROVIDERS). The interoperability testing in **notebook 08** is the centerpiece, demonstrating bidirectional Snowflake ↔ Databricks access via CLD and Horizon IRC.

**Overall Assessment: STRONG foundation, with specific gaps to address.**

---

## 1. Interoperability Pattern Coverage

### Patterns Tested ✅
| Pattern | Notebook | Direction | Status |
|---------|----------|-----------|--------|
| **CLD (Catalog-Linked Database)** | 08 | SF reads DBX (Unity Catalog) | ✅ Tested — 5 tables via `gf_dbx_cld.uniform` |
| **Horizon IRC** | 08 | DBX reads SF (Iceberg REST Catalog) | ✅ Documented — PySpark code provided |
| **Delta UniForm (v2)** | 08 | DBX Delta → SF Iceberg v2 metadata | ✅ Tested via CLD uniform schema |
| **Iceberg v3 via CLD** | 08 | SF reads DBX v3 tables | ✅ Tested — `gf_dbx_cld.icebergv3` schema (5 tables) |
| **Cross-Platform Joins** | 08 | SF joins CLD tables | ✅ Multi-table joins on CLD data |
| **Vended Credentials** | 08 | ACCESS_DELEGATION_MODE | ✅ Documented + working |
| **STORAGE_SERIALIZATION_POLICY = COMPATIBLE** | 00, 08 | Interop-ready Parquet | ✅ Used in EXTERNAL_ICEBERG tables |
| **4-Way Storage Comparison** | 00, 02, 09 | Native/Managed/Customer/External | ✅ Comprehensive |

### Patterns NOT Tested ❌
| Pattern | Impact | Recommendation |
|---------|--------|----------------|
| **Write-back from Databricks** | HIGH | Add DBX PySpark INSERT/APPEND to SF Iceberg tables via IRC |
| **Schema Evolution cross-engine** | HIGH | Add column in SF, verify in DBX (and vice versa) |
| **CDC/Streaming sync** | MEDIUM | Delta CDC → Snowflake Streams or vice versa |
| **Partition evolution** | LOW | Test partition spec changes visible cross-engine |
| **Table maintenance (compaction)** | LOW | Test `OPTIMIZE` in DBX visibility from SF, and SF compaction from DBX |

---

## 2. Storage Type Parity Analysis

### Coverage Matrix by Notebook

| Notebook | Native | Managed | Customer (EXVOL) | External (BASE_LOCATION) | CLD |
|----------|--------|---------|-------------------|--------------------------|-----|
| 00 Setup | ✅ | ✅ | ✅ | ✅ | ✅ |
| 01 V3 Basics | — | — | ✅ | — | — |
| 02 Performance | ✅ | ✅ | ✅ | ✅ | — |
| 03 DML | — | ✅ | ✅ | ✅ | — |
| 04 Streams/DT | — | ✅ | ✅ | — | — |
| 05 Governance | — | ✅ | ✅ | — | — |
| 06 HA/DR | — | — | ✅ | — | — |
| 07 Concurrency | — | ✅ | ✅ | — | — |
| 08 Interop | — | — | — | ✅ (EVENTS_V3) | ✅ |
| 09 Summary | ✅ | ✅ | ✅ | ✅ | ✅ |

### Key Observations:
- **External Iceberg** (the most interop-relevant type) is underrepresented in notebooks 04-07
- **CLD** is only tested in notebooks 00, 08, and 09
- Managed and Customer storage have good parity in notebooks 03-07
- Native baseline is properly used for comparison in 00, 02, 09

---

## 3. Critical Interop Gaps

### GAP 1: No Databricks Write-Back Testing (SEVERITY: HIGH)
The POC only tests **read** from Databricks. A real interop engagement needs:
- Databricks PySpark `INSERT INTO` SF Iceberg table via IRC
- Databricks `CREATE TABLE AS SELECT` into shared storage
- Conflict resolution when both engines write

**Recommendation:** Add a Part 4 to notebook 08 with Databricks write-back tests.

### GAP 2: No Schema Evolution Cross-Engine (SEVERITY: HIGH)
No test verifies that schema changes in one engine are visible in the other:
- `ALTER ICEBERG TABLE ... ADD COLUMN` in SF → visible in DBX?
- `ALTER TABLE ... ADD COLUMN` in DBX → visible via CLD?

**Recommendation:** Add schema evolution tests to notebook 08 with before/after schema comparison.

### GAP 3: IRC Endpoint References Old Account (SEVERITY: MEDIUM)
Notebook 08 Part 2 references `sfsenorthamerica-demo_gfuribondo2` but the current account is `sfsenorthamerica-demo_gfuribondo`. This will cause IRC connection failures.

**Recommendation:** Update all IRC endpoint references to current account.

### GAP 4: HA/DR Not Executed (SEVERITY: MEDIUM)
Notebook 06 is entirely template-based. No actual replication was tested. For interop, cross-region failover of Iceberg tables with external storage is a critical customer concern.

**Recommendation:** Either execute with a secondary account or document as out-of-scope with clear prerequisites.

### GAP 5: Governance Not Tested on External Iceberg (SEVERITY: MEDIUM)
Masking policies, RAP, and tags in notebook 05 are only tested on Managed and Customer storage. External Iceberg tables (the interop target) have no governance tests.

**Recommendation:** Add governance tests for `EXTERNAL_ICEBERG` schema tables in notebook 05.

### GAP 6: No Role-Switching Masking Validation (SEVERITY: MEDIUM)
All governance tests run as ACCOUNTADMIN. Masking policies are never validated from a restricted role, so their effectiveness is unproven.

**Recommendation:** Create a test role, grant SELECT, and verify masking is enforced.

### GAP 7: Concurrency Not Truly Parallel (SEVERITY: LOW)
Notebook 07 runs sequential SQL cells, not 50 concurrent sessions. For Iceberg with external storage, concurrent writer conflicts are a real concern.

**Recommendation:** Use Python `ThreadPoolExecutor` or `snowflake-connector-python` for true parallel execution.

### GAP 8: Snowpipe Streaming Not Implemented (SEVERITY: LOW)
Listed in notebook 04 test cases but never implemented.

---

## 4. Iceberg v3 Interop Readiness

### v3 Features Tested
| Feature | SF-Side | DBX-Side | Cross-Engine | Status |
|---------|---------|----------|--------------|--------|
| Nanosecond timestamps (TIMESTAMP_NTZ(9)) | ✅ NB01, NB08 | ✅ PySpark code (NB08) | 🟡 Documented but not executed | PARTIAL |
| Default column values | ✅ NB08 (created_at) | ❌ Not tested | ❌ Not tested | GAP |
| VARIANT columns | ✅ NB01, NB02, NB07 | ❌ Not in DBX tests | ❌ Not tested | GAP |
| Row-level lineage | ❌ | ❌ | ❌ | NOT TESTED |
| Multi-argument transforms | ❌ | ❌ | ❌ | NOT TESTED |

### v3 Interop Concerns:
1. **Databricks Spark v3 support**: Requires DBR 14.x+. This is noted in NB08 but not validated.
2. **VARIANT cross-engine**: Snowflake VARIANT stores as Iceberg's `string` type with JSON. Spark reads this as `StringType` — needs explicit schema mapping test.
3. **Nanosecond precision loss**: Spark's `TimestampType` only supports microseconds. Nanosecond data written by SF may be truncated when read by DBX. This is a **known interop risk** that should be explicitly documented with test evidence.
4. **Default values in Iceberg v3 metadata**: Spark may not honor Iceberg v3 default values on INSERT. Needs validation.

---

## 5. Recommendations (Prioritized)

### P0 — Must Fix Before Customer Demo
1. **Fix IRC endpoint** in notebook 08 (old account `gfuribondo2` → `gfuribondo`)
2. **Add VARIANT cross-engine test** in NB08: create SF Iceberg table with VARIANT, read from DBX PySpark, verify JSON parsing
3. **Add nanosecond precision loss documentation**: Explicitly test and document that Spark truncates to microseconds

### P1 — High Value Additions
4. **Add write-back test** (NB08 Part 4): DBX PySpark writes to SF Iceberg table via IRC
5. **Add schema evolution test** (NB08 Part 5): ADD COLUMN in SF, verify in DBX
6. **Add External Iceberg governance tests** in NB05
7. **Add role-switching masking validation** in NB05
8. **Execute or scope-out HA/DR** in NB06

### P2 — Nice to Have
9. **Add Dynamic Table on External Iceberg** in NB04
10. **True parallel concurrency** in NB07 using Python threading
11. **Snowpipe Streaming implementation** in NB04
12. **Partition evolution cross-engine test** in NB08
13. **Table maintenance visibility test** (OPTIMIZE in DBX → visible from SF CLD)

---

## 6. Notebook-Specific Feedback

### NB00 — Setup Environment ✅ EXCELLENT
- Clean 4-way storage comparison with 20 healthcare tables
- Proper External Volume, warehouse sizing, CLD verification
- **Minor**: Consider adding `STORAGE_SERIALIZATION_POLICY = COMPATIBLE` to EXTERNAL_ICEBERG tables explicitly in DDL comments for clarity

### NB01 — V3 Basics ✅ GOOD
- Strong VARIANT + nanosecond timestamp testing with 1M rows
- Good clinical data model with nested JSON
- **Gap**: Only tests Customer Storage (TESTS schema). Add Managed Storage comparison.

### NB02 — Performance ✅ EXCELLENT
- Comprehensive 4-way comparison with 10 test categories
- Search Optimization, auto-clustering, warehouse scaling
- Query history performance analysis
- **Gap**: No CLD query performance comparison (important for interop latency)

### NB03 — DML ✅ GOOD
- INSERT/UPDATE/DELETE/MERGE across 3 Iceberg types
- Time travel on Customer and External
- **Gap**: No MERGE or time travel on Managed Storage

### NB04 — Streams/Tasks/DT 🟡 PARTIAL
- Streams and Dynamic Table on Customer Storage
- **Gaps**: No Task for Managed stream, no DT on Managed, no External Iceberg CDC, Snowpipe Streaming not implemented

### NB05 — Governance 🟡 PARTIAL
- Good masking policy coverage (SSN, DOB, Phone)
- RAP with geographic filtering
- **Gaps**: No External Iceberg governance, no role-switching test, RAP/tags only on Customer Storage

### NB06 — HA/DR 🔴 INCOMPLETE
- Template only — no execution
- Requires secondary account
- **Recommendation**: Scope-out or provision test account

### NB07 — Concurrency 🟡 PARTIAL
- Good Managed vs Customer comparison
- VARIANT and nanosecond timestamp under load
- **Gap**: Sequential, not truly concurrent; no External Iceberg

### NB08 — Interop ✅ GOOD (with gaps noted above)
- Bidirectional CLD + IRC architecture well documented
- v2 and v3 CLD testing
- Vended credentials setup reference
- **Gaps**: No write-back, no schema evolution, old IRC endpoint, PySpark cells not executable locally

### NB09 — Summary ✅ GOOD
- Comprehensive rollup with 5 success criteria
- Feature compatibility matrix
- Cost analysis
- **Gap**: SC-04 (HA/DR) marked PARTIAL

---

## 7. Overall Interop Maturity Score

| Category | Score | Notes |
|----------|-------|-------|
| CLD (SF → DBX) | 9/10 | Comprehensive, v2+v3, multi-table |
| IRC (DBX → SF) | 6/10 | Documented but not executed, wrong endpoint |
| v3 Feature Interop | 5/10 | SF-side strong, cross-engine untested |
| Write-Back | 0/10 | Not tested |
| Schema Evolution | 0/10 | Not tested |
| Governance Interop | 4/10 | SF-only, no cross-engine, no role test |
| Performance Interop | 7/10 | Good SF-side, missing CLD perf comparison |
| **Overall** | **6/10** | Strong read-path foundation, needs write-path + cross-engine validation |
