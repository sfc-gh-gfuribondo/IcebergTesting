#!/usr/bin/env python3
"""
Test Databricks reading Snowflake Iceberg tables via Horizon IRC.
Tests blob vs abfss endpoint compatibility.
"""
import requests
import time
import json
import sys

DBX_URL = "https://adb-2584487012733217.17.azuredatabricks.net"
CLUSTER_ID = "0312-195438-x8cogecz"

def get_context(token):
    resp = requests.post(f"{DBX_URL}/api/1.2/contexts/create",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"language": "python", "clusterId": CLUSTER_ID})
    ctx = resp.json()
    ctx_id = ctx.get("id")
    if not ctx_id:
        print(f"Failed to create context: {ctx}")
        sys.exit(1)
    print(f"Context ID: {ctx_id}")
    return ctx_id

def run_code(token, ctx_id, code, label=""):
    resp = requests.post(f"{DBX_URL}/api/1.2/commands/execute",
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json"},
        json={"language": "python", "clusterId": CLUSTER_ID, "contextId": ctx_id, "command": code})
    cmd_id = resp.json().get("id")
    if not cmd_id:
        print(f"Failed to submit: {label} -> {resp.json()}")
        return None
    
    for _ in range(120):
        time.sleep(3)
        r = requests.get(f"{DBX_URL}/api/1.2/commands/status",
            headers={"Authorization": f"Bearer {token}"},
            params={"clusterId": CLUSTER_ID, "contextId": ctx_id, "commandId": cmd_id}).json()
        if r["status"] == "Finished":
            if r["results"].get("resultType") == "error":
                print(f"ERROR [{label}]: {r['results'].get('cause', '')[:500]}")
                return None
            data = r["results"].get("data", "")
            print(f"OK [{label}]:")
            print(data[:1000] if data else "(no output)")
            return r["results"]
        elif r["status"] in ("Error", "Cancelled"):
            print(f"ERROR [{label}]: {r}")
            return None
    print(f"TIMEOUT [{label}]")
    return None

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python test_horizon_irc.py <DBX_PAT> <SNOWFLAKE_PAT>")
        sys.exit(1)
    
    dbx_token = sys.argv[1]
    sf_pat = sys.argv[2]
    
    ctx_id = get_context(dbx_token)
    
    # Test 1: Configure Horizon IRC catalog
    config_code = f'''
spark.conf.set("spark.sql.catalog.snowflake_horizon", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set("spark.sql.catalog.snowflake_horizon.type", "rest")
spark.conf.set("spark.sql.catalog.snowflake_horizon.uri", 
    "https://sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com/polaris/api/catalog")
spark.conf.set("spark.sql.catalog.snowflake_horizon.credential", "{sf_pat}")
spark.conf.set("spark.sql.catalog.snowflake_horizon.warehouse", "ICEBERG_POC")
spark.conf.set("spark.sql.catalog.snowflake_horizon.scope", "PRINCIPAL_ROLE:ALL")
print("IRC catalog configured")
'''
    run_code(dbx_token, ctx_id, config_code, "Configure IRC Catalog")
    
    # Test 2: List namespaces
    list_code = '''
try:
    df = spark.sql("SHOW NAMESPACES IN snowflake_horizon")
    df.show()
except Exception as e:
    print(f"Error listing namespaces: {e}")
'''
    run_code(dbx_token, ctx_id, list_code, "List Namespaces")
    
    # Test 3: List tables in EXTERNAL_ICEBERG
    tables_code = '''
try:
    df = spark.sql("SHOW TABLES IN snowflake_horizon.EXTERNAL_ICEBERG")
    df.show()
except Exception as e:
    print(f"Error listing tables: {e}")
'''
    run_code(dbx_token, ctx_id, tables_code, "List Tables")
    
    # Test 4: Read CUSTOMERS table
    read_code = '''
try:
    df = spark.read.table("snowflake_horizon.EXTERNAL_ICEBERG.CUSTOMERS")
    print(f"Row count: {df.count()}")
    df.show(5)
except Exception as e:
    print(f"Error reading table: {e}")
    import traceback
    traceback.print_exc()
'''
    run_code(dbx_token, ctx_id, read_code, "Read CUSTOMERS")
    
    # Test 5: Aggregation query
    agg_code = '''
try:
    df = spark.sql("""
        SELECT customer_tier, region, COUNT(*) as cnt
        FROM snowflake_horizon.EXTERNAL_ICEBERG.CUSTOMERS
        GROUP BY customer_tier, region
        ORDER BY cnt DESC
        LIMIT 10
    """)
    df.show()
except Exception as e:
    print(f"Error in aggregation: {e}")
'''
    run_code(dbx_token, ctx_id, agg_code, "Aggregation Query")
    
    print("\n=== TEST COMPLETE ===")
