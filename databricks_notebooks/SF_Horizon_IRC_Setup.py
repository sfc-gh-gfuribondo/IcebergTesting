# Databricks notebook source
# MAGIC %md
# MAGIC # Snowflake Horizon IRC Setup
# MAGIC 
# MAGIC Configure Spark to connect to Snowflake Iceberg tables via the Horizon REST Catalog (IRC) endpoint.
# MAGIC 
# MAGIC ## Prerequisites
# MAGIC - Databricks secret scope `snowflake-irc` with key `pat` containing Snowflake PAT
# MAGIC - Cluster with Iceberg runtime (Spark 3.x + Iceberg 1.4+)
# MAGIC - Network access to Snowflake endpoint

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configuration Parameters

# COMMAND ----------

SNOWFLAKE_ACCOUNT = "sfsenorthamerica-demo_gfuribondo2"
SNOWFLAKE_IRC_URI = f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com/polaris/api/catalog"
SNOWFLAKE_WAREHOUSE = "ICEBERG_POC"
CATALOG_NAME = "snowflake_horizon"

print(f"Snowflake Account: {SNOWFLAKE_ACCOUNT}")
print(f"IRC Endpoint: {SNOWFLAKE_IRC_URI}")
print(f"Warehouse: {SNOWFLAKE_WAREHOUSE}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Configure Snowflake Horizon IRC Catalog

# COMMAND ----------

SF_PAT = dbutils.secrets.get(scope="snowflake-irc", key="pat")

spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}", "org.apache.iceberg.spark.SparkCatalog")
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.type", "rest")
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.uri", SNOWFLAKE_IRC_URI)
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.credential", SF_PAT)
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.warehouse", SNOWFLAKE_WAREHOUSE)
spark.conf.set(f"spark.sql.catalog.{CATALOG_NAME}.scope", "PRINCIPAL_ROLE:ALL")

print(f"Catalog '{CATALOG_NAME}' configured successfully")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Connection

# COMMAND ----------

try:
    namespaces_df = spark.sql(f"SHOW NAMESPACES IN {CATALOG_NAME}")
    print("Connection successful! Available namespaces:")
    namespaces_df.show()
except Exception as e:
    print(f"Connection failed: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Storage Endpoint Configuration (if needed)
# MAGIC 
# MAGIC Snowflake uses `azure://...blob.core.windows.net` paths. If Spark has issues resolving these,
# MAGIC configure Azure storage access below.

# COMMAND ----------

STORAGE_ACCOUNT = "gfstorageaccountwest2"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option A: Using Storage Account Key (from secrets)

# COMMAND ----------

try:
    storage_key = dbutils.secrets.get(scope="azure-storage", key=STORAGE_ACCOUNT)
    spark.conf.set(f"fs.azure.account.key.{STORAGE_ACCOUNT}.blob.core.windows.net", storage_key)
    print(f"Azure storage key configured for {STORAGE_ACCOUNT}")
except Exception as e:
    print(f"Storage key not found in secrets (may not be needed if vended credentials work): {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Option B: Using OAuth / Service Principal (recommended for production)

# COMMAND ----------

# Uncomment and configure if using Service Principal authentication
# spark.conf.set(f"fs.azure.account.auth.type.{STORAGE_ACCOUNT}.blob.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{STORAGE_ACCOUNT}.blob.core.windows.net", 
#     "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{STORAGE_ACCOUNT}.blob.core.windows.net", "<client-id>")
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{STORAGE_ACCOUNT}.blob.core.windows.net", 
#     dbutils.secrets.get(scope="azure-sp", key="client-secret"))
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{STORAGE_ACCOUNT}.blob.core.windows.net", 
#     "https://login.microsoftonline.com/<tenant-id>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Setup Complete
# MAGIC 
# MAGIC Run the `SF_Horizon_IRC_Tests` notebook to test reading Snowflake Iceberg tables.
