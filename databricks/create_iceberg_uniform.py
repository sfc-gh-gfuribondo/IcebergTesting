# Databricks notebook source
# MAGIC %md
# MAGIC # Create Unity Catalog with Iceberg/UniForm Table
# MAGIC 
# MAGIC This notebook creates:
# MAGIC 1. A Unity Catalog
# MAGIC 2. A schema for Iceberg tables
# MAGIC 3. An Iceberg table with UniForm enabled

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create Catalog (using default storage)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create catalog using default storage
# MAGIC CREATE CATALOG IF NOT EXISTS gf_iceberg_catalog
# MAGIC COMMENT 'Catalog for Iceberg/UniForm testing with Snowflake interop';

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Set as current catalog
# MAGIC USE CATALOG gf_iceberg_catalog;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create Schema

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS iceberg_schema
# MAGIC COMMENT 'Schema for Iceberg tables';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE SCHEMA iceberg_schema;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Create Iceberg Table with UniForm

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create Delta table with UniForm (Iceberg) enabled
# MAGIC CREATE TABLE IF NOT EXISTS customer_events_uniform (
# MAGIC     event_id BIGINT,
# MAGIC     customer_id BIGINT,
# MAGIC     event_type STRING,
# MAGIC     event_timestamp TIMESTAMP,
# MAGIC     event_data STRING,
# MAGIC     event_metadata STRING,
# MAGIC     created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
# MAGIC )
# MAGIC USING DELTA
# MAGIC TBLPROPERTIES (
# MAGIC     'delta.universalFormat.enabledFormats' = 'iceberg',
# MAGIC     'delta.enableIcebergCompatV2' = 'true'
# MAGIC )
# MAGIC COMMENT 'Customer events table with UniForm (Iceberg) enabled';

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 4: Insert Sample Data

# COMMAND ----------

# MAGIC %sql
# MAGIC INSERT INTO customer_events_uniform (event_id, customer_id, event_type, event_timestamp, event_data, event_metadata)
# MAGIC VALUES
# MAGIC     (1, 101, 'login', '2026-03-11 10:00:00', '{"device": "mobile", "browser": "Chrome"}', '{"session_id": "abc123"}'),
# MAGIC     (2, 101, 'purchase', '2026-03-11 10:15:00', '{"device": "mobile", "amount": 99.99}', '{"session_id": "abc123"}'),
# MAGIC     (3, 102, 'login', '2026-03-11 10:30:00', '{"device": "desktop", "browser": "Firefox"}', '{"session_id": "def456"}'),
# MAGIC     (4, 102, 'view', '2026-03-11 10:45:00', '{"device": "desktop", "page": "/products/123"}', '{"session_id": "def456"}'),
# MAGIC     (5, 103, 'login', '2026-03-11 11:00:00', '{"device": "tablet", "browser": "Safari"}', '{"session_id": "ghi789"}');

# COMMAND ----------

# Generate more data
from pyspark.sql.functions import *

df = spark.range(1000).select(
    (col("id") + 100).alias("event_id"),
    (100 + (col("id") % 50)).alias("customer_id"),
    when(col("id") % 5 == 0, "login")
        .when(col("id") % 5 == 1, "purchase")
        .when(col("id") % 5 == 2, "view")
        .when(col("id") % 5 == 3, "logout")
        .otherwise("click").alias("event_type"),
    (current_timestamp() - expr("INTERVAL id SECONDS")).alias("event_timestamp"),
    to_json(struct(
        when(col("id") % 3 == 0, "mobile")
            .when(col("id") % 3 == 1, "desktop")
            .otherwise("tablet").alias("device"),
        lit("Chrome").alias("browser")
    )).alias("event_data"),
    to_json(struct(
        expr("uuid()").alias("session_id")
    )).alias("event_metadata")
)

df.write.mode("append").saveAsTable("customer_events_uniform")
print("Inserted 1000 additional rows")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 5: Verify Table

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) as row_count FROM customer_events_uniform;

# COMMAND ----------

# MAGIC %sql
# MAGIC DESCRIBE EXTENDED customer_events_uniform;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TBLPROPERTIES customer_events_uniform;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 6: Get Iceberg Metadata Location

# COMMAND ----------

# Get table details including Iceberg metadata path
table_details = spark.sql("DESCRIBE DETAIL customer_events_uniform").collect()[0]
print(f"Table Location: {table_details['location']}")
print(f"Table Format: {table_details['format']}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SUCCESS
# MAGIC 
# MAGIC The table `gf_iceberg_catalog.iceberg_schema.customer_events_uniform` is now:
# MAGIC - A Delta table with UniForm enabled
# MAGIC - Readable as Iceberg format
# MAGIC - Can be accessed from Snowflake via Iceberg REST Catalog
