# Databricks notebook source
# MAGIC %md
# MAGIC # Create Snowflake Foreign Catalog in Unity Catalog
# MAGIC 
# MAGIC Creates a Foreign Catalog connection to Snowflake's Iceberg REST Catalog (IRC) using PAT authentication.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 1: Create the Connection

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CONNECTION IF NOT EXISTS snowflake_iceberg_conn
# MAGIC TYPE iceberg_rest
# MAGIC OPTIONS (
# MAGIC   host 'sfsenorthamerica-demo_gfuribondo2.snowflakecomputing.com',
# MAGIC   warehouse 'ICEBERG_POC',
# MAGIC   token secret('snowflake-irc', 'pat')
# MAGIC );

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 2: Create the Foreign Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FOREIGN CATALOG IF NOT EXISTS snowflake_iceberg
# MAGIC USING CONNECTION snowflake_iceberg_conn;

# COMMAND ----------

# MAGIC %md
# MAGIC ## Step 3: Verify the Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW CATALOGS LIKE 'snowflake*';

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW SCHEMAS IN snowflake_iceberg;

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES IN snowflake_iceberg.public;
