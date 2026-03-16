# Databricks notebook source
# MAGIC %md
# MAGIC # Delta vs UniForm Interoperability Test
# MAGIC 
# MAGIC This notebook tests both Delta tables and UniForm (Delta + Iceberg) tables in the same Unity Catalog.

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql import functions as F

CATALOG = "gf_interop"
DELTA_SCHEMA = "delta_only"
UNIFORM_SCHEMA = "uniform"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Create UniForm Test Table

# COMMAND ----------

uniform_data = [
    (1, "UniForm Record 1", "2024-01-15", 150.50),
    (2, "UniForm Record 2", "2024-01-16", 250.75),
    (3, "UniForm Record 3", "2024-01-17", 350.25),
    (4, "UniForm Record 4", "2024-01-18", 450.00),
    (5, "UniForm Record 5", "2024-01-19", 550.99)
]

schema = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("created_date", StringType(), True),
    StructField("amount", DoubleType(), True)
])

df_uniform = spark.createDataFrame(uniform_data, schema)

df_uniform.write.format("delta") \
    .mode("overwrite") \
    .option("delta.universalFormat.enabledFormats", "iceberg") \
    .saveAsTable(f"{CATALOG}.{UNIFORM_SCHEMA}.uniform_test_table")

print("Created UniForm table: uniform_test_table")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Verify Both Tables

# COMMAND ----------

print("=== Delta-Only Table ===")
spark.table(f"{CATALOG}.{DELTA_SCHEMA}.delta_test_table").show()

print("\n=== UniForm Table ===")
spark.table(f"{CATALOG}.{UNIFORM_SCHEMA}.uniform_test_table").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## List All Tables

# COMMAND ----------

print(f"=== Tables in {DELTA_SCHEMA} ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.{DELTA_SCHEMA}").show()

print(f"\n=== Tables in {UNIFORM_SCHEMA} ===")
spark.sql(f"SHOW TABLES IN {CATALOG}.{UNIFORM_SCHEMA}").show()

# COMMAND ----------

print("Test complete! Now check Snowflake CLD to verify only UniForm tables are visible.")
