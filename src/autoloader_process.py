# Databricks notebook source
# MAGIC %md
# MAGIC # Auto Loader Processing for CMS Data
# MAGIC
# MAGIC Uses Auto Loader to incrementally read CSVs from Volume and append to Delta tables.
# MAGIC
# MAGIC **Benefits of Auto Loader:**
# MAGIC * Automatic file tracking (no need for custom audit table)
# MAGIC * Efficient incremental processing
# MAGIC * Schema inference and evolution
# MAGIC * Built-in checkpointing and exactly-once semantics

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
from datetime import datetime

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("source_path", "", "Source path in Volume (e.g., /Volumes/catalog/schema/cms_landing/table_name)")
dbutils.widgets.text("table_name", "", "Target table name")
dbutils.widgets.text("catalog", "sandbox", "Catalog name")
dbutils.widgets.text("schema", "cms_data_raw", "Schema name")
dbutils.widgets.text("checkpoint_path", "", "Checkpoint location (optional, will auto-generate if empty)")

# COMMAND ----------

# Get parameters
source_path = dbutils.widgets.get("source_path")
table_name = dbutils.widgets.get("table_name")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
checkpoint_path = dbutils.widgets.get("checkpoint_path")

# Generate checkpoint path if not provided
if not checkpoint_path:
    checkpoint_path = f"/Volumes/{catalog}/{schema}/cms_checkpoints/{table_name}"

full_table_name = f"{catalog}.{schema}.{table_name}"

print(f"Source path: {source_path}")
print(f"Target table: {full_table_name}")
print(f"Checkpoint path: {checkpoint_path}")

# COMMAND ----------

# Verify source path exists
import json

try:
    files = dbutils.fs.ls(source_path)
    print(f"✓ Source path exists with {len(files)} entries")
except Exception as e:
    print(f"⚠ Source path not found or empty: {source_path}")
    dbutils.notebook.exit(json.dumps({
        "status": "no_source_data",
        "message": f"Source path {source_path} does not exist or is empty"
    }))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Auto Loader Configuration
# MAGIC
# MAGIC Key options:
# MAGIC * `cloudFiles.format`: csv
# MAGIC * `cloudFiles.schemaLocation`: Auto schema inference location
# MAGIC * `cloudFiles.inferColumnTypes`: true (infer column types automatically)
# MAGIC * `cloudFiles.schemaEvolutionMode`: addNewColumns (handle schema changes)

# COMMAND ----------

# Read CSVs using Auto Loader
df = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "csv")
    .option("header", "true")
    .option("cloudFiles.schemaLocation", f"{checkpoint_path}/schema")
    .option("cloudFiles.inferColumnTypes", "true")
    .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
    .option("cloudFiles.maxFilesPerTrigger", 100)  # Process up to 100 files per batch
    .load(source_path)
)

# COMMAND ----------

# Add metadata columns for lineage and tracking
df_with_metadata = (df
    .withColumn("_load_timestamp", current_timestamp())
    .withColumn("_source_file", input_file_name())
    .withColumn("_processing_date", lit(datetime.now().strftime("%Y-%m-%d")))
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Write to Delta Table
# MAGIC
# MAGIC Using structured streaming with trigger once for batch processing.
# MAGIC * `append` mode: Additive process as required
# MAGIC * `mergeSchema`: true: Handle schema evolution
# MAGIC * `trigger(availableNow=True)`: Process all available files then stop

# COMMAND ----------

# Create table if it doesn't exist
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {full_table_name}
    USING DELTA
    LOCATION '/mnt/cms_data/{table_name}'
    COMMENT 'CMS data loaded via Auto Loader'
""")

print(f"✓ Table {full_table_name} ready")

# COMMAND ----------

# Write stream to Delta table
print(f"Processing files from {source_path}...")

query = (df_with_metadata.writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")
    .trigger(availableNow=True)  # Process all available files then stop
    .toTable(full_table_name)
)

# Wait for the stream to finish processing
query.awaitTermination()

print("✓ Auto Loader processing complete")

# COMMAND ----------

# Get processing statistics
row_count = spark.table(full_table_name).count()
latest_load = spark.table(full_table_name).select(max("_load_timestamp")).collect()[0][0]

result = {
    "status": "success",
    "table_name": full_table_name,
    "total_rows": row_count,
    "latest_load_timestamp": str(latest_load) if latest_load else None,
    "checkpoint_path": checkpoint_path
}

print(f"\n✅ Processed to {full_table_name}")
print(f"   Total rows: {row_count:,}")
print(f"   Latest load: {latest_load}")

# COMMAND ----------

# Return result
import json
dbutils.notebook.exit(json.dumps(result))
