# Databricks notebook source
# DBTITLE 1,Cell 1
# MAGIC %md
# MAGIC # CMS Data Load Orchestrator - Task Generator
# MAGIC
# MAGIC Generates the list of download tasks to be executed by the job's For Each task.
# MAGIC
# MAGIC **New Pipeline Flow:**
# MAGIC 1. Load table mappings and generate URL tasks
# MAGIC 2. Output tasks as JSON array for For Each task
# MAGIC 3. (Job orchestration handles parallel downloads)
# MAGIC 4. Aggregate results and process with Auto Loader

# COMMAND ----------

# DBTITLE 1,Cell 2
import json
from datetime import datetime

# COMMAND ----------

# DBTITLE 1,Cell 3
# Widget parameters
dbutils.widgets.text("mode", "incremental", "Mode: backfill or incremental")
dbutils.widgets.text("start_year", "2020", "Start year (for backfill)")
dbutils.widgets.text("end_year", "2024", "End year (for backfill)")
dbutils.widgets.text("catalog", "sandbox", "Catalog name")
dbutils.widgets.text("schema", "cms_data_raw", "Schema name")

# COMMAND ----------

# DBTITLE 1,Cell 4
# Get parameters
mode = dbutils.widgets.get("mode")
start_year = int(dbutils.widgets.get("start_year"))
end_year = int(dbutils.widgets.get("end_year"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")

print(f"=== CMS Data Load Orchestrator - Task Generator ===")
print(f"Mode: {mode}")
if mode == "backfill":
    print(f"Years: {start_year} to {end_year}")
print(f"Target: {catalog}.{schema}")
print()

# COMMAND ----------

# MAGIC %run ./utils

# COMMAND ----------

# DBTITLE 1,Cell 6
# Load table mappings
with open("/Workspace/Users/cvollstadt@gmail.com/cms_data_load/src/table_mappings.json", "r") as f:
    config = json.load(f)
    mappings = config["mappings"]

print(f"✓ Loaded {len(mappings)} table mappings")

# COMMAND ----------

# Generate date ranges based on mode
date_ranges = generate_date_ranges(start_year, end_year, mode)

print(f"✓ Generated {len(date_ranges)} date range(s)")
if mode == "incremental":
    dr = date_ranges[0]
    print(f"  Processing: {dr['month_name']} {dr['year']} ({dr['quarter']})")
else:
    print(f"  From: {date_ranges[0]['month_name']} {date_ranges[0]['year']}")
    print(f"  To: {date_ranges[-1]['month_name']} {date_ranges[-1]['year']}")

# COMMAND ----------

# Build URL tasks
url_tasks = build_url_tasks(mappings, date_ranges)

print(f"\n✓ Generated {len(url_tasks)} download tasks across {len(set(t['table_name'] for t in url_tasks))} tables")

# Show sample tasks
print("\nSample tasks:")
for task in url_tasks[:3]:
    print(f"  - {task['table_name']}: {task['url'][:80]}...")

# COMMAND ----------

# DBTITLE 1,Output tasks for For Each
# Output tasks array for For Each task
import json

print(f"\n✓ Generated {len(url_tasks)} tasks")
print(f"\nOutputting task array for For Each execution...")

dbutils.notebook.exit(json.dumps(url_tasks))
