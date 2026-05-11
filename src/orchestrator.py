# Databricks notebook source
# MAGIC %md
# MAGIC # CMS Data Load Orchestrator
# MAGIC
# MAGIC Main orchestration notebook that coordinates the entire CMS data loading pipeline.
# MAGIC
# MAGIC **Pipeline Flow:**
# MAGIC 1. Load table mappings and generate URL tasks
# MAGIC 2. Download & Extract: Call download_extract notebook for each URL
# MAGIC 3. Process with Auto Loader: Call autoloader_process for each table
# MAGIC 4. Generate summary report

# COMMAND ----------

import json
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("mode", "incremental", "Mode: backfill or incremental")
dbutils.widgets.text("start_year", "2020", "Start year (for backfill)")
dbutils.widgets.text("end_year", "2024", "End year (for backfill)")
dbutils.widgets.text("catalog", "sandbox", "Catalog name")
dbutils.widgets.text("schema", "cms_data_raw", "Schema name")
dbutils.widgets.text("max_parallel", "5", "Max parallel downloads")

# COMMAND ----------

# Get parameters
mode = dbutils.widgets.get("mode")
start_year = int(dbutils.widgets.get("start_year"))
end_year = int(dbutils.widgets.get("end_year"))
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
max_parallel = int(dbutils.widgets.get("max_parallel"))

print(f"=== CMS Data Load Orchestrator ===")
print(f"Mode: {mode}")
if mode == "backfill":
    print(f"Years: {start_year} to {end_year}")
print(f"Target: {catalog}.{schema}")
print(f"Max parallel downloads: {max_parallel}")
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

# MAGIC %md
# MAGIC ## Phase 1: Download & Extract
# MAGIC
# MAGIC Download zip files and extract CSVs to Volume landing zone.

# COMMAND ----------

def download_task(task):
    """Execute download_extract notebook for a single task"""
    try:
        params = {
            "url": task["url"],
            "landing_path": task["landing_path"],
            "catalog": catalog,
            "schema": schema,
            "year": str(task["year"]) if task["year"] else "",
            "month": str(task["month"]) if task["month"] else ""
        }
        
        result = dbutils.notebook.run(
            "./download_extract",
            timeout_seconds=600,  # 10 minute timeout per file
            arguments=params
        )
        
        return {
            "task": task,
            "result": json.loads(result) if result else {},
            "success": True
        }
    except Exception as e:
        return {
            "task": task,
            "error": str(e),
            "success": False
        }

# COMMAND ----------

print(f"\n=== Phase 1: Downloading and Extracting {len(url_tasks)} files ===\n")

download_results = []
success_count = 0
notfound_count = 0
error_count = 0

# Process downloads with limited parallelism
with ThreadPoolExecutor(max_workers=max_parallel) as executor:
    futures = {executor.submit(download_task, task): task for task in url_tasks}
    
    for i, future in enumerate(as_completed(futures), 1):
        result = future.result()
        download_results.append(result)
        
        task = result["task"]
        table_name = task["table_name"]
        
        if result["success"]:
            status = result["result"].get("status", "unknown")
            if status == "success":
                success_count += 1
                print(f"✓ [{i}/{len(url_tasks)}] {table_name}: Success")
            elif status == "not_found":
                notfound_count += 1
                print(f"⚠ [{i}/{len(url_tasks)}] {table_name}: Not found (404)")
            else:
                error_count += 1
                print(f"✗ [{i}/{len(url_tasks)}] {table_name}: {status}")
        else:
            error_count += 1
            print(f"✗ [{i}/{len(url_tasks)}] {table_name}: {result.get('error', 'Unknown error')}")

print(f"\n=== Download Summary ===")
print(f"Success: {success_count}")
print(f"Not found: {notfound_count}")
print(f"Errors: {error_count}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Phase 2: Auto Loader Processing
# MAGIC
# MAGIC Process CSVs from Volume landing zone into Delta tables using Auto Loader.

# COMMAND ----------

# Get unique tables that have successful downloads
successful_tables = set()
for result in download_results:
    if result["success"] and result["result"].get("status") == "success":
        successful_tables.add(result["task"]["table_name"])

print(f"\n=== Phase 2: Processing {len(successful_tables)} tables with Auto Loader ===\n")

# COMMAND ----------

# DBTITLE 1,Cell 14
processing_results = []

for i, table_name in enumerate(sorted(successful_tables), 1):
    # Find landing path for this table
    landing_path = next((m["landing_path"] for m in mappings if m["table_name"] == table_name), table_name)
    table_comment = next((m["comment"] for m in mappings if m["table_name"] == table_name), "")
    
    source_path = f"/Volumes/{catalog}/{schema}/cms_landing/{landing_path}"
    
    print(f"[{i}/{len(successful_tables)}] Processing {table_name}...")
    
    try:
        params = {
            "source_path": source_path,
            "table_name": table_name,
            "catalog": catalog,
            "schema": schema,
            "checkpoint_path": "",  # Will auto-generate
            "comment": table_comment
        }
        
        result = dbutils.notebook.run(
            "./autoloader_process",
            timeout_seconds=1800,  # 30 minute timeout per table
            arguments=params
        )
        
        result_dict = json.loads(result) if result else {}
        processing_results.append({
            "table_name": table_name,
            "result": result_dict,
            "success": True
        })
        
        total_rows = result_dict.get("total_rows", 0)
        print(f"  ✓ Loaded {total_rows:,} rows")
        
    except Exception as e:
        processing_results.append({
            "table_name": table_name,
            "error": str(e),
            "success": False
        })
        print(f"  ✗ Error: {str(e)}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Final Summary

# COMMAND ----------

# Calculate final statistics
successful_processing = sum(1 for r in processing_results if r["success"])
failed_processing = len(processing_results) - successful_processing

total_rows_loaded = sum(r["result"].get("total_rows", 0) for r in processing_results if r["success"])

summary = {
    "mode": mode,
    "start_time": datetime.now().isoformat(),
    "downloads": {
        "total": len(url_tasks),
        "success": success_count,
        "not_found": notfound_count,
        "errors": error_count
    },
    "processing": {
        "tables_processed": len(successful_tables),
        "successful": successful_processing,
        "failed": failed_processing,
        "total_rows_loaded": total_rows_loaded
    },
    "configuration": {
        "catalog": catalog,
        "schema": schema,
        "start_year": start_year,
        "end_year": end_year
    }
}

print("\n" + "="*60)
print("=== FINAL SUMMARY ===")
print("="*60)
print(f"\nMode: {mode}")
print(f"\nDownloads:")
print(f"  Total tasks: {len(url_tasks)}")
print(f"  ✓ Success: {success_count}")
print(f"  ⚠ Not found: {notfound_count}")
print(f"  ✗ Errors: {error_count}")
print(f"\nProcessing:")
print(f"  Tables processed: {len(successful_tables)}")
print(f"  ✓ Successful: {successful_processing}")
print(f"  ✗ Failed: {failed_processing}")
print(f"  Total rows loaded: {total_rows_loaded:,}")
print(f"\nTarget: {catalog}.{schema}")
print("="*60)

# COMMAND ----------

# Return summary
import json
dbutils.notebook.exit(json.dumps(summary))
