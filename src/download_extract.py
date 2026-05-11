# Databricks notebook source
# MAGIC %md
# MAGIC # Download and Extract CMS Data Files
# MAGIC
# MAGIC Downloads zip files from CMS URLs, extracts CSVs, and lands them in Unity Catalog Volumes.

# COMMAND ----------

# MAGIC %pip install requests

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import requests
import zipfile
import io
import os
import tempfile
import hashlib
from typing import List, Dict, Tuple, Optional
from pyspark.sql import DataFrame

# COMMAND ----------

# DBTITLE 1,Additional imports for audit logging
import json
import uuid
from datetime import datetime
from pyspark.sql import Row

# COMMAND ----------

# Widget parameters
dbutils.widgets.text("url", "", "URL to download")
dbutils.widgets.text("landing_path", "", "Landing path in Volume")
dbutils.widgets.text("catalog", "sandbox", "Catalog name")
dbutils.widgets.text("schema", "cms_data_raw", "Schema name")
dbutils.widgets.text("year", "", "Year (optional)")
dbutils.widgets.text("month", "", "Month (optional)")

# COMMAND ----------

# Get parameters
url = dbutils.widgets.get("url")
landing_path = dbutils.widgets.get("landing_path")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
year_str = dbutils.widgets.get("year")
month_str = dbutils.widgets.get("month")

year = int(year_str) if year_str else None
month = int(month_str) if month_str else None

print(f"Downloading: {url}")
print(f"Landing path: {landing_path}")
print(f"Year: {year}, Month: {month}")

# COMMAND ----------

# DBTITLE 1,Create audit table
# Create audit table if it doesn't exist
audit_table_name = f"{catalog}.{schema}.cms_pipeline_audit"

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {audit_table_name} (
        run_id STRING,
        run_timestamp TIMESTAMP,
        url STRING,
        landing_path STRING,
        catalog STRING,
        schema STRING,
        year INT,
        month INT,
        download_status STRING,
        download_error_msg STRING,
        files_in_zip ARRAY<STRING>,
        files_written ARRAY<STRUCT<filename: STRING, size_bytes: BIGINT>>,
        csv_count INT,
        success_count INT
    )
    USING DELTA
    COMMENT 'Audit log for CMS data download and extract pipeline'
""")

print(f"Audit table: {audit_table_name}")

# COMMAND ----------

def download_file(url: str, timeout: int = 300) -> Tuple[bytes, str]:
    """
    Download file from URL.
    
    Args:
        url: URL to download
        timeout: Request timeout in seconds
    
    Returns:
        Tuple of (file_content, status_message)
    """
    try:
        response = requests.get(url, timeout=timeout, stream=True)
        response.raise_for_status()
        
        content = response.content
        file_hash = hashlib.md5(content).hexdigest()
        
        print(f"✓ Downloaded {len(content)} bytes (MD5: {file_hash})")
        return content, "success"
        
    except requests.exceptions.HTTPError as e:
        if e.response.status_code == 404:
            print(f"⚠ File not found (404): {url}")
            return None, "not_found"
        else:
            print(f"✗ HTTP error: {e}")
            return None, f"http_error_{e.response.status_code}"
    except Exception as e:
        print(f"✗ Download error: {str(e)}")
        return None, f"error: {str(e)}"

# COMMAND ----------

# DBTITLE 1,Cell 10
def extract_csv_from_zip(zip_content: bytes) -> Tuple[List[Tuple[str, bytes]], List[str]]:
    """
    Extract all CSV files from zip content and list all files in archive.
    
    Args:
        zip_content: Bytes content of zip file
    
    Returns:
        Tuple of (csv_files, all_files_in_zip)
        - csv_files: List of tuples (filename, csv_content) for CSV files only
        - all_files_in_zip: List of all filenames found in the zip archive
    """
    csv_files = []
    all_files = []
    
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_info in zip_file.filelist:
                filename = file_info.filename
                
                # Skip directories
                if not file_info.is_dir():
                    basename = os.path.basename(filename)
                    all_files.append(basename)
                    
                    # Only extract CSV files
                    if filename.lower().endswith('.csv'):
                        csv_content = zip_file.read(filename)
                        csv_files.append((basename, csv_content))
                        
                        print(f"✓ Extracted CSV: {basename} ({len(csv_content)} bytes)")
                    else:
                        print(f"ℹ Found non-CSV: {basename}")
        
        if not csv_files:
            print("⚠ No CSV files found in zip")
        
        print(f"\nSummary: {len(csv_files)} CSV files, {len(all_files)} total files in archive")
        return csv_files, all_files
        
    except zipfile.BadZipFile as e:
        print(f"✗ Bad zip file: {str(e)}")
        return [], []
    except Exception as e:
        print(f"✗ Extraction error: {str(e)}")
        return [], []

# COMMAND ----------

def write_csv_to_volume(csv_content: bytes, filename: str, volume_path: str) -> bool:
    """
    Write CSV content to Unity Catalog Volume.
    
    Args:
        csv_content: CSV file content as bytes
        filename: Name of the CSV file
        volume_path: Full path in Volume
    
    Returns:
        True if successful, False otherwise
    """
    try:
        # Ensure directory exists using dbutils.fs (Volumes require dbutils, not os.makedirs)
        dbutils.fs.mkdirs(volume_path)
        
        # Build full file path
        file_path = f"{volume_path}/{filename}"
        
        # Write file using dbutils.fs (decode bytes to string for put)
        # Note: dbutils.fs.put expects string content
        content_str = csv_content.decode('utf-8')
        dbutils.fs.put(file_path, content_str, overwrite=True)
        
        file_size = len(csv_content)
        print(f"✓ Wrote {file_path} ({file_size} bytes)")
        
        return True
        
    except Exception as e:
        print(f"✗ Write error for {filename}: {str(e)}")
        return False

# COMMAND ----------

# DBTITLE 1,Audit logging function
def write_audit_record(
    run_id: str,
    run_timestamp: datetime,
    url: str,
    landing_path: str,
    catalog: str,
    schema: str,
    year: Optional[int],
    month: Optional[int],
    download_status: str,
    download_error_msg: Optional[str],
    files_in_zip: List[str],
    files_written: List[Dict[str, any]],
    csv_count: int,
    success_count: int
) -> None:
    """
    Write audit record to Delta table.
    
    Args:
        run_id: Unique identifier for this run
        run_timestamp: Timestamp of the run
        url: URL that was downloaded
        landing_path: Path where files were landed
        catalog: Catalog name
        schema: Schema name
        year: Year (optional)
        month: Month (optional)
        download_status: Status (success, not_found, error, etc.)
        download_error_msg: Error message if failed
        files_in_zip: List of all CSV filenames found in zip
        files_written: List of dicts with filename and size_bytes
        csv_count: Number of CSVs found
        success_count: Number successfully written
    """
    try:
        audit_table = f"{catalog}.{schema}.cms_pipeline_audit"
        
        # Create audit record row
        audit_row = Row(
            run_id=run_id,
            run_timestamp=run_timestamp,
            url=url,
            landing_path=landing_path,
            catalog=catalog,
            schema=schema,
            year=year,
            month=month,
            download_status=download_status,
            download_error_msg=download_error_msg,
            files_in_zip=files_in_zip,
            files_written=[Row(**f) for f in files_written] if files_written else [],
            csv_count=csv_count,
            success_count=success_count
        )
        
        # Write to audit table
        audit_df = spark.createDataFrame([audit_row])
        audit_df.write.mode("append").saveAsTable(audit_table)
        
        print(f"✓ Audit record written: {run_id}")
        
    except Exception as e:
        # Don't fail the main process if audit logging fails
        print(f"⚠ Failed to write audit record: {str(e)}")

# COMMAND ----------

# DBTITLE 1,Cell 10
# Main execution
import json

# Generate unique run ID and timestamp
run_id = str(uuid.uuid4())
run_timestamp = datetime.now()

print(f"Run ID: {run_id}")
print(f"Run Timestamp: {run_timestamp}")

result = {
    "run_id": run_id,
    "url": url,
    "status": "unknown",
    "csv_count": 0,
    "files_written": []
}

# Initialize audit tracking
files_in_zip = []
files_written_audit = []  # List of {filename, size_bytes}

# Step 1: Download zip file
print(f"\n=== Step 1: Downloading from {url} ===")
zip_content, download_status = download_file(url)

if zip_content is None:
    result["status"] = download_status
    print(f"\n❌ Download failed: {download_status}")
    
    # Write audit record for failed download
    write_audit_record(
        run_id=run_id,
        run_timestamp=run_timestamp,
        url=url,
        landing_path=landing_path,
        catalog=catalog,
        schema=schema,
        year=year,
        month=month,
        download_status=download_status,
        download_error_msg=download_status,
        files_in_zip=[],
        files_written=[],
        csv_count=0,
        success_count=0
    )
    
    dbutils.notebook.exit(json.dumps(result))

# Step 2: Extract CSVs from zip
print(f"\n=== Step 2: Extracting CSVs ===")
csv_files, files_in_zip = extract_csv_from_zip(zip_content)

if not csv_files:
    result["status"] = "no_csv_found"
    print(f"\n⚠ No CSV files extracted")
    
    # Write audit record for no CSVs (but capture all files found)
    write_audit_record(
        run_id=run_id,
        run_timestamp=run_timestamp,
        url=url,
        landing_path=landing_path,
        catalog=catalog,
        schema=schema,
        year=year,
        month=month,
        download_status="no_csv_found",
        download_error_msg="No CSV files found in zip archive",
        files_in_zip=files_in_zip,  # Now captures ALL files
        files_written=[],
        csv_count=0,
        success_count=0
    )
    
    dbutils.notebook.exit(json.dumps(result))

result["csv_count"] = len(csv_files)

# Step 3: Write CSVs to Volume
print(f"\n=== Step 3: Writing to Volume ===")

# Build volume path
if year and month:
    volume_path = f"/Volumes/{catalog}/{schema}/cms_landing/{landing_path}/year={year}/month={month:02d}"
elif year:
    volume_path = f"/Volumes/{catalog}/{schema}/cms_landing/{landing_path}/year={year}"
else:
    volume_path = f"/Volumes/{catalog}/{schema}/cms_landing/{landing_path}"

print(f"Target path: {volume_path}")

success_count = 0
for filename, csv_content in csv_files:
    size_bytes = len(csv_content)
    
    if write_csv_to_volume(csv_content, filename, volume_path):
        success_count += 1
        result["files_written"].append(filename)
        files_written_audit.append({
            "filename": filename,
            "size_bytes": size_bytes
        })

# Determine final status
if success_count == len(csv_files):
    result["status"] = "success"
    final_status = "success"
    error_msg = None
    print(f"\n✅ Successfully processed {success_count}/{len(csv_files)} files")
else:
    result["status"] = "partial_success"
    final_status = "partial_success"
    error_msg = f"Only {success_count}/{len(csv_files)} files written successfully"
    print(f"\n⚠ Processed {success_count}/{len(csv_files)} files (some failures)")

# Write audit record for success/partial success
write_audit_record(
    run_id=run_id,
    run_timestamp=run_timestamp,
    url=url,
    landing_path=landing_path,
    catalog=catalog,
    schema=schema,
    year=year,
    month=month,
    download_status=final_status,
    download_error_msg=error_msg,
    files_in_zip=files_in_zip,  # Now captures ALL files in the zip
    files_written=files_written_audit,
    csv_count=len(csv_files),
    success_count=success_count
)

# COMMAND ----------

# DBTITLE 1,Cell 11
# Return result with run_id for tracking
import json

print(f"\nReturning result for run: {run_id}")
dbutils.notebook.exit(json.dumps(result))
