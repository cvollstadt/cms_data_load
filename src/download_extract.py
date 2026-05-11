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

def extract_csv_from_zip(zip_content: bytes) -> List[Tuple[str, bytes]]:
    """
    Extract all CSV files from zip content.
    
    Args:
        zip_content: Bytes content of zip file
    
    Returns:
        List of tuples (filename, csv_content)
    """
    csv_files = []
    
    try:
        with zipfile.ZipFile(io.BytesIO(zip_content)) as zip_file:
            for file_info in zip_file.filelist:
                filename = file_info.filename
                
                # Only extract CSV files, skip directories
                if filename.lower().endswith('.csv') and not file_info.is_dir():
                    csv_content = zip_file.read(filename)
                    
                    # Get just the basename if nested in folders
                    basename = os.path.basename(filename)
                    csv_files.append((basename, csv_content))
                    
                    print(f"✓ Extracted: {basename} ({len(csv_content)} bytes)")
        
        if not csv_files:
            print("⚠ No CSV files found in zip")
            
        return csv_files
        
    except zipfile.BadZipFile as e:
        print(f"✗ Bad zip file: {str(e)}")
        return []
    except Exception as e:
        print(f"✗ Extraction error: {str(e)}")
        return []

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

# Main execution
result = {
    "url": url,
    "status": "unknown",
    "csv_count": 0,
    "files_written": []
}

# Step 1: Download zip file
print(f"\n=== Step 1: Downloading from {url} ===")
zip_content, download_status = download_file(url)

if zip_content is None:
    result["status"] = download_status
    print(f"\n❌ Download failed: {download_status}")
    dbutils.notebook.exit(result)

# Step 2: Extract CSVs from zip
print(f"\n=== Step 2: Extracting CSVs ===")
csv_files = extract_csv_from_zip(zip_content)

if not csv_files:
    result["status"] = "no_csv_found"
    print(f"\n⚠ No CSV files extracted")
    dbutils.notebook.exit(result)

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
    if write_csv_to_volume(csv_content, filename, volume_path):
        success_count += 1
        result["files_written"].append(filename)

# Final result
if success_count == len(csv_files):
    result["status"] = "success"
    print(f"\n✅ Successfully processed {success_count}/{len(csv_files)} files")
else:
    result["status"] = "partial_success"
    print(f"\n⚠ Processed {success_count}/{len(csv_files)} files (some failures)")

# COMMAND ----------

# Return result
import json
dbutils.notebook.exit(json.dumps(result))
