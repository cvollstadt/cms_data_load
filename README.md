# CMS Data Load Pipeline

Automated data pipeline for downloading, extracting, and processing CMS (Centers for Medicare & Medicaid Services) public data files into Delta Lake tables using Databricks Jobs and Auto Loader.

## Overview

This pipeline automates the ingestion of 26 different CMS datasets from public URLs, covering Medicare Advantage and Part D plan enrollment data, quality metrics, and supplemental information. The pipeline supports both historical backfill (5 years) and ongoing monthly incremental loads.

### Key Features

* **Automated Downloads**: Downloads and extracts CSV files from zip archives
* **Auto Loader Processing**: Incremental file processing with automatic tracking
* **Schema Evolution**: Automatically handles schema changes over time
* **Parallel Processing**: Configurable concurrency for efficient downloads
* **Error Handling**: Graceful handling of missing files (404s) and corrupt data
* **Idempotency**: Safe to rerun - Auto Loader tracks already-processed files
* **Metadata Tracking**: Each row includes load timestamp, source file, and processing date

---

## Architecture

### Two-Phase Processing

**Phase 1: Download & Extract**
1. Orchestrator generates URL tasks from configuration
2. Downloads zip files from CMS.gov in parallel
3. Extracts CSVs to Unity Catalog Volumes landing zone
4. Organizes files by dataset/year/month

**Phase 2: Auto Loader Processing**
1. Auto Loader reads CSVs from landing zone
2. Infers schema with column type detection
3. Appends to Delta tables with metadata columns
4. Maintains checkpoints for incremental processing

### Components

```
cms_data_load/
├── src/
│   ├── table_mappings.json      # Maps URL patterns to table names
│   ├── utils.py                  # Helper functions (URL expansion, dates)
│   ├── download_extract.py       # Downloads zips, extracts CSVs
│   ├── autoloader_process.py    # Auto Loader → Delta processing
│   └── orchestrator.py           # Main pipeline coordinator
├── resources/
│   ├── volumes.volume.yml        # Unity Catalog Volumes
│   ├── backfill_job.job.yml     # Historical backfill job
│   └── monthly_job.job.yml       # Scheduled monthly job
├── target_files.json             # List of 26 CMS URL patterns
└── databricks.yml                # Bundle configuration
```

---

## Data Sources

The pipeline processes 26 CMS datasets with parameterized URLs:

* **Monthly Enrollment**: Contract, CPSC, Plan, State-level data
* **Geographic Enrollment**: State/County breakdowns (MA & PDP)
* **Penetration Reports**: MA and PDP market penetration
* **Service Areas**: Contract and plan service area definitions
* **Quality Metrics**: HEDIS Public Use Files (MA & SNP)
* **Plan Information**: Plan directories and crosswalks
* **Special Programs**: SNP comprehensive reports, LIS enrollment

### URL Parameterization

URLs support three parameters:
* `{year}`: Four-digit year (e.g., 2024)
* `{month_name}`: Lowercase month name (e.g., january)
* `{quarter}`: Quarter designation (q1, q2, q3, q4)

---

## Setup

### Prerequisites

* Databricks workspace (AWS, Azure, or GCP)
* Unity Catalog enabled
* Catalog and schema created for target tables
* Email address for job notifications

### 1. Configure Variables

Edit `databricks.yml` to set your target catalog and schema:

```yaml
variables:
  catalog:
    description: Unity Catalog name
    default: sandbox
  
  schema:
    description: Schema name
    default: cms_data_raw
```

### 2. Deploy the Bundle

```bash
# Validate configuration
databricks bundle validate

# Deploy to dev environment
databricks bundle deploy --target dev

# Or deploy to production
databricks bundle deploy --target prod
```

This creates:
* 2 Unity Catalog Volumes (`cms_landing`, `cms_checkpoints`)
* 2 Databricks Jobs (backfill and monthly)
* All notebooks in the workspace

### 3. Verify Deployment

```bash
# List deployed resources
databricks bundle summary

# Check job status
databricks jobs list | grep "CMS Data"
```

---

## Usage

### Historical Backfill (One-Time)

Load 5 years of historical data (2020-2024):

```bash
# Run via CLI
databricks bundle run cms_backfill_job

# Or via workspace UI
# Navigate to Workflows > Jobs > "CMS Data Historical Backfill" > Run Now
```

**Parameters:**
* `start_year`: Starting year (default: 2020)
* `end_year`: Ending year (default: 2024)
* `max_parallel`: Parallel download threads (default: 10)

**Expected Duration:** 6-12 hours depending on data size and network speed

### Monthly Incremental Load (Scheduled)

Automatically runs on the 5th of each month at midnight ET to load previous month's data.

**Manual Trigger:**
```bash
databricks bundle run cms_monthly_incremental
```

**Schedule:** `0 0 5 * *` (5th of month, 00:00 EST)

**Expected Duration:** 1-2 hours

### Monitoring

**Via CLI:**
```bash
# Get latest job run
databricks jobs runs list --job-name "CMS Data Monthly Incremental Load" --limit 1

# Get run details
databricks jobs runs get --run-id <run_id>
```

**Via UI:**
* Navigate to **Workflows > Jobs**
* Click job name to view run history
* Click run to view detailed logs and metrics

---

## Configuration

### Table Mappings

Edit `src/table_mappings.json` to add/modify datasets:

```json
{
  "mappings": [
    {
      "url_pattern": "https://www.cms.gov/files/zip/...-{month_name}-{year}.zip",
      "table_name": "target_table_name",
      "parameters": ["month_name", "year"],
      "landing_path": "landing_folder_name"
    }
  ]
}
```

### Job Configuration

**Backfill Job** (`resources/backfill_job.job.yml`):
* Larger cluster (4 workers)
* 12-hour timeout
* High parallelism (10 concurrent downloads)

**Monthly Job** (`resources/monthly_job.job.yml`):
* Smaller cluster (2-4 workers with autoscaling)
* 2-hour timeout
* Moderate parallelism (5 concurrent downloads)
* Scheduled via Quartz cron expression

### Email Notifications

Update email addresses in job YAML files:

```yaml
email_notifications:
  on_success:
    - your-email@example.com
  on_failure:
    - your-email@example.com
```

---

## Data Organization

### Landing Zone (Volumes)

CSVs are extracted to:
```
/Volumes/{catalog}/{schema}/cms_landing/{table_name}/year={year}/month={month}/
```

Example:
```
/Volumes/sandbox/cms_data_raw/cms_landing/
├── enrollment_contract/
│   ├── year=2020/month=01/data.csv
│   ├── year=2020/month=02/data.csv
│   └── ...
├── ma_hedis_pufs/
│   ├── year=2020/data.csv
│   └── ...
└── ma_plan_directory/
    └── data.csv  (no year/month partition)
```

### Delta Tables

Target tables in Unity Catalog:
```
{catalog}.{schema}.{table_name}
```

**Standard Columns:**
* All original CSV columns (auto-detected)
* `_load_timestamp`: When record was loaded
* `_source_file`: Original CSV file path
* `_processing_date`: Date of processing batch

### Checkpoints

Auto Loader checkpoints stored in:
```
/Volumes/{catalog}/{schema}/cms_checkpoints/{table_name}/
```

These track which files have been processed and should not be modified manually.

---

## Troubleshooting

### 404 Not Found Errors

**Symptom:** Some downloads report "not found (404)"

**Cause:** CMS may not publish data for all year/month combinations, or URLs may have changed.

**Resolution:** 
* Expected behavior - pipeline handles gracefully
* Check CMS.gov for available files
* Update `target_files.json` if URL patterns have changed

### Schema Mismatch Errors

**Symptom:** Auto Loader fails with schema evolution error

**Cause:** CSV column structure changed between files

**Resolution:**
* Auto Loader's `mergeSchema` option handles most cases automatically
* For major schema changes, clear checkpoint and reprocess:
  ```bash
  # Clear checkpoint for specific table
  dbutils.fs.rm("/Volumes/{catalog}/{schema}/cms_checkpoints/{table_name}", recurse=True)
  ```

### Job Timeout

**Symptom:** Job fails with timeout error

**Cause:** Large backfill or slow network

**Resolution:**
* Increase `timeout_seconds` in job YAML
* Reduce `start_year` or `end_year` range
* Increase cluster size

### Download Failures

**Symptom:** Multiple download errors

**Cause:** Network issues or rate limiting

**Resolution:**
* Jobs include automatic retry logic (2 retries)
* Check network connectivity from cluster
* Reduce `max_parallel` parameter to decrease load

### Missing Data in Tables

**Symptom:** Tables exist but have fewer rows than expected

**Cause:** CSV files may not have landed in Volume, or Auto Loader checkpoint issue

**Resolution:**
1. Check landing zone for CSV files:
   ```python
   dbutils.fs.ls("/Volumes/{catalog}/{schema}/cms_landing/{table_name}")
   ```

2. Check checkpoint status:
   ```python
   dbutils.fs.ls("/Volumes/{catalog}/{schema}/cms_checkpoints/{table_name}")
   ```

3. Review orchestrator logs for download/extract failures

---

## Development

### Local Testing

**Run individual components:**

```python
# Test download & extract
dbutils.notebook.run(
    "/Workspace/Users/your-email/cms_data_load/src/download_extract",
    timeout_seconds=600,
    arguments={
        "url": "https://...",
        "landing_path": "test_table",
        "catalog": "sandbox",
        "schema": "cms_data_raw"
    }
)

# Test Auto Loader processing
dbutils.notebook.run(
    "/Workspace/Users/your-email/cms_data_load/src/autoloader_process",
    timeout_seconds=1800,
    arguments={
        "source_path": "/Volumes/sandbox/cms_data_raw/cms_landing/test_table",
        "table_name": "test_table",
        "catalog": "sandbox",
        "schema": "cms_data_raw"
    }
)
```

### Adding New Datasets

1. Add URL pattern to `src/table_mappings.json`:
   ```json
   {
     "url_pattern": "https://cms.gov/files/zip/new-dataset-{month_name}-{year}.zip",
     "table_name": "new_dataset",
     "parameters": ["month_name", "year"],
     "landing_path": "new_dataset"
   }
   ```

2. Deploy updated bundle:
   ```bash
   databricks bundle deploy
   ```

3. Test with single file before full backfill

### Modifying Date Ranges

Edit job parameters in YAML or override at runtime:

```bash
databricks jobs run-now --job-name "CMS Data Historical Backfill" \
  --notebook-params '{"start_year": "2023", "end_year": "2024"}'
```

---

## Performance Optimization

### Parallelism Tuning

* **Backfill**: Set `max_parallel=10-20` for faster processing
* **Monthly**: Set `max_parallel=5-10` to balance speed and resource usage

### Cluster Sizing

* **Small datasets** (< 1M rows/month): 2 workers sufficient
* **Medium datasets** (1-10M rows/month): 4 workers recommended
* **Large datasets** (> 10M rows/month): 8+ workers with autoscaling

### Network Optimization

If downloading from AWS region, ensure cluster is in same region as workspace for faster downloads.

---

## Security & Compliance

### Data Access

* All data is publicly available from CMS.gov
* No authentication required for downloads
* Data stored in Unity Catalog with governance controls

### PII/PHI Considerations

These datasets contain:
* **Aggregated data only** - no individual patient records
* **Public statistics** - plan enrollment, quality metrics
* **No PHI** - HIPAA not applicable

---

## Support

### Documentation Links

* [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
* [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
* [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/jobs.html)
* [CMS Data Portal](https://www.cms.gov/data-research)

### Common Commands

```bash
# Validate bundle
databricks bundle validate

# Deploy
databricks bundle deploy

# Run backfill
databricks bundle run cms_backfill_job

# Run monthly
databricks bundle run cms_monthly_incremental

# View logs
databricks jobs runs list --job-name "CMS Data Monthly Incremental Load"

# Destroy resources
databricks bundle destroy --target dev
```

---

## License

This project is for data ingestion from public CMS datasets. Check CMS.gov for data usage terms and conditions.
