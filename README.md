# CMS Data Load Pipeline

Automated data pipeline for downloading, extracting, and processing CMS (Centers for Medicare & Medicaid Services) public data files into Delta Lake tables using Databricks Jobs and Auto Loader.

## Overview

This pipeline automates the ingestion of 26 different CMS datasets from public URLs, covering Medicare Advantage and Part D plan enrollment data, quality metrics, and supplemental information. The pipeline supports both historical backfill (5 years) and ongoing monthly incremental loads.

### Key Features

* **Automated Downloads**: Downloads and extracts CSV files from zip archives using `dbutils.fs` for Unity Catalog Volumes
* **Auto Loader Processing**: Incremental file processing with automatic tracking and Unity Catalog compatibility
* **Schema Evolution**: Automatically handles schema changes over time with `mergeSchema` enabled
* **Snake_case Standardization**: All column names automatically converted to lowercase snake_case for consistency
* **Parallel Processing**: Configurable concurrency for efficient downloads
* **Error Handling**: Graceful handling of missing files (404s) and corrupt data
* **Idempotency**: Safe to rerun - Auto Loader tracks already-processed files
* **Metadata Tracking**: Each row includes load timestamp, source file path, and processing date
* **Table Documentation**: Automatically populates table comments from configuration
* **Serverless Compute**: Runs on Databricks serverless compute (no cluster management required)

---

## Architecture

### Two-Phase Processing

**Phase 1: Download & Extract**
1. Orchestrator generates URL tasks from configuration
2. Downloads zip files from CMS.gov in parallel
3. Extracts CSVs to Unity Catalog Volumes landing zone using `dbutils.fs.put()`
4. Organizes files by dataset/year/month

**Phase 2: Auto Loader Processing**
1. Auto Loader reads CSVs from landing zone
2. **Standardizes column names** to snake_case (lowercase, underscores for spaces/special chars)
3. Infers schema with column type detection
4. Adds metadata columns using Unity Catalog-native `_metadata.file_path`
5. Appends to Delta tables with schema evolution support
6. Maintains checkpoints for incremental processing

### Components

```
cms_data_load/
├── src/
│   ├── table_mappings.json      # Maps URL patterns to tables (with comments)
│   ├── utils.py                  # Helper functions (URL expansion, dates)
│   ├── download_extract.py       # Downloads zips, extracts CSVs to Volumes
│   ├── autoloader_process.py    # Auto Loader → Delta (snake_case, UC-native)
│   └── orchestrator.py           # Main pipeline coordinator
├── resources/
│   ├── schemas.schema.yml        # Unity Catalog schema (auto-created)
│   ├── cms_landing.volume.yml    # Landing zone volume
│   ├── cms_checkpoints.volume.yml # Checkpoint volume
│   ├── backfill_job.job.yml     # Historical backfill job (serverless)
│   └── monthly_job.job.yml       # Scheduled monthly job (serverless)
├── tests/
│   └── test_pipeline.py          # End-to-end tests
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

* Databricks workspace (AWS, Azure, or GCP) with serverless compute enabled
* Unity Catalog enabled
* Catalog created (e.g., `sandbox`) - schema will be auto-created by bundle
* Email address for job notifications

### 1. Configure Variables

Edit `databricks.yml` to set your target catalog and schema:

```yaml
variables:
  catalog:
    description: Unity Catalog name
  schema:
    description: Schema name

targets:
  dev:
    variables:
      catalog: sandbox
      schema: ${workspace.current_user.short_name}  # Creates user-specific schema
  
  prod:
    variables:
      catalog: sandbox
      schema: cms_data_raw  # Fixed schema for production
```

**Note:** Development mode automatically prefixes schema names (e.g., `dev_cvollstadt_cvollstadt`). This is expected behavior.

### 2. Deploy the Bundle

```bash
# Validate configuration
databricks bundle validate --target dev

# Deploy to dev environment
databricks bundle deploy --target dev

# Or deploy to production
databricks bundle deploy --target prod
```

This creates:
* Unity Catalog schema (e.g., `sandbox.dev_cvollstadt_cvollstadt` for dev)
* 2 Unity Catalog Volumes (`cms_landing`, `cms_checkpoints`)
* 2 Databricks Jobs (backfill and monthly) configured for serverless compute
* All notebooks deployed with source-linked mode

### 3. Verify Deployment

```bash
# View deployed resources
databricks bundle summary --target dev

# Check job status
databricks jobs list --output JSON | jq '.jobs[] | select(.settings.name | contains("CMS Data"))'
```

---

## Usage

### Historical Backfill (One-Time)

Load 5 years of historical data (2020-2024):

```bash
# Run via CLI
databricks bundle run cms_backfill_job --target dev

# Or via workspace UI
# Navigate to Workflows > Jobs > "[dev user] CMS Data Historical Backfill" > Run Now
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
databricks bundle run cms_monthly_incremental --target dev
```

**Schedule:** Quartz cron expression `0 0 0 5 * ? *` 
- Format: `<seconds> <minutes> <hours> <day-of-month> <month> <day-of-week> <year>`
- Meaning: At 00:00:00 (midnight) on the 5th day of every month

**Expected Duration:** 1-2 hours

### Monitoring

**Via CLI:**
```bash
# Get latest job run
databricks jobs runs list --job-name "[dev user] CMS Data Monthly Incremental Load" --limit 1

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
      "landing_path": "landing_folder_name",
      "comment": "Description of this dataset (appears in Unity Catalog)"
    }
  ]
}
```

**Note:** Ensure the JSON file is UTF-8 encoded without BOM (Byte Order Mark).

### Job Configuration

**Both jobs use serverless compute** - no cluster configuration required.

**Backfill Job** (`resources/backfill_job.job.yml`):
* 12-hour timeout
* High parallelism (10 concurrent downloads)
* Runs on-demand only (not scheduled)

**Monthly Job** (`resources/monthly_job.job.yml`):
* 2-hour timeout
* Moderate parallelism (5 concurrent downloads)
* Scheduled via Quartz cron expression: `0 0 0 5 * ? *`
* Timezone: America/New_York

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
/Volumes/sandbox/dev_cvollstadt_cvollstadt/cms_landing/
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

**Column Naming:**
* All CSV column names converted to **snake_case**:
  - Spaces → underscores: `Contract ID` → `contract_id`
  - Special chars removed: `Cost ($)` → `cost`
  - Lowercase: `OrganizationName` → `organizationname`
  - Multiple underscores collapsed: `Some__Column` → `some_column`

**Metadata Columns:**
* `_load_timestamp`: Timestamp when record was loaded (TIMESTAMP)
* `_source_file`: Full path to source CSV using Unity Catalog `_metadata.file_path`
* `_processing_date`: Date of processing batch (STRING, YYYY-MM-DD format)

**Table Properties:**
* `COMMENT`: Automatically populated from `table_mappings.json`
* `USING DELTA`: All tables are Delta format
* Schema evolution enabled via `mergeSchema=true`

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
* Expected behavior - pipeline handles gracefully and continues
* Check CMS.gov for available files
* Update `src/table_mappings.json` if URL patterns have changed

### Schema Mismatch Errors

**Symptom:** Auto Loader fails with schema evolution error

**Cause:** CSV column structure changed significantly between files

**Resolution:**
* Auto Loader's `mergeSchema` option handles most cases automatically
* For major schema changes, clear checkpoint and reprocess:
  ```python
  dbutils.fs.rm("/Volumes/{catalog}/{schema}/cms_checkpoints/{table_name}", recurse=True)
  ```
* Verify column name standardization didn't cause unexpected mappings

### Job Timeout

**Symptom:** Job fails with timeout error

**Cause:** Large backfill, slow network, or serverless cold start

**Resolution:**
* Increase `timeout_seconds` in job YAML
* Reduce `start_year` or `end_year` range
* Reduce `max_parallel` to decrease memory pressure

### Download Failures

**Symptom:** Multiple download errors

**Cause:** Network issues or rate limiting from CMS.gov

**Resolution:**
* Jobs include automatic retry logic (1-2 retries)
* Check network connectivity from serverless compute
* Reduce `max_parallel` parameter to decrease load on CMS servers

### Missing Data in Tables

**Symptom:** Tables exist but have fewer rows than expected

**Cause:** CSV files may not have landed in Volume, or Auto Loader checkpoint issue

**Resolution:**
1. Check landing zone for CSV files:
   ```python
   display(dbutils.fs.ls("/Volumes/{catalog}/{schema}/cms_landing/{table_name}"))
   ```

2. Check checkpoint status:
   ```python
   display(dbutils.fs.ls("/Volumes/{catalog}/{schema}/cms_checkpoints/{table_name}"))
   ```

3. Review orchestrator logs for download/extract failures
4. Check for 404 errors in download phase - may indicate files don't exist for that period

### Volume Write Errors

**Symptom:** "operation not supported" when writing to Volumes

**Cause:** Using standard Python file I/O instead of `dbutils.fs`

**Resolution:**
* Already fixed in current version - uses `dbutils.fs.mkdirs()` and `dbutils.fs.put()`
* Volumes require Databricks filesystem operations, not `os.makedirs()` or `open()`

### JSONDecodeError in Notebooks

**Symptom:** `JSONDecodeError: Unexpected UTF-8 BOM` when reading config files

**Cause:** JSON files contain UTF-8 BOM (Byte Order Mark)

**Resolution:**
* Already fixed - `table_mappings.json` cleaned to remove BOM
* All config files should be UTF-8 without BOM

---

## Development

### Local Testing

Run the test suite in `tests/test_pipeline.py`:

```python
# Open test notebook
/Users/your-email/cms_data_load/tests/test_pipeline

# Run all cells in order
# Tests validate:
#   1. Download & Extract
#   2. Files in Volume
#   3. Auto Loader Processing
#   4. Table Verification (with snake_case columns)
#   5. Utility Functions
#   6. Table Mappings
```

**Test individual components:**

```python
# Test download & extract
dbutils.notebook.run(
    "../src/download_extract",
    timeout_seconds=600,
    arguments={
        "url": "https://www.cms.gov/files/zip/ma-plan-directory.zip",
        "landing_path": "test_table",
        "catalog": "sandbox",
        "schema": "dev_cvollstadt_cvollstadt",
        "year": "",
        "month": ""
    }
)

# Test Auto Loader processing
dbutils.notebook.run(
    "../src/autoloader_process",
    timeout_seconds=1800,
    arguments={
        "source_path": "/Volumes/sandbox/dev_cvollstadt_cvollstadt/cms_landing/test_table",
        "table_name": "test_table",
        "catalog": "sandbox",
        "schema": "dev_cvollstadt_cvollstadt",
        "checkpoint_path": "",
        "comment": "Test table"
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
     "landing_path": "new_dataset",
     "comment": "Description of the new dataset"
   }
   ```

2. Ensure JSON file is UTF-8 without BOM:
   ```python
   # If you see encoding errors, remove BOM:
   with open("table_mappings.json", "r", encoding="utf-8-sig") as f:
       content = f.read()
   with open("table_mappings.json", "w", encoding="utf-8") as f:
       f.write(content)
   ```

3. Deploy updated bundle:
   ```bash
   databricks bundle validate --target dev
   databricks bundle deploy --target dev
   ```

4. Test with single file before full backfill

### Modifying Date Ranges

Override parameters at runtime:

```bash
databricks jobs run-now --job-id <job_id> \
  --notebook-params '{"start_year": "2023", "end_year": "2024"}'
```

---

## Performance Optimization

### Parallelism Tuning

* **Backfill**: Set `max_parallel=10-20` for faster processing (serverless scales automatically)
* **Monthly**: Set `max_parallel=5-10` to balance speed and resource usage

### Serverless Compute Benefits

* **No cluster management** - automatically provisions and scales resources
* **Fast cold starts** - ready in seconds
* **Cost optimization** - pay only for compute used
* **Latest runtime** - automatically uses latest Databricks Runtime with Photon

### Network Optimization

* Serverless compute runs in same region as workspace for optimal performance
* Large files download faster due to parallel processing in download phase

---

## Technical Details

### Unity Catalog Compatibility

* **File metadata**: Uses `_metadata.file_path` instead of deprecated `input_file_name()`
* **Volume operations**: All file I/O uses `dbutils.fs` API
* **Schema management**: Schema auto-created by DAB with proper dependencies
* **Table comments**: Automatically populated from configuration

### Column Standardization

The `to_snake_case()` function in `autoloader_process.py`:
1. Replaces special characters and spaces with underscores
2. Converts to lowercase
3. Removes leading/trailing underscores
4. Collapses multiple consecutive underscores

**Examples:**
* `Patient ID` → `patient_id`
* `Total-Cost` → `total_cost`
* `Date of Service` → `date_of_service`
* `Cost ($)` → `cost`

### Notebook Communication

* `dbutils.notebook.run()` returns strings
* `dbutils.notebook.exit()` requires string arguments
* All inter-notebook communication uses JSON serialization: `json.dumps()` and `json.loads()`

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

## Bundle Resources

### Deployment Structure

The DAB (Databricks Asset Bundle) creates resources following best practices:

**Individual resource files:**
* `schemas.schema.yml` - Unity Catalog schema
* `cms_landing.volume.yml` - Landing zone volume
* `cms_checkpoints.volume.yml` - Checkpoint volume
* `backfill_job.job.yml` - Backfill job
* `monthly_job.job.yml` - Monthly job

**Benefits:**
* Clean validation (no warnings)
* Easy to modify individual resources
* Clear dependency management
* Follows Databricks best practices

---

## Support

### Documentation Links

* [Auto Loader Documentation](https://docs.databricks.com/ingestion/auto-loader/index.html)
* [Delta Lake Documentation](https://docs.databricks.com/delta/index.html)
* [Databricks Jobs Documentation](https://docs.databricks.com/workflows/jobs/jobs.html)
* [Unity Catalog Documentation](https://docs.databricks.com/data-governance/unity-catalog/index.html)
* [Databricks Asset Bundles](https://docs.databricks.com/dev-tools/bundles/index.html)
* [Quartz Cron Syntax](https://www.quartz-scheduler.org/documentation/quartz-2.3.0/tutorials/crontrigger.html)
* [CMS Data Portal](https://www.cms.gov/data-research)

### Common Commands

```bash
# Validate bundle
databricks bundle validate --target dev

# Deploy
databricks bundle deploy --target dev

# View deployed resources
databricks bundle summary --target dev

# Run backfill
databricks bundle run cms_backfill_job --target dev

# Run monthly
databricks bundle run cms_monthly_incremental --target dev

# View logs
databricks jobs runs list --output JSON | jq '.runs[0]'

# Destroy resources (careful!)
databricks bundle destroy --target dev --auto-approve
```

---

## Recent Updates

### Version 1.1 (Latest)

**Data Processing:**
* Added automatic snake_case column standardization
* Switched to Unity Catalog-native `_metadata.file_path` for source tracking
* Removed UTF-8 BOM from configuration files for better compatibility
* Added table comment support from `table_mappings.json`

**Infrastructure:**
* Migrated to serverless compute (no cluster management)
* Schema now auto-created by DAB deployment
* Split volumes into individual resource files (best practice)
* Updated cron expressions to Quartz format (7 fields)
* Fixed Volume write operations to use `dbutils.fs` API

**Reliability:**
* Improved inter-notebook communication with proper JSON serialization
* Enhanced error handling for 404s and missing data
* Added comprehensive test suite with 6 validation tests

---

## License

This project is for data ingestion from public CMS datasets. Check CMS.gov for data usage terms and conditions.
