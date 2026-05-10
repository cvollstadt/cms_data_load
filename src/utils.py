# Databricks notebook source
# MAGIC %md
# MAGIC # Utility Functions for CMS Data Load Pipeline
# MAGIC
# MAGIC Helper functions for URL generation, date handling, and file path management.

# COMMAND ----------

from datetime import datetime, timedelta
from typing import List, Dict, Tuple
import calendar

# COMMAND ----------

# Month name mapping
MONTHS = [
    "january", "february", "march", "april", "may", "june",
    "july", "august", "september", "october", "november", "december"
]

# COMMAND ----------

def get_quarter_from_month(month: int) -> str:
    """
    Convert month number (1-12) to quarter string (q1-q4).
    
    Args:
        month: Month number (1-12)
    
    Returns:
        Quarter string (q1, q2, q3, or q4)
    """
    if month in [1, 2, 3]:
        return "q1"
    elif month in [4, 5, 6]:
        return "q2"
    elif month in [7, 8, 9]:
        return "q3"
    else:
        return "q4"

# COMMAND ----------

def generate_date_ranges(start_year: int, end_year: int, mode: str = "backfill") -> List[Dict[str, any]]:
    """
    Generate list of year/month/quarter combinations for processing.
    
    Args:
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
        mode: Either "backfill" (all months) or "incremental" (current month only)
    
    Returns:
        List of dicts with keys: year, month (1-12), month_name, quarter
    """
    date_ranges = []
    
    if mode == "incremental":
        # Only current month
        now = datetime.now()
        # Use previous month since CMS data is typically available by 5th of following month
        prev_month = now.replace(day=1) - timedelta(days=1)
        
        date_ranges.append({
            "year": prev_month.year,
            "month": prev_month.month,
            "month_name": MONTHS[prev_month.month - 1],
            "quarter": get_quarter_from_month(prev_month.month)
        })
    else:
        # Backfill mode: all months in range
        for year in range(start_year, end_year + 1):
            for month in range(1, 13):
                date_ranges.append({
                    "year": year,
                    "month": month,
                    "month_name": MONTHS[month - 1],
                    "quarter": get_quarter_from_month(month)
                })
    
    return date_ranges

# COMMAND ----------

def expand_url(url_pattern: str, params: Dict[str, str]) -> str:
    """
    Expand URL pattern with provided parameters.
    
    Args:
        url_pattern: URL with {param} placeholders
        params: Dictionary of parameter values
    
    Returns:
        Expanded URL string
    """
    url = url_pattern
    for key, value in params.items():
        url = url.replace(f"{{{key}}}", str(value))
    return url

# COMMAND ----------

def generate_landing_path(catalog: str, schema: str, landing_path: str, year: int = None, month: int = None) -> str:
    """
    Generate Unity Catalog Volume path for landing CSVs.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        landing_path: Base path for this dataset
        year: Year (optional, for partitioning)
        month: Month (optional, for partitioning)
    
    Returns:
        Full Volume path
    """
    base_path = f"/Volumes/{catalog}/{schema}/cms_landing/{landing_path}"
    
    if year and month:
        return f"{base_path}/year={year}/month={month:02d}"
    elif year:
        return f"{base_path}/year={year}"
    else:
        return base_path

# COMMAND ----------

def get_table_full_name(catalog: str, schema: str, table_name: str) -> str:
    """
    Generate fully qualified table name.
    
    Args:
        catalog: Catalog name
        schema: Schema name
        table_name: Table name
    
    Returns:
        Fully qualified table name (catalog.schema.table)
    """
    return f"{catalog}.{schema}.{table_name}"

# COMMAND ----------

def build_url_tasks(mappings: List[Dict], date_ranges: List[Dict]) -> List[Dict]:
    """
    Build list of download tasks by expanding URL patterns with date ranges.
    
    Args:
        mappings: List of table mapping configurations
        date_ranges: List of date range dicts (year, month, month_name, quarter)
    
    Returns:
        List of task dicts with: url, table_name, landing_path, year, month, quarter
    """
    tasks = []
    
    for mapping in mappings:
        url_pattern = mapping["url_pattern"]
        parameters = mapping["parameters"]
        
        if not parameters:
            # Static URL (no parameters) - process only once
            tasks.append({
                "url": url_pattern,
                "table_name": mapping["table_name"],
                "landing_path": mapping["landing_path"],
                "year": None,
                "month": None,
                "quarter": None
            })
        else:
            # Parameterized URL - expand for each date range
            for date_range in date_ranges:
                # Check if this URL needs the specific parameters
                params = {}
                
                if "year" in parameters:
                    params["year"] = date_range["year"]
                
                if "month_name" in parameters:
                    params["month_name"] = date_range["month_name"]
                
                if "quarter" in parameters:
                    params["quarter"] = date_range["quarter"]
                
                # Only create task if all required parameters are available
                if len(params) == len(parameters):
                    expanded_url = expand_url(url_pattern, params)
                    
                    tasks.append({
                        "url": expanded_url,
                        "table_name": mapping["table_name"],
                        "landing_path": mapping["landing_path"],
                        "year": date_range.get("year"),
                        "month": date_range.get("month"),
                        "quarter": date_range.get("quarter")
                    })
    
    return tasks
