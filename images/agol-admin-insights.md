# Admin Insights: Organization Inventory Notebook

## Purpose
This notebook automates the population of **all tables in the Admin Insights - Data Table** — Users & Licensing, Groups, and Content (including upstream and downstream dependency mapping when enabled) — in a single execution. It snapshots your organization's users, licenses, groups, and content, then publishes each dataset to its respective admin insights hosted table, ensuring all data is ready for downstream dashboards and analyses.

---

## Intended Audience
ArcGIS administrators responsible for deploying and maintaining the admin insights template.

---

## Before You Run: Configuration Checklist

Ensure all required variables in **Step 1a** are correctly set before executing the notebook.

### ⚠️ Hosted Feature Service ID
Update `fs_id` with the item ID of the hosted feature service created in the initialization notebook.
```python
fs_id = "1ed3df79ff9447db8ad64ea37920711e"
```

### ⚠️ Dependency Mapping (Optional — Content Only)
Dependency mapping identifies upstream and downstream relationships between items in your ArcGIS organization. When enabled, this provides enhanced visibility into how content is interconnected — supporting impact analysis, risk mitigation, and a deeper understanding of content usage.

> Because this analysis can be time-intensive, especially in larger environments, it is recommended to leave `dependency_mapping` **disabled** if faster performance is preferred.
```python
dependency_mapping = True
```

### ⚠️ Test Mode
Set `TEST_MODE = True` and reduce the limit variables to process a small subset of records before running against your full organization.
```python
TEST_MODE  = False
USER_LIMIT = 99999
GROUP_LIMIT = 99999
ITEM_CAP = None
```

---

## Notebook Structure

| Step | Description |
|---|---|
| **Step 1** | Configuration, imports, and GIS connection |
| **Step 2** | Define all functions — global utilities, publishing, and module-specific logic |
| **Step 3** | Execute snapshots and publish to hosted tables |

---

## Step 3 Execution Overview

The notebook executes three sequential snapshot operations. Each section inventories your organization, builds a DataFrame, and publishes records automatically — reporting progress and success per batch.

### 3a — Users & Licensing
Captures the organization's user inventory and licensing entitlement data and publishes it to the Users and Licenses hosted tables.

### 3b — Groups
Captures the organization's group inventory — including membership, activity, and content metrics — and publishes it to the Groups hosted table.

### 3c — Content
Captures the organization's full content inventory — with optional upstream and downstream dependency mapping — and publishes it to the Content (and optionally Upstream/Downstream Dependencies) hosted tables.

---

## Expected Outputs

| Module | Output |
|---|---|
| **Users & Licensing** | Updated hosted tables with user inventory and licensing data; Users & Licensing dashboard auto-configured |
| **Groups** | Updated hosted table with group inventory; Groups dashboard auto-configured |
| **Content** | Updated hosted table with content inventory (+ dependencies if enabled); Content dashboard auto-configured |

All three modules produce:
- Progress logs showing records **attempted**, **succeeded**, and **failed** per batch
- A final status message confirming publish success or errors

---

## Notes / Best Practices
- This notebook is intended to be **run periodically** to maintain up-to-date results. **Scheduling automated runs is strongly recommended.**
- Refer to the published log output for validation and troubleshooting of any failed batches.
- The **Content snapshot** is the most time-intensive module, particularly when `dependency_mapping = True`. Disable it when execution speed is a priority.
- Item size collection (`add_item_sizes`) is called within `run_governance_pipeline`. Comment it out if size data is not needed, as it adds significant runtime for large organizations.
- All three modules share a single GIS connection — ensure the connecting account has sufficient administrative privileges across all three data domains.

# Step 1: Imports and Configuration

## Step 1a: Define Variables


```python
# ----------------------------
# Configuration Settings
# ----------------------------

# Hosted Feature Service
# SECURITY: Use environment variable for production deployments
import os
fs_id = os.getenv("AGOL_FEATURE_SERVICE_ID", "1ed3df79ff9447db8ad64ea37920711e")

# Data Management
delete_content = True
RECENT_DAYS_THRESHOLD = 90

# Dependency Mapping (Content only)
dependency_mapping = True
outside_org = False 

# Test / Sampling Limits
TEST_MODE = False
USER_LIMIT = 99999
GROUP_LIMIT = 99999
ITEM_CAP = None

# ----------------------------
# Performance Configuration
# ----------------------------

# API and Processing Limits
MAX_ITEMS_PER_GROUP = int(os.getenv("MAX_ITEMS_PER_GROUP", "1000"))
CHUNK_SIZE = int(os.getenv("CHUNK_SIZE", "500"))
USER_BATCH_SIZE = int(os.getenv("USER_BATCH_SIZE", "50"))
MAX_CONTENT_ITEMS = int(os.getenv("MAX_CONTENT_ITEMS", "100"))

# Time thresholds (days)
RECENT_DAYS_THRESHOLD = int(os.getenv("RECENT_DAYS_THRESHOLD", "90"))
INACTIVE_180_DAYS = 180
INACTIVE_365_DAYS = 365

# Progress reporting
PROGRESS_PRINT_INTERVAL = int(os.getenv("PROGRESS_PRINT_INTERVAL", "25"))
GROUP_PROGRESS_INTERVAL = int(os.getenv("GROUP_PROGRESS_INTERVAL", "50"))

# Connection and retry settings
CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "300"))  # 5 minutes
REQUEST_TIMEOUT = int(os.getenv("REQUEST_TIMEOUT", "60"))      # 1 minute
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY = int(os.getenv("RETRY_DELAY", "2"))               # Base delay for exponential backoff

# String truncation limits
MAX_STRING_LENGTH = int(os.getenv("MAX_STRING_LENGTH", "256"))
MAX_TITLE_LENGTH = int(os.getenv("MAX_TITLE_LENGTH", "256"))
MAX_DESCRIPTION_LENGTH = int(os.getenv("MAX_DESCRIPTION_LENGTH", "256"))
```


```python
# ----------------------------
# Naming Conventions
# ----------------------------

ELEVATED_PRIVILEGE_PATTERNS = [
    "portal:admin:",
    "marketplace:admin:",
    "premium:publisher:",
    "features:user:fullEdit",
    "portal:publisher:publishServerServices",
]
ELEV_PREFIXES = tuple(ELEVATED_PRIVILEGE_PATTERNS)

ROLE_MAP = {
    "org_admin": "Administrator",
    "org_publisher": "Publisher",
    "org_user": "User",
}

USERTYPE_MAP = {
    "creatorUT": "Creator",
    "GISProfessionalBasicUT": "Professional Basic",
    "GISProfessionalAdvUT": "Professional Plus",
    "GISProfessionalStdUT": "Professional Standard",
    "standardUT": "Standard",
    "advancedUT": "Advanced",
    "basicUT": "Basic",
    "editorUT": "Editor",
    "fieldWorkerUT": "Mobile Worker",
    "viewerUT": "Viewer",
}
```

## Step 1b: Import Libraries and Connect to Your Organization


```python
from arcgis.gis import GIS
from arcgis.features import Feature, FeatureLayerCollection
from arcgis.apps.itemgraph import ItemGraph

from concurrent.futures import ThreadPoolExecutor, TimeoutError as FutureTimeoutError

from datetime import datetime, date, timedelta, timezone
from collections import defaultdict

import pandas as pd
import numpy as np

import os
import re
import html
import json
import ast
import math
import time
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Connection management settings
CONNECTION_TIMEOUT = 300  # 5 minutes
REQUEST_TIMEOUT = 60      # 1 minute per request
MAX_RETRIES = 3
RETRY_DELAY = 2           # Base delay for exponential backoff

# Connect as currently signed-in administrator with timeout handling
try:
    gis = GIS("home", validate_url=True)
    logger.info(f"Successfully connected to {gis.properties.name}")
except Exception as e:
    logger.error(f"Failed to connect to ArcGIS Online: {e}")
    raise

# Validate feature service connection with timeout
try:
    agk_fs = gis.content.get(fs_id)
    if agk_fs is None:
        raise ValueError(f"Feature service with ID {fs_id} not found")
    
    flc = FeatureLayerCollection.fromitem(agk_fs)
    logger.info("Feature service connection validated")
except Exception as e:
    logger.error(f"Failed to connect to feature service: {e}")
    raise

print(f"✓ Connected to {gis.properties.name}")
print(f"  Logged in as:  {gis.properties.user.username}")
print(f"  Role:          {gis.properties.user.role}")
print(f"  Feature service: {agk_fs.homepage}")
print(f"  Last ran: {datetime.now()}")
```

    2026-07-01 12:02:50,739 - INFO - Feature service connection validated


    ✓ Connected to Tennessee Division of Forestry
      Logged in as:  colin.stiles_tndof
      Role:          org_admin
      Feature service: https://www.arcgis.com/home/item.html?id=1ed3df79ff9447db8ad64ea37920711e
      Last ran: 2026-07-01 12:02:50.740930


# Step 2: Define Functions

## Step 2a: Global Utility Functions


```python
# ----------------------------
# Time thresholds (epoch ms)
# ----------------------------
now_ms          = int(datetime.now(timezone.utc).timestamp() * 1000)
cutoff_180_ms   = now_ms - 180 * 86_400_000
cutoff_365_ms   = now_ms - 365 * 86_400_000

DISPLAY_DT_FORMAT = "%m/%d/%Y %H:%M"


def safe_get(obj, key, default=None):
    """
    Safely fetch `key` from an object (via attribute) or dict (via key).

    Parameters
    ----------
    obj : object or dict
    key : str
    default : any, optional

    Returns
    -------
    any
        Attribute/key value, or `default` if not found.
    """
    try:
        if hasattr(obj, key):
            return getattr(obj, key)
        if isinstance(obj, dict):
            return obj.get(key, default)
    except Exception:
        pass
    return default


def ms_to_dt(ms):
    """
    Convert epoch milliseconds to a UTC datetime.

    Parameters
    ----------
    ms : int or str

    Returns
    -------
    datetime or None
        None if the value is missing, zero, or invalid.
    """
    if ms in (None, -1, 0, "0", "-1", ""):
        return None
    try:
        return datetime.fromtimestamp(int(ms) / 1000.0, tz=timezone.utc)
    except (ValueError, TypeError, OSError) as e:
        logger.error(f"ms_to_dt failed: {ms} ({type(ms)}) - {e}")
        return None

def days_since_ms(ms):
    """
    Return the number of whole days elapsed since an epoch-millisecond timestamp.

    Parameters
    ----------
    ms : int or str

    Returns
    -------
    int or None
    """
    dt_val = ms_to_dt(ms)
    if not dt_val:
        return None
    return (datetime.now(timezone.utc) - dt_val).days


def dt_str_from_ms(ms):
    """
    Convert epoch milliseconds to a formatted datetime string.

    Parameters
    ----------
    ms : int or str

    Returns
    -------
    str
        Formatted as MM/DD/YYYY HH:MM, or an empty string if invalid.
    """
    if ms in (None, "", 0):
        return ""
    try:
        return datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc).strftime(DISPLAY_DT_FORMAT)
    except (ValueError, TypeError, OSError):
        return ""


def truncate_string(value, max_length=256):
    """
    Convert a value to a string and enforce a maximum length.

    Parameters
    ----------
    value : any
    max_length : int, optional
        Default is 256.

    Returns
    -------
    str
        Truncated string with "..." suffix if over limit. Empty string if None.
    """
    if value is None:
        return ""
    s = str(value)
    return s if len(s) <= max_length else s[:max_length - 3] + "..."


def strip_html_to_text(val):
    """
    Enhanced HTML sanitization with security improvements.
    
    Removes script/style blocks, strips all HTML tags, decodes entities,
    and applies additional security measures.

    Parameters
    ----------
    val : any

    Returns
    -------
    str or None
    """
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return None
    
    s = str(val)
    
    # Remove potentially dangerous protocols
    dangerous_patterns = [
        r'javascript:',
        r'vbscript:',
        r'data:',
        r'file:',
        r'ftp:',
    ]
    
    for pattern in dangerous_patterns:
        s = re.sub(pattern, '', s, flags=re.IGNORECASE)
    
    # Remove script and style blocks completely
    s = re.sub(r'(?is)<(script|style).*?>.*?</\1>', ' ', s)
    
    # Remove HTML comments
    s = re.sub(r'<!--.*?-->', ' ', s, flags=re.DOTALL)
    
    # Remove all remaining HTML tags
    s = re.sub(r'<[^>]*>', ' ', s)
    
    # Decode HTML entities
    html_entities = {
        '&nbsp;': ' ',
        '&amp;': '&',
        '&lt;': '<',
        '&gt;': '>',
        '&quot;': '"',
        '&#39;': "'",
        '&apos;': "'",
    }
    
    for entity, replacement in html_entities.items():
        s = s.replace(entity, replacement)
    
    # Handle numeric entities
    s = re.sub(r'&#(\d+);', lambda m: chr(int(m.group(1))), s)
    s = re.sub(r'&#x([0-9a-fA-F]+);', lambda m: chr(int(m.group(1), 16)), s)
    
    # Normalize whitespace and strip
    s = re.sub(r'\s+', ' ', s).strip()
    
    # Additional security: remove control characters except newlines and tabs
    s = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', s)
    
    return s or None


def sanitize(df: pd.DataFrame) -> pd.DataFrame:
    """
    Prepare a DataFrame for ArcGIS edit_features() operations.

    Converts datetime columns to epoch milliseconds (UTC) and replaces
    NaT, NaN, and pd.NA with None.

    Parameters
    ----------
    df : pandas.DataFrame

    Returns
    -------
    pandas.DataFrame
    """
    df2 = df.copy()
    dt_cols = df2.select_dtypes(include=["datetime64[ns]", "datetime64[ns, UTC]"]).columns
    for c in dt_cols:
        s = pd.to_datetime(df2[c], errors="coerce")
        ms = (s.astype("int64", copy=False) // 1_000_000)
        df2[c] = ms.where(s.notna(), None).astype("object")
    df2 = df2.replace({pd.NaT: None, np.nan: None})
    df2 = df2.where(pd.notna(df2), None)
    return df2


# ----------------------------
# Progress Reporting Utility
# ----------------------------
def print_progress(current, total, start_time, item_type="items", print_interval=25):
    """
    Standardized progress reporting utility.
    
    Parameters
    ----------
    current : int
        Current number of items processed
    total : int
        Total number of items to process
    start_time : float
        Start time from time.perf_counter()
    item_type : str
        Type of items being processed (for display)
    print_interval : int
        How often to print progress updates
    """
    if current % print_interval == 0 or current == total:
        now = time.perf_counter()
        elapsed = now - start_time
        avg = elapsed / max(1, current)
        remaining = total - current
        eta = remaining * avg
        
        pct = current / max(1, total) * 100
        print(
            f"  Progress: {current:,}/{total:,} "
            f"({pct:,.1f}%) | "
            f"elapsed {elapsed:,.1f}s | avg {avg:,.3f}s/{item_type[:-1]} | ETA {eta/60:,.1f} min"
        )


# ----------------------------
# Retry Utility
# ----------------------------
def retry_with_backoff(func, max_retries=MAX_RETRIES, base_delay=RETRY_DELAY, 
                    exceptions=(ConnectionError, TimeoutError)):
    """
    Execute a function with exponential backoff retry logic.
    
    Parameters
    ----------
    func : callable
        Function to execute
    max_retries : int
        Maximum number of retry attempts
    base_delay : float
        Base delay in seconds for exponential backoff
    exceptions : tuple
        Exception types that trigger retries
        
    Returns
    -------
    Any
        Result from function execution
        
    Raises
    ------
    Exception
        The last exception if all retries fail
    """
    for attempt in range(max_retries):
        try:
            return func()
        except exceptions as e:
            if attempt == max_retries - 1:
                raise
            delay = base_delay * (2 ** attempt)
            logger.warning(f"Attempt {attempt + 1} failed: {e}. Retrying in {delay}s...")
            time.sleep(delay)
        except Exception as e:
            # Don't retry on non-network exceptions
            raise


print("✓ Global utility functions loaded")
```

## Step 2b: Global Publishing Functions


```python
def get_table_url(feature_service_item, table_name, gis=None):
    """
    Return the hosted table layer object, its sublayer ID, and its AGOL Data page URL.

    Parameters
    ----------
    feature_service_item : arcgis.gis.Item
    table_name : str
        Case-insensitive name of the table within the feature service.
    gis : arcgis.gis.GIS, optional
        Required only if feature_service_item.homepage is unavailable.

    Returns
    -------
    tuple
        (table_layer, sublayer_id, data_url)
    """
    if feature_service_item is None:
        raise ValueError("feature_service_item is None")

    try:
        flc = FeatureLayerCollection.fromitem(feature_service_item)
    except Exception as e:
        raise RuntimeError(f"Failed to create FeatureLayerCollection from item: {e}")

    table_layer = next(
        (t for t in flc.tables
         if (safe_get(t.properties, "name", "") or "").lower() == (table_name or "").lower()),
        None
    )

    if table_layer is None:
        available = [getattr(t.properties, "name", None) for t in flc.tables]
        raise ValueError(f'Table "{table_name}" not found. Available tables: {available}')

    sublayer_id = getattr(table_layer.properties, "id", None)
    if sublayer_id is None:
        raise ValueError(f'Could not determine sublayer id for table "{table_name}".')

    base_item_url = getattr(feature_service_item, "homepage", None)
    if not base_item_url:
        if gis is None:
            raise ValueError("feature_service_item.homepage is missing; pass gis to construct URL.")
        base_item_url = f"{gis.url.rstrip('/')}/home/item.html?id={feature_service_item.id}"

    data_url = f"{base_item_url}&sublayer={sublayer_id}#data"
    return table_layer, sublayer_id, data_url


def clear_table(table_layer, max_retries=3):
    """
    Remove all records from a hosted table with retry logic.

    Attempts truncate first; falls back to delete_features(where="1=1").

    Parameters
    ----------
    table_layer : arcgis.features.Table
    max_retries : int, optional
        Number of retry attempts for failed operations
    """
    print(f"\nClearing existing records from table: {table_layer.properties.name}")

    # Try truncate first
    for attempt in range(max_retries):
        try:
            result = table_layer.manager.truncate()
            print("Truncate successful.")
            print(result)
            return
        except (PermissionError, RuntimeError) as e:
            print(f"Truncate attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                print("All truncate attempts failed. Falling back to delete_features.")
            else:
                time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error during truncate: {e}")
            break

    # Fallback to delete_features
    for attempt in range(max_retries):
        try:
            result = table_layer.delete_features(where="1=1")
            print("Delete (where=1=1) successful.")
            print(result)
            return
        except (PermissionError, RuntimeError) as e:
            print(f"Delete attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                print("All delete attempts failed.")
                raise RuntimeError(f"Failed to clear table after {max_retries} attempts: {e}")
            else:
                time.sleep(2 ** attempt)  # Exponential backoff
        except Exception as e:
            print(f"Unexpected error during delete: {e}")
            raise RuntimeError(f"Unexpected error clearing table: {e}")


def publish_dataframe_to_table(df, table_layer, chunk_size=500, max_retries=3):
    """
    Publish a pandas DataFrame to an ArcGIS hosted table in chunked batches with retry logic.

    Parameters
    ----------
    df : pandas.DataFrame
        Columns must exactly match the target table's field names.
    table_layer : arcgis.features.Table
    chunk_size : int
        Number of rows per request. Default is 500.
    max_retries : int
        Number of retry attempts for failed operations
    """
    print(f"\nPublishing to table: {table_layer.properties.name}")

    if df.empty:
        print("DataFrame is empty. Nothing to publish.")
        return

    # Strip HTML from all string columns before publishing
    df = df.copy()
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].apply(strip_html_to_text)

    df = sanitize(df)

    total_rows      = len(df)
    total_attempted = 0
    total_succeeded = 0
    total_failed    = 0
    total_chunks    = (total_rows + chunk_size - 1) // chunk_size

    print(f"Total records to publish : {total_rows:,}")
    print(f"Chunk size               : {chunk_size}")

    t0           = time.perf_counter()
    last_print_t = t0

    for chunk_num, start in enumerate(range(0, total_rows, chunk_size), start=1):
        end   = min(start + chunk_size, total_rows)
        chunk = df.iloc[start:end].copy()

        print(f"\nPublishing rows {start + 1}–{end}...")

        # Retry logic for each chunk
        chunk_success = False
        for attempt in range(max_retries):
            try:
                features = [Feature(attributes=row.to_dict()) for _, row in chunk.iterrows()]
                result   = table_layer.edit_features(adds=features)
                add_results = result.get("addResults", []) or []

                attempted  = len(features)
                succeeded  = sum(1 for r in add_results if r.get("success"))
                failed     = attempted - succeeded

                total_attempted += attempted
                total_succeeded += succeeded
                total_failed    += failed

                print(f"  Attempted : {attempted}")
                print(f"  Succeeded : {succeeded}")
                print(f"  Failed    : {failed}")

                if failed > 0:
                    errors = [r for r in add_results if not r.get("success")]
                    print("  Sample errors:")
                    for e in errors[:5]:
                        print("   ", e)

                chunk_success = True
                break

            except (ConnectionError, TimeoutError) as e:
                print(f"  Chunk attempt {attempt + 1} failed (network): {e}")
                if attempt < max_retries - 1:
                    time.sleep(2 ** attempt)  # Exponential backoff
                else:
                    print(f"  Chunk failed after {max_retries} attempts")
                    total_failed += len(chunk)
                    total_attempted += len(chunk)
            except Exception as e:
                print(f"  Unexpected error in chunk: {e}")
                raise RuntimeError(f"Failed to publish chunk {chunk_num}: {e}")

        if not chunk_success:
            print(f"  Failed to publish chunk {chunk_num} after all retries")

        # Progress reporting
        now     = time.perf_counter()
        elapsed = now - t0
        avg     = elapsed / chunk_num
        eta     = (total_chunks - chunk_num) * avg

        if now - last_print_t >= 0.2 or chunk_num == total_chunks:
            print(
                f"  Progress: {chunk_num:,}/{total_chunks:,} "
                f"({chunk_num / total_chunks * 100:.1f}%) | "
                f"elapsed {elapsed:.1f}s | avg {avg:.3f}s/chunk | ETA {eta / 60:.1f} min"
            )
            last_print_t = now

    print(f"\n{table_layer.properties.name} — Publish complete.")
    print(f"  Total attempted : {total_attempted:,}")
    print(f"  Total succeeded : {total_succeeded:,}")
    print(f"  Total failed    : {total_failed:,}")
    print(f"  Status: {'SUCCESS' if total_failed == 0 else 'COMPLETED WITH ERRORS'}")


print("✓ Global publishing functions loaded")
```

## Step 2c: Define Insights Specific Functions

### Step 2c.1: Define Users & Licensing Functions


```python
# -------------------------------------------------------
# Users & Licensing — Table Assembly Functions
# -------------------------------------------------------
# User Table
def build_user_snapshot(
    gis,
    users,
    role_map,
    usertype_map,
    elev_prefixes,
    cutoff_365_ms,
    cutoff_180_ms,
):
    """
    Build a governance and licensing snapshot for ArcGIS Online users.

    Iterates through a collection of user objects and compiles structured
    metadata including identity details, role mappings, user type mappings,
    login history, item ownership counts, privilege elevation flags,
    authentication provider details, group memberships, and governance
    recommendations. 

    Parameters:
        gis (arcgis.gis.GIS)
            Authenticated GIS connection used to query user content.
        users (list)
            Collection of ArcGIS Online user objects to analyze.
        role_map (dict)
            Mapping of raw role IDs to readable role names.
        usertype_map (dict)
            Mapping of license type IDs to readable user type names.
        elev_prefixes (tuple | str)
            Privilege name prefixes used to detect elevated privileges.
        cutoff_365_ms (int)
            Millisecond timestamp representing 1-year inactivity threshold.
        cutoff_180_ms (int)
            Millisecond timestamp representing 6-month inactivity threshold.

    Returns: pandas.DataFrame
        DataFrame containing structured user governance metrics,
        licensing insights, privilege indicators, and optimization
        recommendations.
    """

    start = time.perf_counter()
    rows = []

    total_users = len(users)
    print(f"Analyzing users: {total_users:,}")

    provider_map = {
        "arcgis": "ArcGIS",
        "enterprise": "Enterprise / SSO",
        "google": "Google",
        "facebook": "Facebook",
        "apple": "Apple",
        "github": "GitHub",
    }

    # Batch user item count lookup for better performance
    print("Fetching user item counts in batches...")
    user_item_counts = get_user_item_counts_batch(gis, [safe_get(u, "username") for u in users if safe_get(u, "username")])

    # -------- PERIODIC UPDATES (time metric) --------
    t0 = time.perf_counter()
    processed = 0
    total_targets = total_users
    last_print_t = t0
    verbose = True
    print_every_n = 25

    for i, u in enumerate(users, start=1):

        # ---------------- BASIC IDENTITY ----------------
        username = safe_get(u, "username")
        full_name = safe_get(u, "fullName")
        name = full_name if full_name else "N/A"
        email = safe_get(u, "email")

        raw_role = safe_get(u, "role")
        role = role_map.get(raw_role, raw_role)

        user_type_id = safe_get(u, "userLicenseTypeId")
        user_type = usertype_map.get(user_type_id, user_type_id)

        # ---------------- DATES ----------------
        last_login_ms = safe_get(u, "lastLogin")
        created_ms = safe_get(u, "created")

        last_login_dt = ms_to_dt(last_login_ms)
        created_dt = ms_to_dt(created_ms)

        # ---------------- ITEM COUNT (Optimized) ----------------
        item_count = user_item_counts.get(username, 0) if username else 0

        # ---------------- PRIVILEGES ----------------
        is_admin = (raw_role == "org_admin")

        try:
            privs = safe_get(u, "privileges", []) or []
        except Exception:
            privs = []

        elevated_privileges = (
            is_admin
            or any((p or "").startswith(elev_prefixes) for p in privs)
        )

        elevated_privileges_yn = "Yes" if elevated_privileges else "No"

        # ---------------- FLAGS ----------------
        disabled_yn = "Yes" if bool(safe_get(u, "disabled", False)) else "No"
        mfa_yn = "Yes" if bool(safe_get(u, "mfaEnabled", False)) else "No"

        # ---------------- PROVIDER ----------------
        provider = safe_get(u, "provider")
        idp_username = safe_get(u, "idpUsername")

        if not provider:
            provider = "enterprise" if idp_username else "arcgis"

        idp_provider = provider_map.get(str(provider).lower(), provider)

        # ---------------- GROUP COUNT ----------------
        try:
            group_count = len(safe_get(u, "groups", []) or [])
        except Exception:
            group_count = 0


        # ---------------- RECOMMENDATIONS ----------------
        suggestion = ""

        if user_type in ["Viewer", "Editor"]:
            if (not last_login_ms) and created_ms and created_ms < cutoff_365_ms:
                suggestion = (
                    "User has never logged in and was added more than 1 year ago. "
                    "Consider removing the user if you need more Viewer or Contributor licenses."
                )

        elif user_type == "Mobile Worker":
            if last_login_ms and last_login_ms < cutoff_365_ms:
                suggestion = (
                    "Mobile Worker has not logged in within the last year. "
                    "Consider reassignment to Contributor or Viewer."
                )

        elif user_type == "Creator":
            if item_count == 0:
                suggestion = (
                    "Creator does not own content. Consider reassignment to Contributor or Mobile Worker."
                )
            elif last_login_ms and last_login_ms < cutoff_180_ms:
                suggestion = (
                    "Creator has not logged in within the last 6 months. "
                    "Consider content migration and reassignment."
                )

        elif user_type in ["Professional Plus", "Professional Standard", "Professional Basic", "Advanced"]:
            if last_login_ms and last_login_ms < cutoff_365_ms:
                suggestion = (
                    "No AGOL login in last year. Consider reassignment and follow up on ArcGIS Pro usage."
                )
            elif last_login_ms and last_login_ms < cutoff_180_ms and item_count == 0:
                suggestion = (
                    "No AGOL login in 6 months and no AGOL content. "
                    "Consider reassignment and follow up on ArcGIS Pro usage."
                )

        # ---------------- ROW ----------------
        rows.append({
            "username": username,
            "name": name,
            "email": email,
            "last_login": last_login_dt,
            "created_date": created_dt,
            "role": role,
            "user_type": user_type,
            "group_count": group_count,
            "item_count": item_count,
            "suggestion": suggestion,
            "elevated_privileges": elevated_privileges_yn,
            "disabled_flag": disabled_yn,
            "mfa_enabled": mfa_yn,
            "idp_provider": idp_provider
        })

        if i % 25 == 0:
            print(f"Processed {i}/{total_users} users...")

        # -------- PERIODIC UPDATES --------
        processed += 1
        if verbose and print_every_n and (processed % int(print_every_n) == 0 or processed == total_targets):
            now = time.perf_counter()
            elapsed = now - t0
            avg = elapsed / max(1, processed)
            remaining = total_targets - processed
            eta = remaining * avg

            # optional: throttle if N is small, avoid printing too fast
            if now - last_print_t >= 0.2 or processed == total_targets:
                pct = processed / max(1, total_targets) * 100
                print(
                    f"  Progress: {processed:,}/{total_targets:,} "
                    f"({pct:,.1f}%) | "
                    f"elapsed {elapsed:,.1f}s | avg {avg:,.3f}s/user | ETA {eta/60:,.1f} min"
                )
                last_print_t = now

    df_users = pd.DataFrame(rows)

    print(f"✓ Processed {len(df_users):,} users in {time.perf_counter() - start:.1f}s.")

    return df_users


def get_user_item_counts_batch(gis, usernames, batch_size=50):
    """
    Efficiently fetch item counts for multiple users in batches.
    
    Parameters
    ----------
    gis : arcgis.gis.GIS
    usernames : list[str]
        List of usernames to fetch item counts for
    batch_size : int
        Number of usernames to query in each batch
        
    Returns
    -------
    dict
        Dictionary mapping username -> item count
    """
    user_counts = {}
    
    # Process usernames in batches to reduce API calls
    for i in range(0, len(usernames), batch_size):
        batch = usernames[i:i + batch_size]
        
        for username in batch:
            try:
                # Use advanced_search with owner filter for efficiency
                res = gis.content.advanced_search(
                    query=f'owner:"{username}"',
                    max_items=1,
                    as_dict=True,
                )
                user_counts[username] = int(res.get("total", 0))
            except Exception as e:
                print(f"Warning: Failed to get item count for {username}: {e}")
                user_counts[username] = 0
                
        # Small delay to avoid rate limiting
        if i + batch_size < len(usernames):
            time.sleep(0.1)
    
    return user_counts


def get_user_item_count(gis, username):
    """
    Return the total number of items owned by a user via a lightweight search.
    (Legacy function - use get_user_item_counts_batch for better performance)

    Parameters
    ----------
    gis : arcgis.gis.GIS
    username : str

    Returns
    -------
    int
    """
    try:
        res = gis.content.advanced_search(
            query=f'owner:"{username}"',
            max_items=1,
            as_dict=True,
        )
        return int(res.get("total", 0))
    except Exception:
        return 0


# License Table
def build_license_summary(gis, allowed_types=None):
    """
    Build an AGOL license inventory summary.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    allowed_types : list[str] | None
        Optional filter for specific user types

    Returns
    -------
    pandas.DataFrame
    """

    print("Fetching license information...")

    try:
        subscription_info = gis.properties.subscriptionInfo.userLicenseTypes
    except Exception as e:
        raise RuntimeError(f"Failed to fetch subscriptionInfo.userLicenseTypes: {e}")

    try:
        assigned_counts_list = gis.users.counts("user_type", as_df=False)
    except Exception as e:
        raise RuntimeError(f"Failed to fetch assigned user counts: {e}")

    # Convert AGOL counts list → dictionary
    assigned_counts = {
        item.get("key"): item.get("count", 0)
        for item in (assigned_counts_list or [])
    }

    license_summary = []

    for ut_id, max_count in (subscription_info or {}).items():

        assigned = assigned_counts.get(ut_id, 0)
        available = max_count - assigned

        license_summary.append({
            "user_type_id": ut_id,
            "total": max_count,
            "assigned": assigned,
            "available": available
        })

    df_licenses = pd.DataFrame(license_summary)

    # ---------------- USER TYPE LABEL MAPPING ----------------
    mapping = {
        "creatorUT": "Creator",
        "GISProfessionalBasicUT": "Professional Basic",
        "GISProfessionalAdvUT": "Professional Plus",
        "GISProfessionalStdUT": "Professional Standard",
        "IndoorsUserUT": "Indoors User",
        "standardUT": "Standard",
        "advancedUT": "Advanced",
        "basicUT": "Basic",
        "editorUT": "Editor",
        "fieldWorkerUT": "Mobile Worker",
        "viewerUT": "Viewer"
    }

    df_licenses["user_type"] = df_licenses["user_type_id"].map(mapping)

    # Preserve unknown license types instead of dropping them
    df_licenses["user_type"] = df_licenses["user_type"].fillna(df_licenses["user_type_id"])

    df_licenses.drop(columns=["user_type_id"], inplace=True)

    # ---------------- OPTIONAL FILTER ----------------
    if allowed_types:
        df_licenses = df_licenses[df_licenses["user_type"].isin(allowed_types)]

    print("✓ License summary complete")

    return df_licenses


print("✓ Users & Licensing functions loaded")
```

### Step 2c.2: Define Group Functions


```python
# -------------------------------------------------------
# Groups — Helper Functions
# -------------------------------------------------------

def get_group_type(group):
    """
    Label a group as Standard and/or Shared Update / Partnered / Distributed collaboration.

    Parameters
    ----------
    group : dict-like or arcgis.gis.Group

    Returns
    -------
    str
    """
    try:
        type_keywords = safe_get(group, "typeKeywords", []) or []
        capabilities  = safe_get(group, "capabilities", []) or []

        is_shared_update = (
            "Shared Update" in type_keywords
            or "updateitemcontrol" in str(capabilities).lower()
        )
        is_partnered    = bool(safe_get(group, "isPartnerCollab")) or ("Partner Collaboration" in type_keywords)
        is_distributed  = bool(safe_get(group, "isDistributedCollab")) or ("Distributed Collaboration" in type_keywords)

        parts = []
        if is_partnered:
            parts.append("Partnered Collaboration")
        if is_distributed:
            parts.append("Distributed Collaboration")
        if is_shared_update:
            parts.append("Shared Update")

        return ", ".join(parts) if parts else "Standard"
    except Exception:
        return "Standard"


def get_group_sharing_level(group):
    """
    Convert a group access value into a friendly sharing level label.

    Parameters
    ----------
    group : object or dict-like

    Returns
    -------
    str
        "Public", "Organization", or "Private".
    """
    try:
        access = getattr(group, "access", "private")
        if access == "public":
            return "Public"
        if access == "org":
            return "Organization"
        return "Private"
    except Exception:
        return "Private"


def is_living_atlas_item(item):
    """
    Return True if the item owner matches known Esri/Living Atlas system account patterns.

    Used to exclude official Esri content from average view calculations.

    Parameters
    ----------
    item : object or dict-like

    Returns
    -------
    bool
    """
    try:
        owner = (safe_get(item, "owner", "") or "").lower()
        esri_exact = {
            "esri", "esri_livingatlas", "esri_demographics", "esri_boundaries",
            "esri_basemaps", "esri_landscape", "esri_imagery", "esri_elevation",
            "esri_vector", "esri_cartography", "esri_hydro", "esri_apps",
            "esri_media", "esri_3d", "esri_livefeeds", "esri_analytics",
        }
        if owner in esri_exact:
            return True
        if owner.startswith("esri_") and "@" not in owner:
            return True
        return False
    except Exception:
        return False


# -------------------------------------------------------
# Groups — Inventory Function
# -------------------------------------------------------

def build_user_lookup(gis, max_users=10000, convert_to_datetime=False):
    """
    Build a {username: {full_name, last_login}} lookup dictionary.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    max_users : int
    convert_to_datetime : bool
        If True, last_login is returned as a datetime instead of epoch ms.

    Returns
    -------
    dict
    """
    print(f"Fetching users for lookup (max={max_users})...")

    try:
        all_users = gis.users.search(query="*", max_users=max_users)
    except Exception as e:
        raise RuntimeError(f"Failed to search users: {e}") from e

    user_info = {}
    for u in all_users:
        uname = safe_get(u, "username")
        if not uname:
            continue

        last_login = safe_get(u, "lastLogin")
        if last_login in (None, -1, 0):
            last_login = safe_get(safe_get(u, "properties", {}), "lastLogin")
        if last_login in (-1, 0):
            last_login = None

        if convert_to_datetime:
            last_login = ms_to_dt(last_login)

        full_name = (
            safe_get(u, "fullName")
            or safe_get(u, "full_name")
            or uname
        )
        user_info[uname] = {"full_name": full_name, "last_login": last_login}

    return user_info


# -------------------------------------------------------
# Groups — Table Assembly Function
# -------------------------------------------------------

def build_group_snapshot(groups_to_analyze, user_info,
                         recent_days_threshold=RECENT_DAYS_THRESHOLD,
                         max_items_per_group=1000):
    """
    Build a governance/activity snapshot DataFrame for ArcGIS groups.

    Parameters
    ----------
    groups_to_analyze : list[Group]
    user_info : dict
        Output from build_user_lookup().
    recent_days_threshold : int
        Number of days defining an "active" user or content update.
    max_items_per_group : int
        Max items to fetch per group for metric calculations.

    Returns
    -------
    pandas.DataFrame
    """
    verbose       = True
    print_every_n = 50

    rows         = []
    total_groups = len(groups_to_analyze)
    print(f"Analyzing {total_groups:,} groups...")

    t0           = time.perf_counter()
    last_print_t = t0

    for idx, g in enumerate(groups_to_analyze, start=1):

        gid = safe_get(g, "id")

        # Members
        try:
            m          = g.get_members()
            usernames  = (m.get("users", []) or []) + (m.get("admins", []) or [])
            member_count = len(usernames)
        except Exception as e:
            print(f"  ⚠ Failed to fetch members for group {gid}: {e}")
            usernames    = []
            member_count = 0

        # Content
        try:
            content    = g.content(max_items=max_items_per_group) or []
            item_count = len(content)
        except Exception as e:
            print(f"  ⚠ Failed to fetch content for group {gid}: {e}")
            content    = []
            item_count = 0

        # Group URL
        try:
            group_url = g.homepage or f"{gis.url}/home/group.html?id={g.id}"
        except Exception:
            group_url = None

        # Active members
        active_members = 0
        for un in usernames:
            try:
                last_login_ms = safe_get(user_info.get(un, {}), "last_login")
                d = days_since_ms(last_login_ms)
                if d is not None and d <= recent_days_threshold:
                    active_members += 1
            except Exception:
                pass

        # Content metrics
        most_recent_modified  = None
        total_views           = 0
        non_esri_item_count   = 0

        for item in content[:100]:
            try:
                mod = safe_get(item, "modified")
                if mod and (most_recent_modified is None or mod > most_recent_modified):
                    most_recent_modified = mod
                if not is_living_atlas_item(item):
                    total_views         += int(safe_get(item, "numViews", 0) or 0)
                    non_esri_item_count += 1
            except Exception:
                pass

        avg_views = round(total_views / non_esri_item_count, 2) if non_esri_item_count else 0.0

        days_since_content_update = (
            int(round(days_since_ms(most_recent_modified)))
            if most_recent_modified else None
        )

        owner_username = safe_get(g, "owner", "")
        u              = gis.users.get(owner_username) if owner_username else None
        owner_name     = safe_get(u, "fullName", owner_username) if u else owner_username
        email          = safe_get(u, "email", None) if u else None

        rows.append({
            "group_id":                  gid,
            "group_title":               truncate_string(safe_get(g, "title",   ""), 256),
            "group_summary":             truncate_string(safe_get(g, "snippet", ""), 256),
            "group_description":         truncate_string(strip_html_to_text(safe_get(g, "description", "")), 256),
            "group_tags":                truncate_string(", ".join(safe_get(g, "tags", []) or []), 256),
            "group_type":                get_group_type(g),
            "owner_username":            owner_username,
            "owner_name":                owner_name,
            "owner_email":               email,
            "creation_date":             ms_to_dt(safe_get(g, "created")),
            "sharing_lvl":               truncate_string(get_group_sharing_level(g), 256),
            "item_count":                item_count,
            "member_count":              member_count,
            "active_members":            active_members,
            "avg_views_per_item":        avg_views,
            "days_since_content_update": days_since_content_update,
            "group_url":                 group_url,
        })

        # Periodic progress update
        if verbose and (idx % print_every_n == 0 or idx == total_groups):
            now     = time.perf_counter()
            elapsed = now - t0
            avg     = elapsed / idx
            eta     = (total_groups - idx) * avg
            if now - last_print_t >= 0.2 or idx == total_groups:
                print(
                    f"  Progress: {idx:,}/{total_groups:,} "
                    f"({idx / total_groups * 100:.1f}%) | "
                    f"elapsed {elapsed:.1f}s | avg {avg:.3f}s/item | ETA {eta / 60:.1f} min"
                )
                last_print_t = now

    df_group_snapshot = pd.DataFrame(rows)

    if "days_since_content_update" in df_group_snapshot.columns:
        df_group_snapshot["days_since_content_update"] = (
            df_group_snapshot["days_since_content_update"].astype("Int64")
        )

    return df_group_snapshot


print("✓ Groups functions loaded")
```

### Step 2c.3 Define Content Functions


```python
# -------------------------------------------------------
# Content — Helper Functions
# -------------------------------------------------------

def format_slice(ms0, ms1):
    """
    Build a human-readable description of a created-date search slice.

    Parameters
    ----------
    ms0 : int or str
        Start epoch time in milliseconds.
    ms1 : int or str
        End epoch time in milliseconds.

    Returns
    -------
    str
    """
    s0 = dt_str_from_ms(ms0)
    s1 = dt_str_from_ms(ms1)
    if s0 and s1:
        return f"Items created between {s0} and {s1}"
    if s0:
        return f"Items created after {s0}"
    if s1:
        return f"Items created before {s1}"
    return "Items with unknown creation date"


def get_sharing_level(access):
    """
    Convert an item access value into a friendly sharing level label.

    Parameters
    ----------
    access : str

    Returns
    -------
    str
        "Public", "Organization", or "Private".
    """
    try:
        if access == "public":
            return "Public"
        if access == "org":
            return "Organization"
        return "Private"
    except Exception:
        return "Private"


def get_content_status(item):
    """
    Return a friendly content status label from an item's contentStatus field.

    Parameters
    ----------
    item : dict or object

    Returns
    -------
    str or None
    """
    try:
        status = safe_get(item, "contentStatus", None)
        status_map = {
            "authoritative":        "Authoritative",
            "org_authoritative":    "Organization Authoritative",
            "public_authoritative": "Public Authoritative",
            "deprecated":           "Deprecated",
        }
        return status_map.get(status, None)
    except Exception:
        return None


# -------------------------------------------------------
# Content — Inventory Functions
# -------------------------------------------------------

def advanced_search_page(gis, query, start=1, page_size=100,
                         sort_field="created", sort_order="asc"):
    """
    Execute a single paged ArcGIS advanced content search, returning raw dicts.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    query : str
    start : int
    page_size : int
    sort_field : str
    sort_order : str

    Returns
    -------
    dict
        Raw advanced_search response.
    """
    return gis.content.advanced_search(
        query=query,
        start=start,
        max_items=page_size,
        sort_field=sort_field,
        sort_order=sort_order,
        as_dict=True,
    )


def get_created_bounds_ms(gis, org_filter):
    """
    Determine the earliest and latest creation timestamps for org-owned items.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    org_filter : str

    Returns
    -------
    tuple
        (min_created_ms, max_created_ms)
    """
    a = advanced_search_page(gis, org_filter, page_size=50, sort_order="asc")
    min_ms = next(
        (r.get("created") for r in a.get("results", [])
         if isinstance(r.get("created"), int) and r["created"] > 0),
        None
    )

    b = advanced_search_page(gis, org_filter, page_size=1, sort_order="desc")
    max_ms = next(
        (r.get("created") for r in b.get("results", [])
         if isinstance(r.get("created"), int) and r["created"] > 0),
        None
    )

    now_ms = int(datetime.now(tz=timezone.utc).timestamp() * 1000)
    return (min_ms or 0), (max_ms or now_ms)


def fetch_all_for_query_up_to_10k(gis, query, test_cap=None):
    """
    Retrieve all items for a query below the 10,000 search ceiling via paging.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    query : str
    test_cap : int, optional

    Returns
    -------
    list[dict]
    """
    out, start = [], 1
    while True:
        res     = advanced_search_page(gis, query, start=start, page_size=100)
        results = res.get("results") or []
        if not results:
            break

        if test_cap is not None:
            remaining = test_cap - len(out)
            if remaining <= 0:
                break
            results = results[:remaining]

        out.extend(results)
        start = res.get("nextStart", -1)
        if start == -1:
            break

    return out


def inventory_org_items(gis, additional_query="*", min_range_ms=86_400_000, test_cap=None):
    """
    Inventory all items owned by the ArcGIS organization.

    Automatically splits searches into created-date slices to stay under the
    10,000-item search limit.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    additional_query : str, optional
    min_range_ms : int, optional
        Minimum slice width before splitting stops. Default is one day.
    test_cap : int, optional
        Limits total retrieved items for testing.

    Returns
    -------
    list[dict]
        Deduplicated list of raw item dictionaries.
    """
    org_id = safe_get(getattr(gis, "properties", None), "id", None)
    if not org_id:
        raise ValueError("Could not determine org id from gis.properties.id")

    org_filter = f'orgid:"{org_id}"'
    start_ms, end_ms = get_created_bounds_ms(gis, org_filter)

    ranges    = [(start_ms, end_ms)]
    all_items = []
    slice_num = 0

    print("\nStarting organization-wide item inventory")

    while ranges:
        if test_cap and len(all_items) >= test_cap:
            print(f"\nTest cap reached: stopping after {test_cap} items")
            break

        ms0, ms1  = ranges.pop()
        slice_num += 1
        query      = f"({org_filter}) AND ({additional_query}) AND created:[{ms0} TO {ms1}]"
        total      = int(advanced_search_page(gis, query, page_size=1).get("total", 0))

        print(f"Slice {slice_num}: {format_slice(ms0, ms1)}")
        print(f"  Items found: {'10,000+' if total >= 10000 else f'{total:,}'}")

        if total >= 10000 and (ms1 - ms0) > min_range_ms:
            mid = (ms0 + ms1) // 2
            ranges.append((mid + 1, ms1))
            ranges.append((ms0,     mid))
            print("  Action: split slice into smaller time windows\n")
            continue

        items = fetch_all_for_query_up_to_10k(gis, query, test_cap)
        all_items.extend(items)
        print(f"  Retrieved:     {len(items):,} items")
        print(f"  Running total: {len(all_items):,}\n")

    return list({d["id"]: d for d in all_items if d.get("id")}.values())


# -------------------------------------------------------
# Content — Table Assembly Functions
# -------------------------------------------------------

def build_governance_table(gis, items):
    """
    Build a content governance DataFrame from raw advanced_search item dicts.

    Includes a user lookup cache to reduce redundant API calls for
    owner metadata.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    items : list[dict]

    Returns
    -------
    pandas.DataFrame
    """
    base       = (getattr(gis, "url", None) or "https://www.arcgis.com").rstrip("/")
    rows       = []
    user_cache = {}

    for d in items:
        modified_ms    = d.get("modified")
        last_viewed_ms = d.get("lastViewed")
        created_ms     = d.get("created")
        item_id        = d.get("id")
        owner_username = safe_get(d, "owner", "")

        if owner_username in user_cache:
            u = user_cache[owner_username]
        else:
            u = gis.users.get(owner_username) if owner_username else None
            user_cache[owner_username] = u

        rows.append({
            "item_id":               item_id,
            "title":                 d.get("title"),
            "owner_username":        owner_username,
            "owner_name":            safe_get(u, "fullName", owner_username) if u else owner_username,
            "owner_email":           safe_get(u, "email", None) if u else None,
            "item_type":             d.get("type"),
            "tags":                  truncate_string(", ".join(safe_get(d, "tags", []) or []), 256),
            "summary":               truncate_string(safe_get(d, "summary",     ""), 256),
            "description":           truncate_string(strip_html_to_text(safe_get(d, "description", "")), 256),
            "view_count":            int(d.get("numViews") or 0),
            "creation_date":         dt_str_from_ms(created_ms),
            "last_updated":          dt_str_from_ms(modified_ms),
            "last_viewed":           dt_str_from_ms(last_viewed_ms),
            "sharing_lvl":           get_sharing_level(d.get("access")),
            "metadata_completeness": d.get("scoreCompleteness", 0),
            "days_since_update":     days_since_ms(modified_ms) if modified_ms else None,
            "content_status":        get_content_status(d),
            "item_page":             f"{base}/home/item.html?id={item_id}" if item_id else "",
            "size_mb":               None,
            "attachments_size_mb":   None,
        })

    print(f"✓ Governance table built ({len(rows):,} items)")
    return pd.DataFrame(rows)


def add_item_sizes(gis, df, id_col="item_id", size_col="size_mb",
                   attachment_size_col="attachments_size_mb",
                   max_workers=20, chunk_size=100):
    """
    Fetch item sizes in parallel and append them to the DataFrame.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    df : pandas.DataFrame
    id_col : str
    size_col : str
    attachment_size_col : str
    max_workers : int
    chunk_size : int

    Returns
    -------
    pandas.DataFrame
    """
    ids        = df[id_col].tolist()
    total_rows = len(df)

    def fetch_size(item_id):
        try:
            it               = gis.content.get(item_id)
            size_bytes       = getattr(it, "size",            0) or 0
            attachment_bytes = getattr(it, "attachment_size", 0) or 0
            return round(size_bytes / (1024 * 1024), 2), round(attachment_bytes / (1024 * 1024), 2)
        except Exception:
            return (None, None)

    print(f"Fetching sizes for {total_rows:,} items using {max_workers} threads...")

    t0           = time.perf_counter()
    last_print_t = t0
    sizes_list       = []
    attachments_list = []

    for start in range(0, total_rows, chunk_size):
        end      = min(start + chunk_size, total_rows)
        chunk_ids = ids[start:end]

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            results = list(executor.map(fetch_size, chunk_ids))

        sizes, attachments = zip(*results)
        sizes_list.extend(sizes)
        attachments_list.extend(attachments)

        processed     = start + len(chunk_ids)
        total_elapsed = time.perf_counter() - t0
        chunk_elapsed = time.perf_counter() - last_print_t
        print(
            f"✓ Processed {processed}/{total_rows} items "
            f"({processed / total_rows:.1%}) in {total_elapsed:.1f}s "
            f"[chunk took {chunk_elapsed:.1f}s]"
        )
        last_print_t = time.perf_counter()

    df = df.copy()
    df[size_col]             = [v if v != -0.0 else 0.0 for v in sizes_list]
    df[attachment_size_col]  = [v if v != -0.0 else 0.0 for v in attachments_list]

    print("✓ Item size retrieval complete")
    return df


def run_governance_pipeline(gis, items):
    """
    Build the full content governance DataFrame, including item sizes.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    items : list[dict]
        Raw item dicts from inventory_org_items().

    Returns
    -------
    pandas.DataFrame
    """
    print("Building governance table...")
    df = build_governance_table(gis, items)

    df = add_item_sizes(gis, df)

    print("✓ Governance dataset ready")
    return df


print("✓ Content inventory and table functions loaded")
```

### Step 2c.3 Define Dependency Functions


```python
# ----------------------------------------
# 1. Build dependency graph
# ----------------------------------------
def create_dependency_graph(
    gis,
    items,
    dependency_mapping,
    batch_size=5,
    outside_org=False,
    include_reverse=False,
    print_every_n=10
):
    """
    Build an ArcGIS ItemGraph representing dependency relationships between items.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    items : list[arcgis.gis.Item]
    dependency_mapping : bool
    batch_size : int
    outside_org : bool
    include_reverse : bool
    print_every_n : int

    Returns
    -------
    arcgis.apps.itemgraph.ItemGraph
    """
    if not dependency_mapping:
        return None  # Skip graph creation if mapping is disabled

    g = ItemGraph(gis=gis)

    total_targets = len(items or [])
    if total_targets == 0:
        return g

    batches = math.ceil(total_targets / batch_size)
    processed = 0
    next_print = print_every_n
    t0 = time.perf_counter()

    for i in range(batches):
        start = i * batch_size
        end = min((i + 1) * batch_size, total_targets)
        batch = items[start:end]

        try:
            g.add_dependencies(batch, outside_org=outside_org, include_reverse=include_reverse)
        except Exception:
            # Try one by one if batch fails
            for item in batch:
                try:
                    g.add_dependencies([item], outside_org=outside_org, include_reverse=include_reverse)
                except Exception:
                    pass  # silently skip failed items

        processed += len(batch)

        # Periodic updates
        if processed >= next_print or processed == total_targets:
            now = time.perf_counter()
            elapsed = now - t0
            avg = elapsed / max(1, processed)
            remaining = total_targets - processed
            eta = remaining * avg
            pct = processed / max(1, total_targets) * 100

            print(
                f"  Progress: {processed:,}/{total_targets:,} "
                f"({pct:,.1f}%) | "
                f"elapsed {elapsed:,.1f}s | avg {avg:,.3f}s/item | ETA {eta/60:,.1f} min"
            )
            next_print += print_every_n

    print("✓ Graph build complete")
    return g

# ----------------------------------------
# 2. Flatten ItemGraph
# ----------------------------------------
def flatten_itemgraph(graph, dependency_mapping):
    """
    Flatten ItemGraph into upstream and downstream dependency DataFrames.

    Returns
    -------
    df_upstream, df_downstream
    """
    if not dependency_mapping:
        # Return empty DataFrames instead of None to avoid unpack errors
        return pd.DataFrame(), pd.DataFrame()

    rows_upstream = []
    rows_downstream = []

    item_ids = graph.all_items(out_format="id") or []

    for parent_id in item_ids:
        node = graph.get_node(parent_id)
        if node is None:
            continue
        try:
            child_ids = node.contains(out_format="id") or []
        except Exception:
            child_ids = []

        for child_id in child_ids:
            rows_upstream.append({
                "source_itemid": parent_id,
                "relation": "depends on",
                "target_itemid": child_id
            })
            rows_downstream.append({
                "source_itemid": child_id,
                "relation": "dependency of",
                "target_itemid": parent_id
            })

    cols = ["source_itemid", "relation", "target_itemid"]
    df_upstream = pd.DataFrame(rows_upstream, columns=cols)
    df_downstream = pd.DataFrame(rows_downstream, columns=cols)
    return df_upstream, df_downstream

# ----------------------------------------
# 3. Build dependency counts
# ----------------------------------------
def build_dependency_counts(df_upstream, df_downstream, dependency_mapping):
    """
    Build a per-item count table from flattened edge tables.
    """
    if not dependency_mapping:
        return pd.DataFrame()  # return empty DF if mapping disabled

    required_cols = ["source_itemid", "target_itemid"]
    for name, df in [("df_upstream", df_upstream), ("df_downstream", df_downstream)]:
        for col in required_cols:
            if col not in df.columns:
                raise ValueError(f"{name} missing required column: {col}. Found: {list(df.columns)}")

    up = (
        df_upstream.groupby("source_itemid")
        .size()
        .rename("upstream_count")
        .reset_index()
        .rename(columns={"source_itemid": "itemid"})
    )
    down = (
        df_downstream.groupby("source_itemid")
        .size()
        .rename("downstream_count")
        .reset_index()
        .rename(columns={"source_itemid": "itemid"})
    )

    df_counts = pd.merge(up, down, on="itemid", how="outer").fillna(0)
    df_counts["upstream_count"] = df_counts["upstream_count"].astype(int)
    df_counts["downstream_count"] = df_counts["downstream_count"].astype(int)
    df_counts["total_dependencies"] = df_counts["upstream_count"] + df_counts["downstream_count"]
    return df_counts.rename(columns={"itemid": "reference_id"})

# ----------------------------------------
# 4. Enrich dependency DataFrame
# ----------------------------------------
def enrich_deps(deps_df, content_df, dependency_mapping, prefix='dependency'):
    """
    Add content metadata to dependency DataFrame.
    """
    if not dependency_mapping:
        return pd.DataFrame()

    return (
        deps_df
        .merge(content_df, left_on='target_itemid', right_on='item_id')
        .drop(columns='item_id')
        .rename(columns={col: f'{prefix}_{col}' for col in content_df.columns if col != 'item_id'})
        .merge(content_df[['item_id', 'title']], left_on='source_itemid', right_on='item_id', how='left')
        .drop(columns='item_id')
        .rename(columns={'title': 'source_title'})
    )

def run_dependency_pipeline(
    gis,
    items_dicts,
    content_df,
    dependency_mapping=True,
    batch_size=5,
    outside_org=False,
    include_reverse=False,
    print_every_n=10
):
    """
    Full pipeline to build dependency graph, flatten it, enrich with metadata,
    compute counts, and merge back into content DataFrame.

    If dependency_mapping=False, the pipeline is completely skipped and
    content_df is returned as-is.

    Parameters
    ----------
    gis : arcgis.gis.GIS
    items_dicts : list[dict]
        List of items, each dict must contain 'id'.
    content_df : pd.DataFrame
        Content metadata with at least 'item_id' and 'title'.
    dependency_mapping : bool, optional
    batch_size : int, optional
    outside_org : bool, optional
    include_reverse : bool, optional
    print_every_n : int, optional

    Returns
    -------
    pd.DataFrame
        Either enriched content DataFrame or original content_df if mapping is disabled.
    """

    # Early exit if dependency mapping is disabled
    if not dependency_mapping:
        empty = pd.DataFrame()
        return empty, empty, content_df.copy()


    # 1. Build dependency graph
    ids = [item["id"] for item in items_dicts]
    graph = create_dependency_graph(
        gis=gis,
        items=ids,
        dependency_mapping=dependency_mapping,
        batch_size=batch_size,
        outside_org=outside_org,
        include_reverse=include_reverse,
        print_every_n=print_every_n
    )

    # Safe fallback if graph is None
    if graph is None:
        upstream_deps, downstream_deps = pd.DataFrame(), pd.DataFrame()
    else:
        # 2. Flatten graph
        upstream_deps, downstream_deps = flatten_itemgraph(
            graph,
            dependency_mapping
        )

    # 3. Enrich dependencies with content metadata
    upstream_df = enrich_deps(
        upstream_deps,
        content_df,
        dependency_mapping,
        prefix='upstream'
    )
    downstream_df = enrich_deps(
        downstream_deps,
        content_df,
        dependency_mapping,
        prefix='downstream'
    )

    # 4. Compute per-item dependency counts
    deps_counts = build_dependency_counts(
        upstream_deps,
        downstream_deps,
        dependency_mapping
    )

    # 5. Merge counts back into content DataFrame
    final_content_df = pd.merge(
        content_df,
        deps_counts,
        left_on="item_id",
        right_on="reference_id",
        how="left"
    )

    # Drop helper reference column
    final_content_df = final_content_df.drop(columns=["reference_id"], errors="ignore")

    # Fill missing counts with 0 and convert to int
    for col in ["upstream_count", "downstream_count", "total_dependencies"]:
        if col in final_content_df.columns:
            final_content_df[col] = final_content_df[col].fillna(0).astype(int)

    return upstream_df, downstream_df, final_content_df
    
print("✓ Dependency functions loaded")
```

# Step 3: Compile and Publish Content

## Step 3a: Users & Licensing

### Step 3a.1: Build Users & Licensing Tables


```python
users = gis.users.search(max_users=USER_LIMIT)
print(f"Found {len(users)} users")

df_users = build_user_snapshot(
    gis=gis,
    users=users,
    role_map=ROLE_MAP,
    usertype_map=USERTYPE_MAP,
    elev_prefixes=ELEV_PREFIXES,
    cutoff_365_ms=cutoff_365_ms,
    cutoff_180_ms=cutoff_180_ms,
)

df_licenses = build_license_summary(
    gis,
    allowed_types=["Creator", "Editor", "Viewer", "Mobile Worker", "Advanced", "Professional Plus"]
)

display(df_users.head())
display(df_licenses)
```

    2026-07-01 12:02:52,834 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:52,889 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,004 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,125 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,244 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,366 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,480 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,539 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,599 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,660 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,723 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,778 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,896 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:53,953 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,008 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,073 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,130 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,189 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,252 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,309 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,369 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,427 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,490 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,549 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,607 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,668 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,726 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,784 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,846 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,903 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:54,961 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,027 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,091 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,148 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,204 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,265 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,323 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,383 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,448 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,506 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,572 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,627 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,687 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,754 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,819 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,875 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,931 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:55,985 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,038 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,097 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,160 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,217 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,275 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,336 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,393 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,457 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,521 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,579 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,636 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,693 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,754 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,810 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,876 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,936 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:56,994 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,052 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,116 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,172 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,234 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,291 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,355 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,411 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,477 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,537 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,597 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,654 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,711 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,773 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,838 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,894 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:57,949 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,012 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,071 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,134 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,190 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,249 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,305 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,364 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,430 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,492 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,558 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,618 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,677 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,733 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,790 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,854 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,910 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:58,968 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,035 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,088 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,144 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,199 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,257 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,316 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,380 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,440 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,495 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,551 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,614 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,678 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,735 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,792 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,850 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,907 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:02:59,962 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,031 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,091 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,157 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,212 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,270 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,328 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,420 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,474 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,530 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,595 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,658 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,723 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,780 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,838 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,905 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:00,962 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,025 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,083 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,142 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,199 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,256 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,311 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,368 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,432 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,487 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,549 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,604 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,667 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,730 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,796 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,857 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,915 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:01,972 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,036 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,102 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,164 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,226 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,284 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,343 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,407 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,470 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,534 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,593 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,658 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,715 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,771 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,832 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,892 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:02,954 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,010 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,065 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,121 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,182 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,246 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,303 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,370 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,433 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,492 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,558 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,616 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,675 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,737 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,801 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,856 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,911 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:03,974 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,031 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,088 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,156 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,216 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,274 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,335 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,390 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,446 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,505 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,562 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,629 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,683 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,738 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,796 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:04,851 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,105 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,344 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,412 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,472 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,527 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,585 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,643 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,705 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,770 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:05,834 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,095 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,333 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,395 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,456 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,514 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,579 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,636 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,699 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,756 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,817 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,873 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,932 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:06,996 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,062 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,118 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,174 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,233 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,292 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,350 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,412 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,477 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,537 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,594 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,649 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,710 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,775 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,840 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,896 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:07,963 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,021 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,080 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,143 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,198 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,257 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,317 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,373 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,430 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,487 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,547 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,601 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,659 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,715 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,773 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,832 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,891 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:08,957 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,017 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,074 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,137 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,200 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,257 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,318 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,388 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,445 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,503 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,561 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,620 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,692 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,756 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,814 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,871 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,927 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:09,988 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:10,052 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:10,116 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:10,173 - INFO - Retrieving roles(start=1, num=100)


    Found 297 users
    Analyzing users: 297
    Fetching user item counts in batches...


    2026-07-01 12:03:35,947 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:36,684 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:37,658 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:38,513 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:39,601 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:42,690 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:44,124 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:45,022 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:46,214 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:46,502 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:47,462 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:47,871 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:48,760 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:51,531 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:53,048 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:54,995 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:55,547 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:56,604 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:57,329 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:03:58,243 - INFO - Retrieving roles(start=1, num=100)


    Processed 25/297 users...
      Progress: 25/297 (8.4%) | elapsed 22.3s | avg 0.892s/user | ETA 4.0 min


    2026-07-01 12:03:59,209 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:00,268 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:01,373 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:02,036 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:07,580 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:08,590 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:13,278 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:15,526 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:18,532 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:23,038 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:23,839 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:25,305 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:27,012 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:28,349 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:28,650 - INFO - Retrieving roles(start=1, num=100)


    Processed 50/297 users...
      Progress: 50/297 (16.8%) | elapsed 53.4s | avg 1.067s/user | ETA 4.4 min


    2026-07-01 12:04:31,799 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:33,734 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:35,784 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:36,693 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:38,024 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:39,223 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:42,803 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:43,742 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:45,527 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:45,951 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:47,191 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:48,083 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:49,041 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:49,976 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:54,275 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:55,613 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:04:58,576 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:04,348 - INFO - Retrieving roles(start=1, num=100)


    Processed 75/297 users...
      Progress: 75/297 (25.3%) | elapsed 88.4s | avg 1.179s/user | ETA 4.4 min


    2026-07-01 12:05:06,618 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:07,813 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:08,095 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:09,277 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:11,739 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:12,392 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:12,906 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:14,092 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:15,126 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:15,672 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:16,105 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:18,479 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:22,848 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:25,775 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:27,948 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:28,898 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:29,868 - INFO - Retrieving roles(start=1, num=100)


    Processed 100/297 users...
      Progress: 100/297 (33.7%) | elapsed 113.9s | avg 1.139s/user | ETA 3.7 min


    2026-07-01 12:05:30,624 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:31,156 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:32,274 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:33,092 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:37,326 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:41,987 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:42,300 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:43,536 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:44,455 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:44,754 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:45,690 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:46,762 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:48,677 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:53,621 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:54,247 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:55,261 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:56,460 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:05:57,524 - INFO - Retrieving roles(start=1, num=100)


    Processed 125/297 users...
      Progress: 125/297 (42.1%) | elapsed 141.6s | avg 1.133s/user | ETA 3.2 min


    2026-07-01 12:05:58,425 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:00,440 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:03,496 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:06,873 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:07,844 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:08,791 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:10,348 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:11,550 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:13,290 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:14,378 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:15,126 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:16,309 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:18,186 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:19,892 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:21,339 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:22,000 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:23,195 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:24,101 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:24,504 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:25,605 - INFO - Retrieving roles(start=1, num=100)


    Processed 150/297 users...
      Progress: 150/297 (50.5%) | elapsed 169.7s | avg 1.131s/user | ETA 2.8 min


    2026-07-01 12:06:26,902 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:27,999 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:28,279 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:30,684 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:31,772 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:32,050 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:36,529 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:37,175 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:38,658 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:39,744 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:41,103 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:42,362 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:43,506 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:44,579 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:46,298 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:47,896 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:49,181 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:52,178 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:52,747 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:53,905 - INFO - Retrieving roles(start=1, num=100)


    Processed 175/297 users...
      Progress: 175/297 (58.9%) | elapsed 197.9s | avg 1.131s/user | ETA 2.3 min


    2026-07-01 12:06:55,758 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:06:57,520 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:00,184 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:01,162 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:02,451 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:04,550 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:05,652 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:06,479 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:08,776 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:11,051 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:12,125 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:13,083 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:15,288 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:16,254 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:17,551 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:20,189 - INFO - Retrieving roles(start=1, num=100)


    Processed 200/297 users...
      Progress: 200/297 (67.3%) | elapsed 224.2s | avg 1.121s/user | ETA 1.8 min


    2026-07-01 12:07:20,769 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:21,689 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:22,660 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:25,845 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:26,386 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:27,708 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:28,922 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:30,019 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:30,928 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:33,116 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:34,198 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:35,140 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:36,231 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:37,186 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:38,134 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:39,634 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:49,249 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:50,031 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:51,012 - INFO - Retrieving roles(start=1, num=100)


    Processed 225/297 users...
      Progress: 225/297 (75.8%) | elapsed 255.1s | avg 1.134s/user | ETA 1.4 min


    2026-07-01 12:07:56,257 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:57,213 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:57,498 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:58,420 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:07:59,677 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:01,964 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:05,318 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:06,500 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:07,446 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:09,355 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:10,001 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:10,531 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:11,476 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:14,318 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:20,200 - INFO - Retrieving roles(start=1, num=100)


    Processed 250/297 users...
      Progress: 250/297 (84.2%) | elapsed 285.8s | avg 1.143s/user | ETA 0.9 min


    2026-07-01 12:08:22,297 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:22,593 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:22,755 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:24,515 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:26,554 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:29,969 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:33,717 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:34,644 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:35,582 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:36,536 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:37,642 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:38,818 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:39,686 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:40,816 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:41,757 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:42,514 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:43,417 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:43,945 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:44,868 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:45,919 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:47,078 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:48,176 - INFO - Retrieving roles(start=1, num=100)


    Processed 275/297 users...
      Progress: 275/297 (92.6%) | elapsed 312.2s | avg 1.135s/user | ETA 0.4 min


    2026-07-01 12:08:49,273 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:49,553 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:50,426 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:51,260 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:52,302 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:54,155 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:55,289 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:56,139 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:58,337 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:08:59,438 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:01,828 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:04,063 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:06,414 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:07,351 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:08,108 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:09,145 - INFO - Retrieving roles(start=1, num=100)


      Progress: 297/297 (100.0%) | elapsed 334.1s | avg 1.125s/user | ETA 0.0 min
    ✓ Processed 297 users in 359.8s.
    Fetching license information...
    ✓ License summary complete



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>username</th>
      <th>name</th>
      <th>email</th>
      <th>last_login</th>
      <th>created_date</th>
      <th>role</th>
      <th>user_type</th>
      <th>group_count</th>
      <th>item_count</th>
      <th>suggestion</th>
      <th>elevated_privileges</th>
      <th>disabled_flag</th>
      <th>mfa_enabled</th>
      <th>idp_provider</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Aaron.Eaddy_TNDOF</td>
      <td>Aaron Eaddy</td>
      <td>Aaron.Eaddy@tn.gov</td>
      <td>2026-06-30 15:07:17+00:00</td>
      <td>2026-01-12 18:54:12+00:00</td>
      <td>Data Editor with External Groups</td>
      <td>Creator</td>
      <td>4</td>
      <td>0</td>
      <td>Creator does not own content. Consider reassig...</td>
      <td>No</td>
      <td>No</td>
      <td>No</td>
      <td>ArcGIS</td>
    </tr>
    <tr>
      <th>1</th>
      <td>aaron.priddy_tndof</td>
      <td>Aaron Priddy</td>
      <td>aaron.priddy@tn.gov</td>
      <td>2026-06-15 13:39:47+00:00</td>
      <td>2023-09-26 11:32:35+00:00</td>
      <td>Data Editor with External Groups</td>
      <td>Mobile Worker</td>
      <td>6</td>
      <td>0</td>
      <td></td>
      <td>No</td>
      <td>No</td>
      <td>No</td>
      <td>ArcGIS</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Aaron.Smith_tndof</td>
      <td>Aaron Smith</td>
      <td>Aaron.Timothy.Smith@tn.gov</td>
      <td>2026-07-01 10:51:20+00:00</td>
      <td>2025-09-22 14:33:58+00:00</td>
      <td>Data Editor with External Groups</td>
      <td>Mobile Worker</td>
      <td>5</td>
      <td>0</td>
      <td></td>
      <td>No</td>
      <td>No</td>
      <td>No</td>
      <td>ArcGIS</td>
    </tr>
    <tr>
      <th>3</th>
      <td>adam.alexander_TNDOF</td>
      <td>Adam Alexander</td>
      <td>adam.alexander@tn.gov</td>
      <td>2026-06-26 23:09:32+00:00</td>
      <td>2021-10-08 15:51:13+00:00</td>
      <td>Data Editor with External Groups</td>
      <td>Mobile Worker</td>
      <td>7</td>
      <td>0</td>
      <td></td>
      <td>No</td>
      <td>No</td>
      <td>No</td>
      <td>ArcGIS</td>
    </tr>
    <tr>
      <th>4</th>
      <td>adam.madewell_TNDOF</td>
      <td>Adam Madewell</td>
      <td>adam.madewell@tn.gov</td>
      <td>2026-06-23 16:26:34+00:00</td>
      <td>2021-10-08 15:15:47+00:00</td>
      <td>Data Editor with External Groups</td>
      <td>Mobile Worker</td>
      <td>7</td>
      <td>0</td>
      <td></td>
      <td>No</td>
      <td>No</td>
      <td>No</td>
      <td>ArcGIS</td>
    </tr>
  </tbody>
</table>
</div>



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>total</th>
      <th>assigned</th>
      <th>available</th>
      <th>user_type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>149</td>
      <td>92</td>
      <td>57</td>
      <td>Creator</td>
    </tr>
    <tr>
      <th>1</th>
      <td>0</td>
      <td>0</td>
      <td>0</td>
      <td>Editor</td>
    </tr>
    <tr>
      <th>2</th>
      <td>205</td>
      <td>201</td>
      <td>4</td>
      <td>Mobile Worker</td>
    </tr>
    <tr>
      <th>3</th>
      <td>6</td>
      <td>4</td>
      <td>2</td>
      <td>Professional Plus</td>
    </tr>
    <tr>
      <th>4</th>
      <td>4</td>
      <td>0</td>
      <td>4</td>
      <td>Viewer</td>
    </tr>
  </tbody>
</table>
</div>


### Step 3b.2: Publish Users & Licensing Tables


```python
# Retrieve table references
users_table,    users_sublayer,    users_url    = get_table_url(agk_fs, "Users",    gis=gis)
licenses_table, licenses_sublayer, licenses_url = get_table_url(agk_fs, "Licenses", gis=gis)

print(f"Users table found    (sublayer={users_sublayer}):    {users_url}")
print(f"Licenses table found (sublayer={licenses_sublayer}): {licenses_url}")

# Clear existing records
if delete_content:
    clear_table(users_table)
    clear_table(licenses_table)

# Publish
publish_dataframe_to_table(df_users,    users_table)
publish_dataframe_to_table(df_licenses, licenses_table)
```

    Truncate successful.
    {'success': True}
    
    Clearing existing records from table: Licenses


    Truncate successful.
    {'success': True}
    
    Publishing to table: Users
    Total records to publish : 297
    Chunk size               : 500
    
    Publishing rows 1–297...


      Attempted : 297
      Succeeded : 297
      Failed    : 0
      Progress: 1/1 (100.0%) | elapsed 1.1s | avg 1.135s/chunk | ETA 0.0 min
    
    Users — Publish complete.
      Total attempted : 297
      Total succeeded : 297
      Total failed    : 0
      Status: SUCCESS
    
    Publishing to table: Licenses
    Total records to publish : 5
    Chunk size               : 500
    
    Publishing rows 1–5...
      Attempted : 5
      Succeeded : 5
      Failed    : 0
      Progress: 1/1 (100.0%) | elapsed 0.2s | avg 0.180s/chunk | ETA 0.0 min
    
    Licenses — Publish complete.
      Total attempted : 5
      Total succeeded : 5
      Total failed    : 0
      Status: SUCCESS


## Step 3b: Groups

### Step 3b.1 Build Groups Table


```python
all_users  = build_user_lookup(gis)
all_groups = gis.groups.search(query="*", max_groups=10000) or []

groups_to_analyze = all_groups[:GROUP_LIMIT] if TEST_MODE else all_groups

print(f"Groups found     : {len(all_groups):,}")
print(f"Groups analyzing : {len(groups_to_analyze):,}")

df_group_snapshot = build_group_snapshot(
    groups_to_analyze,
    all_users,
    recent_days_threshold=RECENT_DAYS_THRESHOLD,
)

display(df_group_snapshot.head())
```

    2026-07-01 12:09:16,639 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:16,776 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:17,331 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:17,459 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:17,601 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:17,736 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:17,898 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:18,091 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:18,323 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:18,562 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:18,804 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:18,949 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:19,110 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:19,249 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:19,401 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:19,540 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:19,693 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:19,843 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:20,343 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:20,757 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:20,897 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:21,254 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:21,485 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:21,708 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:22,065 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:22,206 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:22,422 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:22,660 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:22,802 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:23,103 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:23,523 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:23,704 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:23,843 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:24,000 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:24,164 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:24,305 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:24,634 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:24,788 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:25,125 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:25,279 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:25,426 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:25,573 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:25,781 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:26,105 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:26,565 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:26,718 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:26,981 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:27,386 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:27,651 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:27,845 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:27,981 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:28,135 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:28,401 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:28,543 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:28,687 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:28,829 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:28,971 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:29,119 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:29,267 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:29,531 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:29,996 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:30,249 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:30,477 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:30,616 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:30,840 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:30,973 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:31,143 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:31,350 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:31,505 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:31,806 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:32,204 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:32,349 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:32,498 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:32,633 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:33,000 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:33,330 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:33,471 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:33,728 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:34,165 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:34,306 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:34,462 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:34,607 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:34,758 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:35,095 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:35,493 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:35,775 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:36,062 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:36,242 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:36,379 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:36,631 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:36,789 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:36,933 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:37,079 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:37,226 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:37,375 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:37,637 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:37,799 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:37,932 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,097 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,236 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,389 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,561 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,716 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,856 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:38,996 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:39,169 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:39,392 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:39,529 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:39,679 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:40,028 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:40,180 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:40,349 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:40,495 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:40,642 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:40,851 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:41,004 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:41,203 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:41,341 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:41,472 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:41,719 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:41,953 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:42,100 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:42,235 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:42,469 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:42,703 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:42,951 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:43,114 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:43,268 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:43,499 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:43,654 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:43,804 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:44,062 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:44,323 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:44,483 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:44,640 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:44,907 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:45,051 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:45,276 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:45,509 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:45,680 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:45,828 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:45,980 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:46,337 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:46,489 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:46,632 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:46,800 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:46,957 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:47,109 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:47,358 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:47,498 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:47,637 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:47,775 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:47,917 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:48,083 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:48,235 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:48,728 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:48,886 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:49,036 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:49,399 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:49,543 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:49,688 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:49,836 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:49,986 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:50,250 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:50,539 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:50,704 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:51,052 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:51,492 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:51,641 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:51,806 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:51,965 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:52,220 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:52,716 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:52,954 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:53,084 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:53,240 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:53,371 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:53,625 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:53,901 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:54,174 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:54,316 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:54,467 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:54,612 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:54,755 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:54,901 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,050 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,203 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,356 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,521 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,670 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,833 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:55,977 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:56,118 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:56,264 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:56,401 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:56,607 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:56,746 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:57,246 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:57,404 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:57,560 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:57,801 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:57,975 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:58,113 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:58,355 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:58,489 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:58,739 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:58,957 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:59,207 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:59,345 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:59,480 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:59,620 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:09:59,682 - INFO - Searching groups (q=* accountid:lvPBAGXeSupVUvx2, start=1, num=100)


    2026-07-01 12:09:59,880 - INFO - Searching items (q=group:eca6ceb3c1804fc6bfe726b7168705ff, bbox=None, start=1, num=100)


    Groups found     : 70
    Groups analyzing : 70
    Analyzing 70 groups...


    2026-07-01 12:10:00,195 - INFO - Searching items (q=group:22d64941839d4e16a1d0ee181978c140, bbox=None, start=1, num=100)


    2026-07-01 12:10:00,481 - INFO - Searching items (q=group:b8c1a16bc2b54822be8ca9539c444a07, bbox=None, start=1, num=100)


    2026-07-01 12:10:01,185 - INFO - Searching items (q=group:81cd49e96cae4b1cb6ad2a499e5bc40a, bbox=None, start=1, num=100)


    2026-07-01 12:10:03,001 - INFO - Searching items (q=group:210065dc46784a9f880fc0ff8fae3f5e, bbox=None, start=1, num=100)


    2026-07-01 12:10:03,681 - INFO - Searching items (q=group:210065dc46784a9f880fc0ff8fae3f5e, bbox=None, start=101, num=100)


    2026-07-01 12:10:04,927 - INFO - Searching items (q=group:210065dc46784a9f880fc0ff8fae3f5e, bbox=None, start=201, num=100)


    2026-07-01 12:10:05,090 - INFO - Searching items (q=group:210065dc46784a9f880fc0ff8fae3f5e, bbox=None, start=301, num=100)


    2026-07-01 12:10:05,197 - INFO - Searching items (q=group:210065dc46784a9f880fc0ff8fae3f5e, bbox=None, start=401, num=100)


    2026-07-01 12:10:05,406 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:10:05,645 - INFO - Searching items (q=group:bb68e509e7ca4ac1a97369b0ff768d59, bbox=None, start=1, num=100)


    2026-07-01 12:10:06,023 - INFO - Searching items (q=group:db429aa3cfbf4e29aaf2767195f0a57b, bbox=None, start=1, num=100)


    2026-07-01 12:10:06,260 - INFO - Searching items (q=group:db429aa3cfbf4e29aaf2767195f0a57b, bbox=None, start=101, num=100)


    2026-07-01 12:10:06,676 - INFO - Searching items (q=group:aa51241663b94155bdd31a25322c8db9, bbox=None, start=1, num=100)


    2026-07-01 12:10:07,044 - INFO - Searching items (q=group:c1931d90aa3a41559db26a40d269ef1e, bbox=None, start=1, num=100)


    2026-07-01 12:10:07,350 - INFO - Searching items (q=group:8127c337f00741ce8543d278adef33f8, bbox=None, start=1, num=100)


    2026-07-01 12:10:07,760 - INFO - Searching items (q=group:6f6804c2dd994e11a674060ac2cd1734, bbox=None, start=1, num=100)


    2026-07-01 12:10:08,401 - INFO - Searching items (q=group:c5b34f1f13a44651a7b6717016b1a9e0, bbox=None, start=1, num=100)


    2026-07-01 12:10:08,831 - INFO - Searching items (q=group:a0496e9f4aa34c14ae681d224509d7d9, bbox=None, start=1, num=100)


    2026-07-01 12:10:09,310 - INFO - Searching items (q=group:b89134501d0b4879884b01da498fe8e6, bbox=None, start=1, num=100)


    2026-07-01 12:10:09,679 - INFO - Searching items (q=group:be7fd329a50a42eca7c6cf76900e2719, bbox=None, start=1, num=100)


    2026-07-01 12:10:10,034 - INFO - Searching items (q=group:ee48ffe579c845088dc8f4347e5439e8, bbox=None, start=1, num=100)


    2026-07-01 12:10:10,353 - INFO - Searching items (q=group:f9d040905f894c1c9f274f7e93091a47, bbox=None, start=1, num=100)


    2026-07-01 12:10:10,723 - INFO - Searching items (q=group:397791e8c9114fd7a1c76485d491e136, bbox=None, start=1, num=100)


    2026-07-01 12:10:11,039 - INFO - Searching items (q=group:71ea9c6d1a7045a4b381301cef580bea, bbox=None, start=1, num=100)


    2026-07-01 12:10:11,345 - INFO - Searching items (q=group:1cdb2b36db1c4250bc93437847b7c93b, bbox=None, start=1, num=100)


    2026-07-01 12:10:11,696 - INFO - Searching items (q=group:b7130ef8428a473b91ea033839a05d27, bbox=None, start=1, num=100)


    2026-07-01 12:10:12,137 - INFO - Searching items (q=group:1c8bd2b6be584077b40f431aa2d9f579, bbox=None, start=1, num=100)


    2026-07-01 12:10:12,588 - INFO - Searching items (q=group:e16725a7402347e8a16191b4ac42d869, bbox=None, start=1, num=100)


    2026-07-01 12:10:13,077 - INFO - Searching items (q=group:737a820808ce457dbc86710ac69707c2, bbox=None, start=1, num=100)


    2026-07-01 12:10:13,360 - INFO - Searching items (q=group:6c77495d776e4110ab0e749c13dcc70d, bbox=None, start=1, num=100)


    2026-07-01 12:10:13,723 - INFO - Searching items (q=group:a8e4b97ce0674dea84058c2f49d4d297, bbox=None, start=1, num=100)


    2026-07-01 12:10:14,111 - INFO - Searching items (q=group:7fd74f262e4947eea065a89ed43ff8da, bbox=None, start=1, num=100)


    2026-07-01 12:10:14,463 - INFO - Searching items (q=group:b90f7fd2f2c14fdabeea9f0ffde30077, bbox=None, start=1, num=100)


    2026-07-01 12:10:14,769 - INFO - Searching items (q=group:8a2a106c2772427db6b95e4f795a8b88, bbox=None, start=1, num=100)


    2026-07-01 12:10:15,177 - INFO - Searching items (q=group:fc2cf779ab814a5a8ccf0fc3a349686e, bbox=None, start=1, num=100)


    2026-07-01 12:10:15,533 - INFO - Searching items (q=group:0e1f71113ad8482384d56118cb731f33, bbox=None, start=1, num=100)


    2026-07-01 12:10:15,937 - INFO - Searching items (q=group:e200013518ff4c8c9be3753743a2808e, bbox=None, start=1, num=100)


    2026-07-01 12:10:16,330 - INFO - Searching items (q=group:b13cd245a3e040c1893efe98d4f6173c, bbox=None, start=1, num=100)


    2026-07-01 12:10:16,726 - INFO - Searching items (q=group:8658a8a71c6e43b3a29667cb2a5f2a9b, bbox=None, start=1, num=100)


    2026-07-01 12:10:17,101 - INFO - Searching items (q=group:86fc052f386a43d7bfaf8a1acb705629, bbox=None, start=1, num=100)


    2026-07-01 12:10:17,523 - INFO - Searching items (q=group:f69359a45a1942788ce44bf4b121c08e, bbox=None, start=1, num=100)


    2026-07-01 12:10:17,949 - INFO - Searching items (q=group:55ad52680fb54a61a066e11db650d562, bbox=None, start=1, num=100)


    2026-07-01 12:10:18,329 - INFO - Searching items (q=group:906035e0fb484e809351eadb46ce28df, bbox=None, start=1, num=100)


    2026-07-01 12:10:18,699 - INFO - Searching items (q=group:940a9ec344f14e8c99a2aed806a880e0, bbox=None, start=1, num=100)


    2026-07-01 12:10:19,141 - INFO - Searching items (q=group:439358664c7b4c50bcaa43e93df1d351, bbox=None, start=1, num=100)


    2026-07-01 12:10:19,496 - INFO - Searching items (q=group:4b60b042bbe04e7eab5333f7276e49ef, bbox=None, start=1, num=100)


    2026-07-01 12:10:19,868 - INFO - Searching items (q=group:b18bba60036840f1b93fd04d931fece0, bbox=None, start=1, num=100)


    2026-07-01 12:10:20,230 - INFO - Searching items (q=group:4f226868e6b34283ad4e29073e4886cb, bbox=None, start=1, num=100)


    2026-07-01 12:10:20,580 - INFO - Searching items (q=group:68279e97ae954edc9c8497be2dc15149, bbox=None, start=1, num=100)


    2026-07-01 12:10:20,939 - INFO - Searching items (q=group:d450999133024d1593c9ba4438916168, bbox=None, start=1, num=100)


    2026-07-01 12:10:21,317 - INFO - Searching items (q=group:14e4dfa86ffe47a3ad5340e8b9c51d9e, bbox=None, start=1, num=100)


    2026-07-01 12:10:21,692 - INFO - Searching items (q=group:bbdd4b6c21594ed6856ad0c22b73eeff, bbox=None, start=1, num=100)


    2026-07-01 12:10:22,127 - INFO - Searching items (q=group:4d13e8792fbf478db3651db78019b622, bbox=None, start=1, num=100)


    2026-07-01 12:10:22,398 - INFO - Searching items (q=group:5341638febd749b6b68877f187815599, bbox=None, start=1, num=100)


    2026-07-01 12:10:22,889 - INFO - Searching items (q=group:703f5c3679d34263a469a3d950188a1d, bbox=None, start=1, num=100)


    2026-07-01 12:10:23,250 - INFO - Searching items (q=group:bbb09f8a420d45e3ab436dca7036c4d1, bbox=None, start=1, num=100)


      Progress: 50/70 (71.4%) | elapsed 23.4s | avg 0.467s/item | ETA 0.2 min


    2026-07-01 12:10:24,034 - INFO - Searching items (q=group:60b1b53ea74b4e07b4654cd4d0e81a23, bbox=None, start=1, num=100)


    2026-07-01 12:10:24,414 - INFO - Searching items (q=group:3a5d8fe865c34305b7e9fb9f9450dd35, bbox=None, start=1, num=100)


    2026-07-01 12:10:24,908 - INFO - Searching items (q=group:bbf12754e331496f98067d5fdb0a24f0, bbox=None, start=1, num=100)


    2026-07-01 12:10:25,280 - INFO - Searching items (q=group:53450eed4e384a798074184c07b2ada0, bbox=None, start=1, num=100)


    2026-07-01 12:10:25,657 - INFO - Searching items (q=group:b5d6e159d491414b93e3a29fcd7a02f8, bbox=None, start=1, num=100)


    2026-07-01 12:10:26,118 - INFO - Searching items (q=group:679309d9cf42408d86ab2d2af89c369a, bbox=None, start=1, num=100)


    2026-07-01 12:10:26,503 - INFO - Searching items (q=group:24228e9596cb40d69e2e13dcda5c3b7a, bbox=None, start=1, num=100)


    2026-07-01 12:10:26,991 - INFO - Searching items (q=group:ba772b5f4ff64b12b3af39a3ecb466d4, bbox=None, start=1, num=100)


    2026-07-01 12:10:27,352 - INFO - Searching items (q=group:97315c93f73e4c7ab3e57a439c964c83, bbox=None, start=1, num=100)


    2026-07-01 12:10:27,725 - INFO - Searching items (q=group:0a31c5c020004cb39eb71d5aca9cd514, bbox=None, start=1, num=100)


    2026-07-01 12:10:28,167 - INFO - Searching items (q=group:012e575bb03f422f90c3066e590a1b2a, bbox=None, start=1, num=100)


    2026-07-01 12:10:28,474 - INFO - Searching items (q=group:dad50cbeb44343dfa35fa3a3eb1f3ce0, bbox=None, start=1, num=100)


    2026-07-01 12:10:28,814 - INFO - Searching items (q=group:45618dbadd454bc5ba026bfb90ad87ce, bbox=None, start=1, num=100)


    2026-07-01 12:10:29,759 - INFO - Searching items (q=group:e7223136a375404d9e18c09c49600072, bbox=None, start=1, num=100)


    2026-07-01 12:10:30,405 - INFO - Searching items (q=group:f186b31574b94f6e8da1035495dfa769, bbox=None, start=1, num=100)


    2026-07-01 12:10:30,796 - INFO - Searching items (q=group:a183e82a059447d590612aef13087420, bbox=None, start=1, num=100)


    2026-07-01 12:10:31,213 - INFO - Searching items (q=group:7959693e4c0944ce9799adefec325a02, bbox=None, start=1, num=100)


    2026-07-01 12:10:31,566 - INFO - Searching items (q=group:5650e64112764e2e91546852678102a4, bbox=None, start=1, num=100)


    2026-07-01 12:10:31,945 - INFO - Searching items (q=group:ab9f87368c6d4457bfa883730a69ad02, bbox=None, start=1, num=100)


      Progress: 70/70 (100.0%) | elapsed 32.5s | avg 0.464s/item | ETA 0.0 min



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>group_id</th>
      <th>group_title</th>
      <th>group_summary</th>
      <th>group_description</th>
      <th>group_tags</th>
      <th>group_type</th>
      <th>owner_username</th>
      <th>owner_name</th>
      <th>owner_email</th>
      <th>creation_date</th>
      <th>sharing_lvl</th>
      <th>item_count</th>
      <th>member_count</th>
      <th>active_members</th>
      <th>avg_views_per_item</th>
      <th>days_since_content_update</th>
      <th>group_url</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>eca6ceb3c1804fc6bfe726b7168705ff</td>
      <td>2024 Division TDF ORR</td>
      <td></td>
      <td></td>
      <td></td>
      <td>Standard</td>
      <td>Chris.Carney_TNDOF8</td>
      <td>Chris Carney</td>
      <td>Chris.Carney@tn.gov</td>
      <td>2024-09-03 21:04:04+00:00</td>
      <td>Private</td>
      <td>10</td>
      <td>16</td>
      <td>16</td>
      <td>408.80</td>
      <td>116</td>
      <td>https://www.arcgis.com/home/group.html?id=eca6...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>22d64941839d4e16a1d0ee181978c140</td>
      <td>2025 ORNL OPS</td>
      <td></td>
      <td></td>
      <td></td>
      <td>Standard</td>
      <td>Chris.Carney_TNDOF8</td>
      <td>Chris Carney</td>
      <td>Chris.Carney@tn.gov</td>
      <td>2025-09-07 17:52:40+00:00</td>
      <td>Private</td>
      <td>7</td>
      <td>38</td>
      <td>38</td>
      <td>678.57</td>
      <td>116</td>
      <td>https://www.arcgis.com/home/group.html?id=22d6...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>b8c1a16bc2b54822be8ca9539c444a07</td>
      <td>AGOL Admins</td>
      <td>This group includes TDF leadership positions t...</td>
      <td></td>
      <td>Tennessee Department of Agriculture, Tennessee...</td>
      <td>Standard</td>
      <td>colin.stiles_tndof</td>
      <td>Colin Stiles</td>
      <td>colin.stiles@tn.gov</td>
      <td>2021-07-12 15:19:05+00:00</td>
      <td>Organization</td>
      <td>61</td>
      <td>10</td>
      <td>10</td>
      <td>2609.15</td>
      <td>1</td>
      <td>https://www.arcgis.com/home/group.html?id=b8c1...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>81cd49e96cae4b1cb6ad2a499e5bc40a</td>
      <td>Area 502</td>
      <td></td>
      <td></td>
      <td></td>
      <td>Standard</td>
      <td>Chris.Carney_TNDOF8</td>
      <td>Chris Carney</td>
      <td>Chris.Carney@tn.gov</td>
      <td>2024-09-11 16:20:08+00:00</td>
      <td>Private</td>
      <td>3</td>
      <td>4</td>
      <td>4</td>
      <td>82.67</td>
      <td>670</td>
      <td>https://www.arcgis.com/home/group.html?id=81cd...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>210065dc46784a9f880fc0ff8fae3f5e</td>
      <td>Area 508</td>
      <td>Counties covered by 508 area forester (Hickman...</td>
      <td></td>
      <td>LSF, perry, lewis, hickman, lewis state forest</td>
      <td>Standard</td>
      <td>naomi.handley_TNDOF</td>
      <td>Naomi Handley</td>
      <td>naomi.handley@tn.gov</td>
      <td>2023-07-20 17:16:27+00:00</td>
      <td>Organization</td>
      <td>410</td>
      <td>10</td>
      <td>10</td>
      <td>117.82</td>
      <td>35</td>
      <td>https://www.arcgis.com/home/group.html?id=2100...</td>
    </tr>
  </tbody>
</table>
</div>


### Step 3b.2 Publish Groups Table


```python
# Retrieve table reference
groups_table, groups_sublayer, groups_url = get_table_url(agk_fs, "Groups", gis=gis)
print(f"Groups table found (sublayer={groups_sublayer}): {groups_url}")

# Clear existing records
if delete_content:
    clear_table(groups_table)

# Publish
publish_dataframe_to_table(df_group_snapshot, groups_table)
print("\n✓ Groups snapshot complete")
```

    Truncate successful.
    {'success': True}
    
    Publishing to table: Groups
    Total records to publish : 70
    Chunk size               : 500
    
    Publishing rows 1–70...


      Attempted : 70
      Succeeded : 70
      Failed    : 0
      Progress: 1/1 (100.0%) | elapsed 0.4s | avg 0.358s/chunk | ETA 0.0 min
    
    Groups — Publish complete.
      Total attempted : 70
      Total succeeded : 70
      Total failed    : 0
      Status: SUCCESS
    
    ✓ Groups snapshot complete


## Step 3c: Content

### Step 3c.1 Build Content Table


```python
start_time = time.perf_counter()

# Inventory all org items
items_dicts = inventory_org_items(
    gis,
    additional_query="*",
    test_cap=ITEM_CAP,
)

# Build governance table
content_df = run_governance_pipeline(gis, items_dicts)

print(f"\n{len(content_df):,} total items returned")
display(content_df.head())

print(f"Execution time: {time.perf_counter() - start_time:.2f}s")
```

    2026-07-01 12:11:10,053 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:10,148 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:10,189 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:10,225 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:10,239 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:10,270 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:10,390 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 500/2569 items (19.5%) in 26.9s [chunk took 5.7s]


    2026-07-01 12:11:14,381 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,387 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,474 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,578 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,625 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:14,653 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,692 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,697 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,749 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,764 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:14,884 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 600/2569 items (23.4%) in 31.4s [chunk took 4.5s]


    2026-07-01 12:11:15,647 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:15,914 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:16,427 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:16,479 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:16,649 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:18,369 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:18,495 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,499 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,520 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,604 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,642 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,666 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,747 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,758 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:18,967 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:19,253 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 700/2569 items (27.2%) in 36.7s [chunk took 5.2s]


    2026-07-01 12:11:20,351 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,217 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,219 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,223 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,300 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,398 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,536 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,593 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,606 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,725 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,762 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,787 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,798 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,848 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,861 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,877 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,886 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,902 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,921 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,973 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:21,993 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:22,044 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:22,177 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:22,203 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:22,227 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:22,469 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:22,545 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,318 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,407 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,433 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,568 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,595 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,710 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,720 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,799 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,845 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,859 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,874 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,920 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,931 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,949 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:23,993 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,009 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:24,019 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:24,095 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,153 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,178 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,226 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:24,227 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,261 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:24,353 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,358 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,459 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,589 - INFO - Retrieving roles(start=1, num=100)


    ✓ Processed 800/2569 items (31.1%) in 41.2s [chunk took 4.5s]


    2026-07-01 12:11:24,946 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,954 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,955 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:24,956 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,252 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,253 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,254 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,348 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,354 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,360 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,627 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,629 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,697 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,701 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,702 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,801 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,937 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,974 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:25,978 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,000 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,040 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,111 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,141 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,171 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,172 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,232 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:26,266 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:27,921 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,435 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,503 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:28,504 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,541 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,614 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,673 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,691 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,772 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,797 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,807 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,809 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,826 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:28,859 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:28,867 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:28,868 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:28,885 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:29,106 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:29,344 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 900/2569 items (35.0%) in 46.0s [chunk took 4.8s]


    2026-07-01 12:11:29,748 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:29,749 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:29,749 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:29,749 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,047 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,054 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,151 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,155 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,389 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,455 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,815 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,843 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,889 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:30,966 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:31,036 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:31,085 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:31,123 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:33,080 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,114 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,236 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,257 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,274 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,274 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,446 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,593 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:33,746 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1000/2569 items (38.9%) in 50.3s [chunk took 4.3s]


    2026-07-01 12:11:35,848 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:37,700 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:37,717 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:38,142 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:38,157 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:38,184 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:38,248 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:38,289 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1100/2569 items (42.8%) in 54.8s [chunk took 4.5s]


    2026-07-01 12:11:41,849 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:41,887 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,016 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,037 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,080 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,297 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,319 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,430 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:42,545 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1200/2569 items (46.7%) in 59.7s [chunk took 4.9s]


    2026-07-01 12:11:45,957 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:46,473 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,499 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,501 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,681 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,690 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,741 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,755 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:46,941 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:47,042 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1300/2569 items (50.6%) in 63.6s [chunk took 3.9s]


    2026-07-01 12:11:48,668 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:48,727 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:49,120 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,138 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,211 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,273 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,334 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,432 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:49,587 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,595 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:49,772 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1400/2569 items (54.5%) in 66.3s [chunk took 2.7s]


    2026-07-01 12:11:50,587 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:50,687 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:50,691 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:50,731 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:50,787 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:50,810 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:50,876 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:51,000 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:51,121 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:51,149 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:51,180 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:51,285 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:52,055 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:53,203 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:11:53,291 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,293 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,361 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,407 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,419 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,422 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,431 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,431 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:53,690 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1500/2569 items (58.4%) in 70.4s [chunk took 4.1s]


    2026-07-01 12:11:56,830 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:56,956 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:56,962 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:56,977 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:57,078 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 9


    2026-07-01 12:11:57,099 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:57,116 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:57,199 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:57,203 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:11:57,339 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1600/2569 items (62.3%) in 74.0s [chunk took 3.6s]


    2026-07-01 12:12:00,805 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:00,822 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:00,827 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:01,327 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:01,855 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:01,947 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1700/2569 items (66.2%) in 78.5s [chunk took 4.5s]


    2026-07-01 12:12:02,978 - WARNING - Retrying (Retry(total=1, connect=1, read=5, redirect=None, status=None)) after connection broken by 'NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x7c8052604a50>: Failed to resolve 'di-usfsdata.img.arcgis.com' ([Errno -2] Name or service not known)")': /arcgis/rest/services/Carbon_Predominant_Forest_Biomass_202504282001285/ImageServer


    2026-07-01 12:12:03,163 - WARNING - Retrying (Retry(total=1, connect=1, read=5, redirect=None, status=None)) after connection broken by 'NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x7c8052605bd0>: Failed to resolve 'di-usfsdata.img.arcgis.com' ([Errno -2] Name or service not known)")': /arcgis/rest/services/Carbon_Predominant_Forest_Biomass_202504282001285/ImageServer


    2026-07-01 12:12:03,404 - WARNING - Retrying (Retry(total=0, connect=0, read=5, redirect=None, status=None)) after connection broken by 'NameResolutionError("<urllib3.connection.HTTPSConnection object at 0x7c80526065d0>: Failed to resolve 'di-usfsdata.img.arcgis.com' ([Errno -2] Name or service not known)")': /arcgis/rest/services/Carbon_Predominant_Forest_Biomass_202504282001285/ImageServer


    2026-07-01 12:12:07,893 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,034 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,120 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,297 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,392 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,394 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,418 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:08,659 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1800/2569 items (70.1%) in 89.1s [chunk took 10.6s]


    2026-07-01 12:12:16,098 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,100 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,105 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,112 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,128 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,142 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,161 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,208 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,457 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:16,783 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 1900/2569 items (74.0%) in 93.6s [chunk took 4.5s]


    2026-07-01 12:12:19,507 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:19,780 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:19,822 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:19,823 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:19,829 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:19,867 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:19,873 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:19,875 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 9


    2026-07-01 12:12:19,889 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:20,011 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:20,252 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:20,380 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 2000/2569 items (77.9%) in 97.2s [chunk took 3.6s]


    2026-07-01 12:12:21,351 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,333 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,336 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,492 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,517 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,533 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,550 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,588 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,617 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,625 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,631 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,656 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,727 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,733 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,739 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,741 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,765 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,790 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,797 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,801 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,805 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,817 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,824 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,830 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,877 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,878 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,901 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,904 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,909 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,948 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,996 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:22,997 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,004 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,011 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,078 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,080 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,082 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,102 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,105 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,147 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,164 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,173 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,183 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,198 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,211 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,241 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,267 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,273 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,277 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,292 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,295 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,359 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,362 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,363 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:23,367 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,369 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,383 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,504 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,563 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,620 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:23,679 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:23,892 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:23,966 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:24,061 - INFO - Retrieving roles(start=1, num=100)


    ✓ Processed 2100/2569 items (81.7%) in 100.7s [chunk took 3.5s]


    2026-07-01 12:12:24,448 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:24,546 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:24,748 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:24,862 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:24,947 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:24,950 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:24,951 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,000 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,163 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,168 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,178 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,229 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,250 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,262 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,331 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,446 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,470 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,553 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,574 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,584 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,595 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,643 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,746 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,784 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,806 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,829 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,830 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,868 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,870 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,876 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,878 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,879 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,911 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,940 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:25,947 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,016 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,039 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,070 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,096 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,137 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,174 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,183 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,197 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,213 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,240 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,283 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,307 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,398 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,461 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,464 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,522 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,524 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,536 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,559 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,567 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,609 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,665 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,673 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,736 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,742 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,798 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,800 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,828 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,844 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,930 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,945 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,980 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,992 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:26,994 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,007 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,019 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,022 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,059 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,115 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,116 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,226 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,263 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,271 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,276 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,313 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,374 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,410 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,439 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,440 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,479 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,489 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,511 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,593 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,627 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,679 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,695 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,696 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,782 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,783 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:27,786 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,860 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:27,862 - INFO - Retrieving roles(start=1, num=100)


    ✓ Processed 2200/2569 items (85.6%) in 104.5s [chunk took 3.8s]


    2026-07-01 12:12:28,254 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,255 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,256 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,260 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,260 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,261 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,557 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,557 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,558 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,559 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,848 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,850 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,930 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,983 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:28,993 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,011 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,016 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,028 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,030 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,080 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,083 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,127 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,134 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,247 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,254 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,331 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,334 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,358 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,364 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,367 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,387 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,407 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,459 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,475 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,481 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,573 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,635 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,637 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,650 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,715 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,739 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,755 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,811 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,851 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,882 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,921 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,979 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:29,993 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,015 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,085 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,096 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,108 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,121 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,190 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,212 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,273 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,299 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,317 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,345 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,374 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,409 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,467 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,498 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,503 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,557 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,616 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,621 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,628 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,702 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,706 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,749 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,754 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,783 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,783 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,807 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:30,835 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,023 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,053 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,090 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,103 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,130 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,155 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,201 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,220 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,236 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,238 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,259 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,306 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,310 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,314 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,353 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,383 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,384 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,393 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,407 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,417 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,425 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,452 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,470 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,479 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,563 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,656 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,689 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,692 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,748 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,749 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:31,754 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:31,815 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 2300/2569 items (89.5%) in 108.4s [chunk took 3.9s]


    2026-07-01 12:12:32,346 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,351 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,363 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,458 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,549 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,551 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,555 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,561 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,653 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,658 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,877 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:32,884 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,090 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,110 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,113 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,149 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,156 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,206 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,208 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,246 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,286 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,330 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,397 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,403 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,451 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,485 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,500 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,567 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,584 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,775 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,777 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,845 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,867 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,909 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,925 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,934 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,969 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:33,975 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,060 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,083 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,100 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,108 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,129 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,155 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,162 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,170 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,290 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,339 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,354 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,377 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,444 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,446 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,447 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,476 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,490 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,507 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,562 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,564 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,585 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,607 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,619 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,632 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,662 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,677 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,816 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,826 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,877 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,890 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:34,914 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,941 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:34,941 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:34,944 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:34,945 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:34,988 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:34,989 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:35,059 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:35,464 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 2400/2569 items (93.4%) in 112.0s [chunk took 3.6s]


    2026-07-01 12:12:36,409 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:37,934 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:37,981 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:38,074 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:38,706 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 2500/2569 items (97.3%) in 116.1s [chunk took 4.1s]


    2026-07-01 12:12:40,865 - INFO - Retrieving roles(start=1, num=100)


    2026-07-01 12:12:41,411 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,441 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,472 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,502 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,662 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,671 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,679 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,850 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    2026-07-01 12:12:41,999 - WARNING - Connection pool is full, discarding connection: www.arcgis.com. Connection pool size: 10


    ✓ Processed 2569/2569 items (100.0%) in 118.6s [chunk took 2.4s]
    ✓ Item size retrieval complete
    ✓ Governance dataset ready
    
    2,569 total items returned



<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>item_id</th>
      <th>title</th>
      <th>owner_username</th>
      <th>owner_name</th>
      <th>owner_email</th>
      <th>item_type</th>
      <th>tags</th>
      <th>summary</th>
      <th>description</th>
      <th>view_count</th>
      <th>creation_date</th>
      <th>last_updated</th>
      <th>last_viewed</th>
      <th>sharing_lvl</th>
      <th>metadata_completeness</th>
      <th>days_since_update</th>
      <th>content_status</th>
      <th>item_page</th>
      <th>size_mb</th>
      <th>attachments_size_mb</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>c908219d8bc44b1ebe748aa458d3279e</td>
      <td>Tracts</td>
      <td>colin.stiles_tndof</td>
      <td>Colin Stiles</td>
      <td>colin.stiles@tn.gov</td>
      <td>Service Definition</td>
      <td>Stewardship, forest management, ArcGIS, Servic...</td>
      <td></td>
      <td></td>
      <td>3</td>
      <td>10/07/2015 19:27</td>
      <td>01/26/2026 15:26</td>
      <td>12/31/1969 23:59</td>
      <td>Organization</td>
      <td>40</td>
      <td>155</td>
      <td>None</td>
      <td>https://www.arcgis.com/home/item.html?id=c9082...</td>
      <td>5.44</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>99cb27600e1547a28b8f6d3a2513b33d</td>
      <td>Tracts</td>
      <td>colin.stiles_tndof</td>
      <td>Colin Stiles</td>
      <td>colin.stiles@tn.gov</td>
      <td>Feature Service</td>
      <td>Stewardship, forest management, ArcGIS, Servic...</td>
      <td></td>
      <td></td>
      <td>292</td>
      <td>10/07/2015 19:27</td>
      <td>01/26/2026 15:28</td>
      <td>06/22/2026 17:00</td>
      <td>Organization</td>
      <td>40</td>
      <td>155</td>
      <td>None</td>
      <td>https://www.arcgis.com/home/item.html?id=99cb2...</td>
      <td>32.90</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>60bffe37a3c545769f3adb650ce8dc89</td>
      <td>StewardshipStands</td>
      <td>colin.stiles_tndof</td>
      <td>Colin Stiles</td>
      <td>colin.stiles@tn.gov</td>
      <td>Service Definition</td>
      <td>Forest Stewardship, stands, land use areas, Ar...</td>
      <td></td>
      <td>Forest Stewardship stand polygosn</td>
      <td>2</td>
      <td>05/11/2016 17:09</td>
      <td>01/26/2026 15:22</td>
      <td>12/31/1969 23:59</td>
      <td>Private</td>
      <td>83</td>
      <td>155</td>
      <td>None</td>
      <td>https://www.arcgis.com/home/item.html?id=60bff...</td>
      <td>5.53</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>0e7c981d883f490da0a973361794a8b7</td>
      <td>StewardshipStands</td>
      <td>colin.stiles_tndof</td>
      <td>Colin Stiles</td>
      <td>colin.stiles@tn.gov</td>
      <td>Feature Service</td>
      <td>Forest Stewardship, stands, land use areas, Ar...</td>
      <td></td>
      <td>Forest Stewardship stand polygosn</td>
      <td>80</td>
      <td>05/11/2016 17:09</td>
      <td>01/26/2026 15:24</td>
      <td>06/01/2026 12:00</td>
      <td>Organization</td>
      <td>83</td>
      <td>155</td>
      <td>None</td>
      <td>https://www.arcgis.com/home/item.html?id=0e7c9...</td>
      <td>33.12</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>91514904866b4b4093dd4048c62c33ba</td>
      <td>Priority_Fires_11_18_16</td>
      <td>colin.stiles_tndof</td>
      <td>Colin Stiles</td>
      <td>colin.stiles@tn.gov</td>
      <td>Service Definition</td>
      <td>Fire Mapping,  Tennessee Fires</td>
      <td></td>
      <td>Flight plan for Lakota</td>
      <td>1</td>
      <td>11/18/2016 11:13</td>
      <td>01/26/2026 15:26</td>
      <td>12/31/1969 23:59</td>
      <td>Private</td>
      <td>53</td>
      <td>155</td>
      <td>None</td>
      <td>https://www.arcgis.com/home/item.html?id=91514...</td>
      <td>0.06</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>


    Execution time: 127.62s


### Step 3c.2 Build Dependency Table


```python
upstream_enriched, downstream_enriched, final_content_df = run_dependency_pipeline(
    gis=gis,
    items_dicts=items_dicts,
    content_df=content_df,
    dependency_mapping=dependency_mapping
)
```

      Progress: 60/2,569 (2.3%) | elapsed 49.2s | avg 0.819s/item | ETA 34.3 min


      Progress: 70/2,569 (2.7%) | elapsed 61.4s | avg 0.876s/item | ETA 36.5 min


      Progress: 80/2,569 (3.1%) | elapsed 71.4s | avg 0.893s/item | ETA 37.0 min


      Progress: 90/2,569 (3.5%) | elapsed 87.1s | avg 0.968s/item | ETA 40.0 min


      Progress: 100/2,569 (3.9%) | elapsed 92.4s | avg 0.924s/item | ETA 38.0 min


      Progress: 110/2,569 (4.3%) | elapsed 102.4s | avg 0.931s/item | ETA 38.2 min


      Progress: 120/2,569 (4.7%) | elapsed 108.8s | avg 0.907s/item | ETA 37.0 min


      Progress: 130/2,569 (5.1%) | elapsed 121.2s | avg 0.933s/item | ETA 37.9 min


      Progress: 140/2,569 (5.4%) | elapsed 130.9s | avg 0.935s/item | ETA 37.8 min


      Progress: 150/2,569 (5.8%) | elapsed 137.1s | avg 0.914s/item | ETA 36.9 min


      Progress: 160/2,569 (6.2%) | elapsed 143.9s | avg 0.899s/item | ETA 36.1 min


      Progress: 170/2,569 (6.6%) | elapsed 149.2s | avg 0.878s/item | ETA 35.1 min


      Progress: 180/2,569 (7.0%) | elapsed 155.0s | avg 0.861s/item | ETA 34.3 min


      Progress: 190/2,569 (7.4%) | elapsed 165.8s | avg 0.872s/item | ETA 34.6 min


      Progress: 200/2,569 (7.8%) | elapsed 172.8s | avg 0.864s/item | ETA 34.1 min


      Progress: 210/2,569 (8.2%) | elapsed 210.0s | avg 1.000s/item | ETA 39.3 min


      Progress: 220/2,569 (8.6%) | elapsed 219.6s | avg 0.998s/item | ETA 39.1 min


      Progress: 230/2,569 (9.0%) | elapsed 227.0s | avg 0.987s/item | ETA 38.5 min


      Progress: 240/2,569 (9.3%) | elapsed 230.7s | avg 0.961s/item | ETA 37.3 min


      Progress: 250/2,569 (9.7%) | elapsed 234.5s | avg 0.938s/item | ETA 36.3 min


      Progress: 260/2,569 (10.1%) | elapsed 241.9s | avg 0.931s/item | ETA 35.8 min


      Progress: 270/2,569 (10.5%) | elapsed 247.6s | avg 0.917s/item | ETA 35.1 min


      Progress: 280/2,569 (10.9%) | elapsed 251.0s | avg 0.897s/item | ETA 34.2 min


      Progress: 290/2,569 (11.3%) | elapsed 254.8s | avg 0.879s/item | ETA 33.4 min


      Progress: 300/2,569 (11.7%) | elapsed 258.2s | avg 0.861s/item | ETA 32.6 min


      Progress: 310/2,569 (12.1%) | elapsed 261.7s | avg 0.844s/item | ETA 31.8 min


      Progress: 320/2,569 (12.5%) | elapsed 265.2s | avg 0.829s/item | ETA 31.1 min


      Progress: 330/2,569 (12.8%) | elapsed 270.0s | avg 0.818s/item | ETA 30.5 min


      Progress: 340/2,569 (13.2%) | elapsed 279.6s | avg 0.822s/item | ETA 30.5 min


      Progress: 350/2,569 (13.6%) | elapsed 289.3s | avg 0.826s/item | ETA 30.6 min


      Progress: 360/2,569 (14.0%) | elapsed 296.9s | avg 0.825s/item | ETA 30.4 min


      Progress: 370/2,569 (14.4%) | elapsed 308.9s | avg 0.835s/item | ETA 30.6 min


      Progress: 380/2,569 (14.8%) | elapsed 317.0s | avg 0.834s/item | ETA 30.4 min


      Progress: 390/2,569 (15.2%) | elapsed 323.3s | avg 0.829s/item | ETA 30.1 min


      Progress: 400/2,569 (15.6%) | elapsed 329.4s | avg 0.823s/item | ETA 29.8 min


      Progress: 410/2,569 (16.0%) | elapsed 336.9s | avg 0.822s/item | ETA 29.6 min


      Progress: 420/2,569 (16.3%) | elapsed 342.7s | avg 0.816s/item | ETA 29.2 min


      Progress: 430/2,569 (16.7%) | elapsed 352.1s | avg 0.819s/item | ETA 29.2 min


      Progress: 440/2,569 (17.1%) | elapsed 358.5s | avg 0.815s/item | ETA 28.9 min


      Progress: 450/2,569 (17.5%) | elapsed 362.7s | avg 0.806s/item | ETA 28.5 min


      Progress: 460/2,569 (17.9%) | elapsed 370.3s | avg 0.805s/item | ETA 28.3 min


      Progress: 470/2,569 (18.3%) | elapsed 375.8s | avg 0.800s/item | ETA 28.0 min


      Progress: 480/2,569 (18.7%) | elapsed 382.5s | avg 0.797s/item | ETA 27.7 min


      Progress: 490/2,569 (19.1%) | elapsed 388.1s | avg 0.792s/item | ETA 27.4 min


      Progress: 500/2,569 (19.5%) | elapsed 396.1s | avg 0.792s/item | ETA 27.3 min


      Progress: 510/2,569 (19.9%) | elapsed 408.8s | avg 0.802s/item | ETA 27.5 min


      Progress: 520/2,569 (20.2%) | elapsed 414.9s | avg 0.798s/item | ETA 27.2 min


      Progress: 530/2,569 (20.6%) | elapsed 428.0s | avg 0.808s/item | ETA 27.4 min


      Progress: 540/2,569 (21.0%) | elapsed 434.5s | avg 0.805s/item | ETA 27.2 min


      Progress: 550/2,569 (21.4%) | elapsed 440.0s | avg 0.800s/item | ETA 26.9 min


      Progress: 560/2,569 (21.8%) | elapsed 445.3s | avg 0.795s/item | ETA 26.6 min


      Progress: 570/2,569 (22.2%) | elapsed 452.7s | avg 0.794s/item | ETA 26.5 min


      Progress: 580/2,569 (22.6%) | elapsed 456.9s | avg 0.788s/item | ETA 26.1 min


      Progress: 590/2,569 (23.0%) | elapsed 464.6s | avg 0.787s/item | ETA 26.0 min


      Progress: 600/2,569 (23.4%) | elapsed 473.3s | avg 0.789s/item | ETA 25.9 min


      Progress: 610/2,569 (23.7%) | elapsed 480.7s | avg 0.788s/item | ETA 25.7 min


      Progress: 620/2,569 (24.1%) | elapsed 487.2s | avg 0.786s/item | ETA 25.5 min


      Progress: 630/2,569 (24.5%) | elapsed 495.5s | avg 0.786s/item | ETA 25.4 min


      Progress: 640/2,569 (24.9%) | elapsed 501.4s | avg 0.783s/item | ETA 25.2 min


      Progress: 650/2,569 (25.3%) | elapsed 509.7s | avg 0.784s/item | ETA 25.1 min


      Progress: 660/2,569 (25.7%) | elapsed 518.4s | avg 0.785s/item | ETA 25.0 min


      Progress: 670/2,569 (26.1%) | elapsed 526.9s | avg 0.786s/item | ETA 24.9 min


      Progress: 680/2,569 (26.5%) | elapsed 532.4s | avg 0.783s/item | ETA 24.6 min


      Progress: 690/2,569 (26.9%) | elapsed 540.8s | avg 0.784s/item | ETA 24.5 min


      Progress: 700/2,569 (27.2%) | elapsed 686.5s | avg 0.981s/item | ETA 30.5 min


      Progress: 710/2,569 (27.6%) | elapsed 694.2s | avg 0.978s/item | ETA 30.3 min


      Progress: 720/2,569 (28.0%) | elapsed 700.1s | avg 0.972s/item | ETA 30.0 min


      Progress: 730/2,569 (28.4%) | elapsed 705.6s | avg 0.967s/item | ETA 29.6 min


      Progress: 740/2,569 (28.8%) | elapsed 709.0s | avg 0.958s/item | ETA 29.2 min


      Progress: 750/2,569 (29.2%) | elapsed 718.7s | avg 0.958s/item | ETA 29.1 min


      Progress: 760/2,569 (29.6%) | elapsed 750.9s | avg 0.988s/item | ETA 29.8 min


      Progress: 770/2,569 (30.0%) | elapsed 757.9s | avg 0.984s/item | ETA 29.5 min


      Progress: 780/2,569 (30.4%) | elapsed 764.6s | avg 0.980s/item | ETA 29.2 min


      Progress: 790/2,569 (30.8%) | elapsed 767.9s | avg 0.972s/item | ETA 28.8 min


      Progress: 800/2,569 (31.1%) | elapsed 770.9s | avg 0.964s/item | ETA 28.4 min


      Progress: 810/2,569 (31.5%) | elapsed 773.8s | avg 0.955s/item | ETA 28.0 min


      Progress: 820/2,569 (31.9%) | elapsed 776.8s | avg 0.947s/item | ETA 27.6 min


      Progress: 830/2,569 (32.3%) | elapsed 781.6s | avg 0.942s/item | ETA 27.3 min


      Progress: 840/2,569 (32.7%) | elapsed 788.7s | avg 0.939s/item | ETA 27.1 min


      Progress: 850/2,569 (33.1%) | elapsed 796.2s | avg 0.937s/item | ETA 26.8 min


    2026-07-01 12:25:59,866 - WARNING - Retrying (Retry(total=4, connect=5, read=4, redirect=None, status=None)) after connection broken by 'ConnectionResetError(104, 'Connection reset by peer')': /findAllDependencies


      Progress: 860/2,569 (33.5%) | elapsed 804.4s | avg 0.935s/item | ETA 26.6 min


      Progress: 870/2,569 (33.9%) | elapsed 808.4s | avg 0.929s/item | ETA 26.3 min


      Progress: 880/2,569 (34.3%) | elapsed 814.0s | avg 0.925s/item | ETA 26.0 min


      Progress: 890/2,569 (34.6%) | elapsed 821.3s | avg 0.923s/item | ETA 25.8 min


      Progress: 900/2,569 (35.0%) | elapsed 829.5s | avg 0.922s/item | ETA 25.6 min


      Progress: 910/2,569 (35.4%) | elapsed 832.5s | avg 0.915s/item | ETA 25.3 min


      Progress: 920/2,569 (35.8%) | elapsed 843.3s | avg 0.917s/item | ETA 25.2 min


      Progress: 930/2,569 (36.2%) | elapsed 852.4s | avg 0.917s/item | ETA 25.0 min


      Progress: 940/2,569 (36.6%) | elapsed 860.4s | avg 0.915s/item | ETA 24.9 min


      Progress: 950/2,569 (37.0%) | elapsed 865.4s | avg 0.911s/item | ETA 24.6 min


      Progress: 960/2,569 (37.4%) | elapsed 871.9s | avg 0.908s/item | ETA 24.4 min


      Progress: 970/2,569 (37.8%) | elapsed 876.6s | avg 0.904s/item | ETA 24.1 min


      Progress: 980/2,569 (38.1%) | elapsed 880.9s | avg 0.899s/item | ETA 23.8 min


      Progress: 990/2,569 (38.5%) | elapsed 886.3s | avg 0.895s/item | ETA 23.6 min


      Progress: 1,000/2,569 (38.9%) | elapsed 898.2s | avg 0.898s/item | ETA 23.5 min


      Progress: 1,010/2,569 (39.3%) | elapsed 904.0s | avg 0.895s/item | ETA 23.3 min


      Progress: 1,020/2,569 (39.7%) | elapsed 912.9s | avg 0.895s/item | ETA 23.1 min


      Progress: 1,030/2,569 (40.1%) | elapsed 919.9s | avg 0.893s/item | ETA 22.9 min


      Progress: 1,040/2,569 (40.5%) | elapsed 927.2s | avg 0.891s/item | ETA 22.7 min


      Progress: 1,050/2,569 (40.9%) | elapsed 932.9s | avg 0.888s/item | ETA 22.5 min


      Progress: 1,060/2,569 (41.3%) | elapsed 936.6s | avg 0.884s/item | ETA 22.2 min


      Progress: 1,070/2,569 (41.7%) | elapsed 945.1s | avg 0.883s/item | ETA 22.1 min


      Progress: 1,080/2,569 (42.0%) | elapsed 954.3s | avg 0.884s/item | ETA 21.9 min


      Progress: 1,090/2,569 (42.4%) | elapsed 964.3s | avg 0.885s/item | ETA 21.8 min


      Progress: 1,100/2,569 (42.8%) | elapsed 971.8s | avg 0.883s/item | ETA 21.6 min


      Progress: 1,110/2,569 (43.2%) | elapsed 975.4s | avg 0.879s/item | ETA 21.4 min


      Progress: 1,120/2,569 (43.6%) | elapsed 980.0s | avg 0.875s/item | ETA 21.1 min


      Progress: 1,130/2,569 (44.0%) | elapsed 985.0s | avg 0.872s/item | ETA 20.9 min


      Progress: 1,140/2,569 (44.4%) | elapsed 993.3s | avg 0.871s/item | ETA 20.8 min


      Progress: 1,150/2,569 (44.8%) | elapsed 997.4s | avg 0.867s/item | ETA 20.5 min


      Progress: 1,160/2,569 (45.2%) | elapsed 1,002.4s | avg 0.864s/item | ETA 20.3 min


      Progress: 1,170/2,569 (45.5%) | elapsed 1,007.3s | avg 0.861s/item | ETA 20.1 min


      Progress: 1,180/2,569 (45.9%) | elapsed 1,012.0s | avg 0.858s/item | ETA 19.9 min


      Progress: 1,190/2,569 (46.3%) | elapsed 1,017.3s | avg 0.855s/item | ETA 19.6 min


      Progress: 1,200/2,569 (46.7%) | elapsed 1,023.0s | avg 0.852s/item | ETA 19.5 min


      Progress: 1,210/2,569 (47.1%) | elapsed 1,039.2s | avg 0.859s/item | ETA 19.5 min


      Progress: 1,220/2,569 (47.5%) | elapsed 1,046.5s | avg 0.858s/item | ETA 19.3 min


      Progress: 1,230/2,569 (47.9%) | elapsed 1,052.1s | avg 0.855s/item | ETA 19.1 min


      Progress: 1,240/2,569 (48.3%) | elapsed 1,060.0s | avg 0.855s/item | ETA 18.9 min


      Progress: 1,250/2,569 (48.7%) | elapsed 1,066.7s | avg 0.853s/item | ETA 18.8 min


      Progress: 1,260/2,569 (49.0%) | elapsed 1,075.3s | avg 0.853s/item | ETA 18.6 min


      Progress: 1,270/2,569 (49.4%) | elapsed 1,084.8s | avg 0.854s/item | ETA 18.5 min


      Progress: 1,280/2,569 (49.8%) | elapsed 1,090.7s | avg 0.852s/item | ETA 18.3 min


      Progress: 1,290/2,569 (50.2%) | elapsed 1,097.0s | avg 0.850s/item | ETA 18.1 min


      Progress: 1,300/2,569 (50.6%) | elapsed 1,103.4s | avg 0.849s/item | ETA 18.0 min


      Progress: 1,310/2,569 (51.0%) | elapsed 1,109.5s | avg 0.847s/item | ETA 17.8 min


      Progress: 1,320/2,569 (51.4%) | elapsed 1,112.5s | avg 0.843s/item | ETA 17.5 min


      Progress: 1,330/2,569 (51.8%) | elapsed 1,119.6s | avg 0.842s/item | ETA 17.4 min


      Progress: 1,340/2,569 (52.2%) | elapsed 1,123.4s | avg 0.838s/item | ETA 17.2 min


      Progress: 1,350/2,569 (52.5%) | elapsed 1,127.4s | avg 0.835s/item | ETA 17.0 min


      Progress: 1,360/2,569 (52.9%) | elapsed 1,132.3s | avg 0.833s/item | ETA 16.8 min


      Progress: 1,370/2,569 (53.3%) | elapsed 1,135.4s | avg 0.829s/item | ETA 16.6 min


      Progress: 1,380/2,569 (53.7%) | elapsed 1,141.2s | avg 0.827s/item | ETA 16.4 min


      Progress: 1,390/2,569 (54.1%) | elapsed 1,144.8s | avg 0.824s/item | ETA 16.2 min


      Progress: 1,400/2,569 (54.5%) | elapsed 1,149.0s | avg 0.821s/item | ETA 16.0 min


      Progress: 1,410/2,569 (54.9%) | elapsed 1,154.5s | avg 0.819s/item | ETA 15.8 min


      Progress: 1,420/2,569 (55.3%) | elapsed 1,159.0s | avg 0.816s/item | ETA 15.6 min


      Progress: 1,430/2,569 (55.7%) | elapsed 1,162.4s | avg 0.813s/item | ETA 15.4 min


      Progress: 1,440/2,569 (56.1%) | elapsed 1,165.5s | avg 0.809s/item | ETA 15.2 min


      Progress: 1,450/2,569 (56.4%) | elapsed 1,172.3s | avg 0.808s/item | ETA 15.1 min


      Progress: 1,460/2,569 (56.8%) | elapsed 1,177.9s | avg 0.807s/item | ETA 14.9 min


      Progress: 1,470/2,569 (57.2%) | elapsed 1,193.7s | avg 0.812s/item | ETA 14.9 min


      Progress: 1,480/2,569 (57.6%) | elapsed 1,197.6s | avg 0.809s/item | ETA 14.7 min


      Progress: 1,490/2,569 (58.0%) | elapsed 1,201.6s | avg 0.806s/item | ETA 14.5 min


      Progress: 1,500/2,569 (58.4%) | elapsed 1,208.8s | avg 0.806s/item | ETA 14.4 min


      Progress: 1,510/2,569 (58.8%) | elapsed 1,214.2s | avg 0.804s/item | ETA 14.2 min


      Progress: 1,520/2,569 (59.2%) | elapsed 1,223.1s | avg 0.805s/item | ETA 14.1 min


      Progress: 1,530/2,569 (59.6%) | elapsed 1,232.5s | avg 0.806s/item | ETA 14.0 min


      Progress: 1,540/2,569 (59.9%) | elapsed 1,237.4s | avg 0.804s/item | ETA 13.8 min


      Progress: 1,550/2,569 (60.3%) | elapsed 1,242.4s | avg 0.802s/item | ETA 13.6 min


      Progress: 1,560/2,569 (60.7%) | elapsed 1,248.3s | avg 0.800s/item | ETA 13.5 min


      Progress: 1,570/2,569 (61.1%) | elapsed 1,254.4s | avg 0.799s/item | ETA 13.3 min


      Progress: 1,580/2,569 (61.5%) | elapsed 1,260.4s | avg 0.798s/item | ETA 13.1 min


      Progress: 1,590/2,569 (61.9%) | elapsed 1,267.3s | avg 0.797s/item | ETA 13.0 min


      Progress: 1,600/2,569 (62.3%) | elapsed 1,274.4s | avg 0.796s/item | ETA 12.9 min


      Progress: 1,610/2,569 (62.7%) | elapsed 1,279.7s | avg 0.795s/item | ETA 12.7 min


      Progress: 1,620/2,569 (63.1%) | elapsed 1,301.3s | avg 0.803s/item | ETA 12.7 min


      Progress: 1,630/2,569 (63.4%) | elapsed 1,307.7s | avg 0.802s/item | ETA 12.6 min


      Progress: 1,640/2,569 (63.8%) | elapsed 1,312.1s | avg 0.800s/item | ETA 12.4 min


      Progress: 1,650/2,569 (64.2%) | elapsed 1,315.1s | avg 0.797s/item | ETA 12.2 min


      Progress: 1,660/2,569 (64.6%) | elapsed 1,320.4s | avg 0.795s/item | ETA 12.1 min


      Progress: 1,670/2,569 (65.0%) | elapsed 1,324.8s | avg 0.793s/item | ETA 11.9 min


      Progress: 1,680/2,569 (65.4%) | elapsed 1,331.3s | avg 0.792s/item | ETA 11.7 min


      Progress: 1,690/2,569 (65.8%) | elapsed 1,338.2s | avg 0.792s/item | ETA 11.6 min


      Progress: 1,700/2,569 (66.2%) | elapsed 1,343.2s | avg 0.790s/item | ETA 11.4 min


      Progress: 1,710/2,569 (66.6%) | elapsed 1,347.5s | avg 0.788s/item | ETA 11.3 min


      Progress: 1,720/2,569 (67.0%) | elapsed 1,351.5s | avg 0.786s/item | ETA 11.1 min


      Progress: 1,730/2,569 (67.3%) | elapsed 1,355.9s | avg 0.784s/item | ETA 11.0 min


      Progress: 1,740/2,569 (67.7%) | elapsed 1,359.2s | avg 0.781s/item | ETA 10.8 min


      Progress: 1,750/2,569 (68.1%) | elapsed 1,363.3s | avg 0.779s/item | ETA 10.6 min


      Progress: 1,760/2,569 (68.5%) | elapsed 1,366.6s | avg 0.776s/item | ETA 10.5 min


      Progress: 1,770/2,569 (68.9%) | elapsed 1,373.1s | avg 0.776s/item | ETA 10.3 min


      Progress: 1,780/2,569 (69.3%) | elapsed 1,379.1s | avg 0.775s/item | ETA 10.2 min


      Progress: 1,790/2,569 (69.7%) | elapsed 1,386.8s | avg 0.775s/item | ETA 10.1 min


      Progress: 1,800/2,569 (70.1%) | elapsed 1,392.0s | avg 0.773s/item | ETA 9.9 min


      Progress: 1,810/2,569 (70.5%) | elapsed 1,395.3s | avg 0.771s/item | ETA 9.8 min


      Progress: 1,820/2,569 (70.8%) | elapsed 1,401.1s | avg 0.770s/item | ETA 9.6 min


      Progress: 1,830/2,569 (71.2%) | elapsed 1,432.7s | avg 0.783s/item | ETA 9.6 min


      Progress: 1,840/2,569 (71.6%) | elapsed 1,438.3s | avg 0.782s/item | ETA 9.5 min


      Progress: 1,850/2,569 (72.0%) | elapsed 1,446.0s | avg 0.782s/item | ETA 9.4 min


      Progress: 1,860/2,569 (72.4%) | elapsed 1,453.7s | avg 0.782s/item | ETA 9.2 min


      Progress: 1,870/2,569 (72.8%) | elapsed 1,459.9s | avg 0.781s/item | ETA 9.1 min


      Progress: 1,880/2,569 (73.2%) | elapsed 1,463.8s | avg 0.779s/item | ETA 8.9 min


      Progress: 1,890/2,569 (73.6%) | elapsed 1,469.0s | avg 0.777s/item | ETA 8.8 min


      Progress: 1,900/2,569 (74.0%) | elapsed 1,473.1s | avg 0.775s/item | ETA 8.6 min


      Progress: 1,910/2,569 (74.3%) | elapsed 1,477.3s | avg 0.773s/item | ETA 8.5 min


      Progress: 1,920/2,569 (74.7%) | elapsed 1,482.5s | avg 0.772s/item | ETA 8.4 min


      Progress: 1,930/2,569 (75.1%) | elapsed 1,491.0s | avg 0.773s/item | ETA 8.2 min


      Progress: 1,940/2,569 (75.5%) | elapsed 1,496.6s | avg 0.771s/item | ETA 8.1 min


      Progress: 1,950/2,569 (75.9%) | elapsed 1,503.8s | avg 0.771s/item | ETA 8.0 min


      Progress: 1,960/2,569 (76.3%) | elapsed 1,509.8s | avg 0.770s/item | ETA 7.8 min


      Progress: 1,970/2,569 (76.7%) | elapsed 1,512.9s | avg 0.768s/item | ETA 7.7 min


      Progress: 1,980/2,569 (77.1%) | elapsed 1,516.4s | avg 0.766s/item | ETA 7.5 min


      Progress: 1,990/2,569 (77.5%) | elapsed 1,519.8s | avg 0.764s/item | ETA 7.4 min


      Progress: 2,000/2,569 (77.9%) | elapsed 1,525.4s | avg 0.763s/item | ETA 7.2 min


      Progress: 2,010/2,569 (78.2%) | elapsed 1,531.1s | avg 0.762s/item | ETA 7.1 min


      Progress: 2,020/2,569 (78.6%) | elapsed 1,534.7s | avg 0.760s/item | ETA 7.0 min


      Progress: 2,030/2,569 (79.0%) | elapsed 1,538.5s | avg 0.758s/item | ETA 6.8 min


      Progress: 2,040/2,569 (79.4%) | elapsed 1,542.5s | avg 0.756s/item | ETA 6.7 min


      Progress: 2,050/2,569 (79.8%) | elapsed 1,547.1s | avg 0.755s/item | ETA 6.5 min


      Progress: 2,060/2,569 (80.2%) | elapsed 1,551.7s | avg 0.753s/item | ETA 6.4 min


      Progress: 2,070/2,569 (80.6%) | elapsed 1,554.9s | avg 0.751s/item | ETA 6.2 min


      Progress: 2,080/2,569 (81.0%) | elapsed 1,558.6s | avg 0.749s/item | ETA 6.1 min


      Progress: 2,090/2,569 (81.4%) | elapsed 1,561.7s | avg 0.747s/item | ETA 6.0 min


      Progress: 2,100/2,569 (81.7%) | elapsed 1,565.4s | avg 0.745s/item | ETA 5.8 min


      Progress: 2,110/2,569 (82.1%) | elapsed 1,570.1s | avg 0.744s/item | ETA 5.7 min


      Progress: 2,120/2,569 (82.5%) | elapsed 1,573.1s | avg 0.742s/item | ETA 5.6 min


      Progress: 2,130/2,569 (82.9%) | elapsed 1,576.9s | avg 0.740s/item | ETA 5.4 min


      Progress: 2,140/2,569 (83.3%) | elapsed 1,581.8s | avg 0.739s/item | ETA 5.3 min


      Progress: 2,150/2,569 (83.7%) | elapsed 1,584.9s | avg 0.737s/item | ETA 5.1 min


      Progress: 2,160/2,569 (84.1%) | elapsed 1,588.4s | avg 0.735s/item | ETA 5.0 min


      Progress: 2,170/2,569 (84.5%) | elapsed 1,591.9s | avg 0.734s/item | ETA 4.9 min


      Progress: 2,180/2,569 (84.9%) | elapsed 1,595.2s | avg 0.732s/item | ETA 4.7 min


      Progress: 2,190/2,569 (85.2%) | elapsed 1,598.3s | avg 0.730s/item | ETA 4.6 min


      Progress: 2,200/2,569 (85.6%) | elapsed 1,601.6s | avg 0.728s/item | ETA 4.5 min


      Progress: 2,210/2,569 (86.0%) | elapsed 1,605.1s | avg 0.726s/item | ETA 4.3 min


      Progress: 2,220/2,569 (86.4%) | elapsed 1,608.2s | avg 0.724s/item | ETA 4.2 min


      Progress: 2,230/2,569 (86.8%) | elapsed 1,611.7s | avg 0.723s/item | ETA 4.1 min


      Progress: 2,240/2,569 (87.2%) | elapsed 1,615.2s | avg 0.721s/item | ETA 4.0 min


      Progress: 2,250/2,569 (87.6%) | elapsed 1,618.4s | avg 0.719s/item | ETA 3.8 min


      Progress: 2,260/2,569 (88.0%) | elapsed 1,622.1s | avg 0.718s/item | ETA 3.7 min


      Progress: 2,270/2,569 (88.4%) | elapsed 1,625.2s | avg 0.716s/item | ETA 3.6 min


      Progress: 2,280/2,569 (88.8%) | elapsed 1,628.3s | avg 0.714s/item | ETA 3.4 min


      Progress: 2,290/2,569 (89.1%) | elapsed 1,632.1s | avg 0.713s/item | ETA 3.3 min


      Progress: 2,300/2,569 (89.5%) | elapsed 1,635.2s | avg 0.711s/item | ETA 3.2 min


      Progress: 2,310/2,569 (89.9%) | elapsed 1,638.3s | avg 0.709s/item | ETA 3.1 min


      Progress: 2,320/2,569 (90.3%) | elapsed 1,641.5s | avg 0.708s/item | ETA 2.9 min


      Progress: 2,330/2,569 (90.7%) | elapsed 1,646.4s | avg 0.707s/item | ETA 2.8 min


      Progress: 2,340/2,569 (91.1%) | elapsed 1,649.9s | avg 0.705s/item | ETA 2.7 min


      Progress: 2,350/2,569 (91.5%) | elapsed 1,654.3s | avg 0.704s/item | ETA 2.6 min


      Progress: 2,360/2,569 (91.9%) | elapsed 1,657.9s | avg 0.702s/item | ETA 2.4 min


      Progress: 2,370/2,569 (92.3%) | elapsed 1,661.9s | avg 0.701s/item | ETA 2.3 min


      Progress: 2,380/2,569 (92.6%) | elapsed 1,665.8s | avg 0.700s/item | ETA 2.2 min


      Progress: 2,390/2,569 (93.0%) | elapsed 1,669.9s | avg 0.699s/item | ETA 2.1 min


      Progress: 2,400/2,569 (93.4%) | elapsed 1,673.7s | avg 0.697s/item | ETA 2.0 min


      Progress: 2,410/2,569 (93.8%) | elapsed 1,684.3s | avg 0.699s/item | ETA 1.9 min


      Progress: 2,420/2,569 (94.2%) | elapsed 1,687.3s | avg 0.697s/item | ETA 1.7 min


      Progress: 2,430/2,569 (94.6%) | elapsed 1,697.7s | avg 0.699s/item | ETA 1.6 min


      Progress: 2,440/2,569 (95.0%) | elapsed 1,701.2s | avg 0.697s/item | ETA 1.5 min


      Progress: 2,450/2,569 (95.4%) | elapsed 1,706.4s | avg 0.697s/item | ETA 1.4 min


      Progress: 2,460/2,569 (95.8%) | elapsed 1,712.9s | avg 0.696s/item | ETA 1.3 min


      Progress: 2,470/2,569 (96.1%) | elapsed 1,716.6s | avg 0.695s/item | ETA 1.1 min


      Progress: 2,480/2,569 (96.5%) | elapsed 1,720.6s | avg 0.694s/item | ETA 1.0 min


      Progress: 2,490/2,569 (96.9%) | elapsed 1,726.1s | avg 0.693s/item | ETA 0.9 min


      Progress: 2,500/2,569 (97.3%) | elapsed 1,732.2s | avg 0.693s/item | ETA 0.8 min


      Progress: 2,510/2,569 (97.7%) | elapsed 1,738.3s | avg 0.693s/item | ETA 0.7 min


      Progress: 2,520/2,569 (98.1%) | elapsed 1,743.9s | avg 0.692s/item | ETA 0.6 min


      Progress: 2,530/2,569 (98.5%) | elapsed 1,749.2s | avg 0.691s/item | ETA 0.4 min


      Progress: 2,540/2,569 (98.9%) | elapsed 1,756.8s | avg 0.692s/item | ETA 0.3 min


    2026-07-01 12:41:59,449 - INFO - Retrieving roles(start=1, num=100)


      Progress: 2,550/2,569 (99.3%) | elapsed 1,762.8s | avg 0.691s/item | ETA 0.2 min


      Progress: 2,560/2,569 (99.6%) | elapsed 1,770.1s | avg 0.691s/item | ETA 0.1 min


      Progress: 2,569/2,569 (100.0%) | elapsed 1,774.4s | avg 0.691s/item | ETA 0.0 min
    ✓ Graph build complete


### Step 3c.3 Publish to Content Table


```python
# Retrieve table references
table_names = {"content": "Content"}
if dependency_mapping:
    table_names.update({
        "upstream":   "Upstream Dependencies",
        "downstream": "Downstream Dependencies",
    })

tables = {}
for key, name in table_names.items():
    table, sublayer, url = get_table_url(agk_fs, name, gis=gis)
    tables[key] = {"table": table, "sublayer": sublayer, "url": url}
    print(f"{name} table found (sublayer={sublayer}): {url}")

# Clear existing records
if delete_content:
    for key in table_names:
        clear_table(tables[key]["table"])

# Publish
tables_to_publish = [(final_content_df, tables["content"]["table"])]

if dependency_mapping:
    tables_to_publish.extend([
        (upstream_enriched,   tables["upstream"]["table"]),
        (downstream_enriched, tables["downstream"]["table"]),
    ])

for df, table in tables_to_publish:
    publish_dataframe_to_table(df, table)

print("\n✓ Content snapshot complete")
```

    Upstream Dependencies table found (sublayer=6): https://www.arcgis.com/home/item.html?id=1ed3df79ff9447db8ad64ea37920711e&sublayer=6#data


    Downstream Dependencies table found (sublayer=7): https://www.arcgis.com/home/item.html?id=1ed3df79ff9447db8ad64ea37920711e&sublayer=7#data
    
    Clearing existing records from table: Content


    Truncate successful.
    {'success': True}
    
    Clearing existing records from table: Upstream Dependencies


    Truncate successful.
    {'success': True}
    
    Clearing existing records from table: Downstream Dependencies


    Truncate successful.
    {'success': True}
    
    Publishing to table: Content


    Total records to publish : 2,569
    Chunk size               : 500
    
    Publishing rows 1–500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 1/6 (16.7%) | elapsed 3.1s | avg 3.142s/chunk | ETA 0.3 min
    
    Publishing rows 501–1000...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 2/6 (33.3%) | elapsed 5.8s | avg 2.883s/chunk | ETA 0.2 min
    
    Publishing rows 1001–1500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 3/6 (50.0%) | elapsed 8.3s | avg 2.779s/chunk | ETA 0.1 min
    
    Publishing rows 1501–2000...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 4/6 (66.7%) | elapsed 10.8s | avg 2.712s/chunk | ETA 0.1 min
    
    Publishing rows 2001–2500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 5/6 (83.3%) | elapsed 13.1s | avg 2.620s/chunk | ETA 0.0 min
    
    Publishing rows 2501–2569...


      Attempted : 69
      Succeeded : 69
      Failed    : 0
      Progress: 6/6 (100.0%) | elapsed 13.5s | avg 2.251s/chunk | ETA 0.0 min
    
    Content — Publish complete.
      Total attempted : 2,569
      Total succeeded : 2,569
      Total failed    : 0
      Status: SUCCESS
    
    Publishing to table: Upstream Dependencies


    Total records to publish : 1,911
    Chunk size               : 500
    
    Publishing rows 1–500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 1/4 (25.0%) | elapsed 2.8s | avg 2.751s/chunk | ETA 0.1 min
    
    Publishing rows 501–1000...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 2/4 (50.0%) | elapsed 4.6s | avg 2.290s/chunk | ETA 0.1 min
    
    Publishing rows 1001–1500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 3/4 (75.0%) | elapsed 6.3s | avg 2.086s/chunk | ETA 0.0 min
    
    Publishing rows 1501–1911...


      Attempted : 411
      Succeeded : 411
      Failed    : 0
      Progress: 4/4 (100.0%) | elapsed 8.3s | avg 2.078s/chunk | ETA 0.0 min
    
    Upstream Dependencies — Publish complete.
      Total attempted : 1,911
      Total succeeded : 1,911
      Total failed    : 0
      Status: SUCCESS
    
    Publishing to table: Downstream Dependencies


    Total records to publish : 1,911
    Chunk size               : 500
    
    Publishing rows 1–500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 1/4 (25.0%) | elapsed 1.9s | avg 1.873s/chunk | ETA 0.1 min
    
    Publishing rows 501–1000...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 2/4 (50.0%) | elapsed 4.5s | avg 2.254s/chunk | ETA 0.1 min
    
    Publishing rows 1001–1500...


      Attempted : 500
      Succeeded : 500
      Failed    : 0
      Progress: 3/4 (75.0%) | elapsed 6.2s | avg 2.059s/chunk | ETA 0.0 min
    
    Publishing rows 1501–1911...


      Attempted : 411
      Succeeded : 411
      Failed    : 0
      Progress: 4/4 (100.0%) | elapsed 8.0s | avg 2.011s/chunk | ETA 0.0 min
    
    Downstream Dependencies — Publish complete.
      Total attempted : 1,911
      Total succeeded : 1,911
      Total failed    : 0
      Status: SUCCESS
    
    ✓ Content snapshot complete

