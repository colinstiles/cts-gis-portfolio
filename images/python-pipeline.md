# TDF Buildings - Excel to ArcGIS Online Feature Layer Pipeline

Version: v10 direct hosted feature service publish.

This notebook reads the TDF Buildings workbook, applies reviewer-managed overrides, geocodes addresses, validates geocode results, and publishes a simplified operational hosted feature layer schema by directly creating a hosted feature service and adding a named point layer. Full QA fields remain available in the generated CSV outputs but are not published to the feature layer.


## Table of Contents

1. [Configuration](#1.-Configuration)
2. [Imports, logging, and ArcGIS connection](#2.-Imports,-logging,-and-ArcGIS-connection)
3. [Read the workbook](#3.-Read-the-workbook)
4. [Standardize fields](#4.-Standardize-fields)
5. [Address and attribute quality checks](#5.-Address-and-attribute-quality-checks)
6. [Manual override control file](#6.-Manual-override-control-file)
7. [Geocode unique addresses](#7.-Geocode-unique-addresses)
8. [Join geocode results and apply manual overrides](#8.-Join-geocode-results-and-apply-manual-overrides)
9. [Create a Spatially Enabled DataFrame](#9.-Create-a-Spatially-Enabled-DataFrame)
10. [Optional map preview](#10.-Optional-map-preview)
11. [Publish or refresh hosted feature layer](#11.-Publish-or-refresh-hosted-feature-layer)
12. [Run Summary](#12.-Run-Summary)
13. [Final output checklist](#13.-Final-output-checklist)

## 1. Configuration

Upload the Excel workbook into the notebook files area. In ArcGIS Online Notebooks, uploaded files are normally available under `/arcgis/home`.

Set `run_geocoding = True` only after reviewing the normalized data and QA tables. Set `run_publish = True` only after reviewing the geocoded outputs.

Manual corrections should be entered in `tdf_buildings_manual_overrides.csv`, not in the raw source workbook. The override file supports corrected address values, manual coordinates, acceptance of a verified geocoder candidate, and intentional publish exclusions. New publishes are created directly as hosted feature services; the notebook no longer uses the Spatially Enabled DataFrame `to_featurelayer()` helper for new publishes, which avoids creating a separate source File Geodatabase item.

`pd.read_excel()` reads this `.xlsx` workbook with the `openpyxl` engine. ArcGIS Online Notebooks normally include `openpyxl`; local notebook environments may need `pip install openpyxl`.

The default workbook path assumes this file name exists in `/arcgis/home`:

`RPP 2026 AGR Building & Land Status Check.xlsx`



```python
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

# ArcGIS Online Notebooks normally use /arcgis/home.
HOME = Path('/arcgis/home/tdf_facilities') if Path('/arcgis/home/tdf_facilities').exists() else Path.cwd()

@dataclass(frozen=True)
class NotebookConfig:
    project_name: str = 'tdf_buildings_pipeline'
    
    # Paths
    source_path: Path = HOME / 'RPP 2026 AGR Building & Land Status Check.xlsx'
    sheet_name: str = 'Buildings'
    header_row: int = 1  # pandas is zero-based; 1 means Excel row 2.
    excel_engine: str = 'openpyxl'
    
    output_dir: Path = HOME / 'tdf_buildings_outputs'
    log_dir: Path = HOME / 'tdf_buildings_outputs' / 'logs'
    
    # Manual review/correction file. Do not edit the raw workbook for pipeline-only fixes.
    manual_overrides_file_name: str = 'tdf_buildings_manual_overrides.csv'
    
    # Safety switches. Leave publishing off until QA and geocoding outputs are reviewed.
    run_geocoding: bool = False
    run_publish: bool = True
    
    # Geocoding settings.
    geocode_score_min: int = 85
    geocode_mode: str = 'single'  # 'single' or 'batch'
    geocode_batch_size: Optional[int] = None  # None uses locator SuggestedBatchSize, else 100
    geocode_delay_seconds: float = 0.25
    source_country: str = 'USA'
    geocode_location_type: str = 'rooftop'  # 'rooftop' or 'street'
    geocode_category: Optional[str] = None  # Example: 'Point Address,Street Address'
    geocode_cache_version: str = 'v2_result_validation'
    force_regeocode: bool = False  # Keep False after a validated cache exists; set True only to rebuild geocodes.
    max_allowed_suspect_assignments: int = 10
    minimum_publish_ready_records: int = 1
    write_full_geocode_review: bool = True  # True writes all geocode-review records, not only assignment suspects.
    
    # Manual coordinate sanity bounds. These broad bounds catch sign mistakes and transposed coordinates.
    coordinate_min_latitude: float = 34.0
    coordinate_max_latitude: float = 37.5
    coordinate_min_longitude: float = -91.5
    coordinate_max_longitude: float = -80.5
    
    # Publishing settings
    # publish_title is the ArcGIS Online item title.
    # publish_service_name is the URL-safe FeatureServer service name.
    # publish_layer_name is the Layer (0) name shown inside the service.
    publish_title: str = 'TDF Buildings and Facilities'
    publish_service_name: str = 'TDF_Buildings_and_Facilities'
    publish_layer_name: str = 'TDF Buildings and Facilities'
    allow_service_name_suffix_if_unavailable: bool = False
    max_record_count: int = 2000
    publish_tags: tuple[str, ...] = ('TDF', 'Buildings', 'Facilities', 'Assets', 'Bureaus Veritas')
    publish_summary: str = 'Point locations for buildings owned and managed by Tennessee Division of Forestry.'
    publish_description: str = "This hosted feature layer is generated from the Buildings worksheet in the RPP building and land status workbook. Coordinates are produced from address geocoding, reviewer-managed manual overrides, and notebook QA outputs. This layer is intended for planning, asset management, and public-information workflows. It is not a legal survey or substitute for official deeds, surveys, or property records."
    
    # Leave blank to create a new layer. After first publish, paste item ID here to refresh.
    feature_layer_item_id: str = 'a2f0811ed0e44e3e8933411739302e92'
    
    # Existing-layer refresh behavior
    refresh_strategy: str = 'upsert'  # 'upsert', 'truncate_add', or 'delete_add'
    sync_key_field: str = 'building_code'
    upsert_delete_missing_records: bool = True
    publish_batch_size: int = 200
    truncate_async: bool = False

CONFIG = NotebookConfig()
CONFIG.output_dir.mkdir(parents=True, exist_ok=True)
CONFIG.log_dir.mkdir(parents=True, exist_ok=True)

NORMALIZED_CSV = CONFIG.output_dir / 'tdf_buildings_normalized.csv'
GEOCODED_CSV = CONFIG.output_dir / 'tdf_buildings_geocoded.csv'
GEOCODE_CACHE_CSV = CONFIG.output_dir / 'tdf_buildings_geocode_cache.csv'
GEOCODE_VALIDATION_CSV = CONFIG.output_dir / 'tdf_buildings_geocode_validation_review.csv'
QA_REPORT_CSV = CONFIG.output_dir / 'tdf_buildings_quality_report.csv'
ADDRESS_REVIEW_CSV = CONFIG.output_dir / 'tdf_buildings_address_review.csv'
SHARED_ADDRESS_SUMMARY_CSV = CONFIG.output_dir / 'tdf_buildings_shared_address_summary.csv'
ZIP_CONFLICTS_CSV = CONFIG.output_dir / 'tdf_buildings_zip_conflicts.csv'
POTENTIAL_TYPOS_CSV = CONFIG.output_dir / 'tdf_buildings_potential_typos.csv'
SQUARE_FEET_REVIEW_CSV = CONFIG.output_dir / 'tdf_buildings_square_feet_review.csv'
QA_REVIEW_XLSX = CONFIG.output_dir / 'tdf_buildings_qa_review.xlsx'
MANUAL_OVERRIDES_CSV = CONFIG.output_dir / CONFIG.manual_overrides_file_name
LEGACY_MANUAL_COORDINATE_OVERRIDES_CSV = CONFIG.output_dir / 'tdf_buildings_manual_coordinate_overrides.csv'
MANUAL_OVERRIDE_AUDIT_CSV = CONFIG.output_dir / 'tdf_buildings_manual_override_audit.csv'
PUBLISH_FIELDS_PREVIEW_CSV = CONFIG.output_dir / 'tdf_buildings_publish_fields_preview.csv'

# The generated local file geodatabase/source item is intentionally avoided in v10 new-publish mode.
# New publishing uses gis.content.create_service() + FeatureLayerCollection.manager.add_to_definition().

# Operational hosted-feature-layer schema.
# Internal QA/geocoding outputs keep additional diagnostic fields; only these fields are sent to AGOL when publishing.
PUBLISH_ATTRIBUTE_FIELDS = (
    'building_code',
    'building_name',
    'address',
    'city',
    'county',
    'state',
    'postal_code',
    'longitude',
    'latitude',
    'square_feet',
    'predominant_facility_use',
    'building_utilization',
    'asset_status',
    'strategic_asset_status',
    'building_status',
    'geocode_source',
    'override_notes',
    'override_source',
)

PUBLISH_FIELD_ALIASES = {
    'building_code': 'Building Code',
    'building_name': 'Building Name',
    'address': 'Address',
    'city': 'City',
    'county': 'County',
    'state': 'State',
    'postal_code': 'Postal Code',
    'longitude': 'Longitude',
    'latitude': 'Latitude',
    'square_feet': 'Square Feet',
    'predominant_facility_use': 'Predominant Facility Use',
    'building_utilization': 'Building Utilization',
    'asset_status': 'Asset Status',
    'strategic_asset_status': 'Strategic Asset Status',
    'building_status': 'Building Status',
    'geocode_source': 'Geocode Source',
    'override_notes': 'Override Notes',
    'override_source': 'Override Source',
}

CONFIG

```




    NotebookConfig(project_name='tdf_buildings_pipeline', source_path=PosixPath('/arcgis/home/tdf_facilities/RPP 2026 AGR Building & Land Status Check.xlsx'), sheet_name='Buildings', header_row=1, excel_engine='openpyxl', output_dir=PosixPath('/arcgis/home/tdf_facilities/tdf_buildings_outputs'), log_dir=PosixPath('/arcgis/home/tdf_facilities/tdf_buildings_outputs/logs'), manual_overrides_file_name='tdf_buildings_manual_overrides.csv', run_geocoding=False, run_publish=True, geocode_score_min=85, geocode_mode='single', geocode_batch_size=None, geocode_delay_seconds=0.25, source_country='USA', geocode_location_type='rooftop', geocode_category=None, geocode_cache_version='v2_result_validation', force_regeocode=False, max_allowed_suspect_assignments=10, minimum_publish_ready_records=1, write_full_geocode_review=True, coordinate_min_latitude=34.0, coordinate_max_latitude=37.5, coordinate_min_longitude=-91.5, coordinate_max_longitude=-80.5, publish_title='TDF Buildings and Facilities', publish_service_name='TDF_Buildings_and_Facilities', publish_layer_name='TDF Buildings and Facilities', allow_service_name_suffix_if_unavailable=False, max_record_count=2000, publish_tags=('TDF', 'Buildings', 'Facilities', 'Assets', 'Bureaus Veritas'), publish_summary='Point locations for buildings owned and managed by Tennessee Division of Forestry.', publish_description='This hosted feature layer is generated from the Buildings worksheet in the RPP building and land status workbook. Coordinates are produced from address geocoding, reviewer-managed manual overrides, and notebook QA outputs. This layer is intended for planning, asset management, and public-information workflows. It is not a legal survey or substitute for official deeds, surveys, or property records.', feature_layer_item_id='', refresh_strategy='upsert', sync_key_field='building_code', upsert_delete_missing_records=True, publish_batch_size=200, truncate_async=False)



## 2. Imports, logging, and ArcGIS connection

This uses the active ArcGIS Online notebook session. Standard execution logs are dumped to a `logs/` directory.


```python
import json
import logging
import re
import sys
import time
from datetime import datetime, timezone

import pandas as pd
from IPython.display import display

try:
    import openpyxl
except ImportError as exc:
    raise ImportError(
        'Reading .xlsx files with pandas requires openpyxl. '
        'ArcGIS Online Notebooks normally include it; local environments can install it with: pip install openpyxl'
    ) from exc

from arcgis.gis import GIS
from arcgis.geocoding import get_geocoders, geocode, batch_geocode
from arcgis.features import GeoAccessor, GeoSeriesAccessor, FeatureLayerCollection

RUN_STARTED_UTC = datetime.now(timezone.utc)
RUN_ID = RUN_STARTED_UTC.strftime("%Y%m%dT%H%M%SZ")
log_path = CONFIG.log_dir / f"{CONFIG.project_name}_{RUN_ID}.log"

logger = logging.getLogger(CONFIG.project_name)
logger.setLevel(logging.INFO)
logger.handlers.clear()

formatter = logging.Formatter(
    fmt="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

stream_handler = logging.StreamHandler(sys.stdout)
stream_handler.setFormatter(formatter)
file_handler = logging.FileHandler(log_path, encoding="utf-8")
file_handler.setFormatter(formatter)

logger.addHandler(stream_handler)
logger.addHandler(file_handler)

logger.info("Notebook run started")
logger.info("Run ID: %s", RUN_ID)
logger.info("Log path: %s", log_path)

# Uses the authenticated ArcGIS Online notebook context.
gis = GIS('home')
logger.info('Connected to: %s', gis.properties.get('name', gis.url))
logger.info('User: %s', gis.users.me.username)
logger.info('openpyxl version: %s', openpyxl.__version__)

```

    2026-07-08 19:25:06 | INFO | tdf_buildings_pipeline | Notebook run started
    2026-07-08 19:25:06 | INFO | tdf_buildings_pipeline | Run ID: 20260708T192506Z
    2026-07-08 19:25:06 | INFO | tdf_buildings_pipeline | Log path: /arcgis/home/tdf_facilities/tdf_buildings_outputs/logs/tdf_buildings_pipeline_20260708T192506Z.log


    You are logged on as colin.stiles_tndof with an administrator role, proceed with caution.


    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | Connected to: Tennessee Division of Forestry
    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | User: colin.stiles_tndof
    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | openpyxl version: 3.1.5


## 3. Read the workbook

The workbook has a title/notes row above the headers. The `Buildings` headers are on Excel row 2, so the configuration uses `header_row = 1`.


```python
if not CONFIG.source_path.exists():
    xlsx_files = sorted(p.name for p in HOME.glob('*.xlsx'))
    raise FileNotFoundError(
        f'Workbook not found: {CONFIG.source_path}. XLSX files found in {HOME}: {xlsx_files}'
    )

raw = pd.read_excel(
    CONFIG.source_path,
    sheet_name=CONFIG.sheet_name,
    header=CONFIG.header_row,
    dtype=str,
    engine=CONFIG.excel_engine,
)
raw = raw.dropna(how='all').copy()
raw.columns = [re.sub(r'\s+', ' ', str(c).strip()) for c in raw.columns]

logger.info('Read %s records from sheet "%s".', len(raw), CONFIG.sheet_name)
display(raw.head())

```

    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | Read 463 records from sheet "Buildings".



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
      <th>Building Code</th>
      <th>Building Name</th>
      <th>Address 1</th>
      <th>City Code</th>
      <th>State Code</th>
      <th>Postal Code</th>
      <th>County</th>
      <th>SF</th>
      <th>Strategic Asset Status</th>
      <th>Predominant Facility Use</th>
      <th>Asset Status</th>
      <th>Building Utilization</th>
      <th>Historical Status</th>
      <th>Building Status</th>
      <th>Comments</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10000006</td>
      <td>Holston Mtn Radio Building</td>
      <td>National Forest 56</td>
      <td>ELIZABETHTON</td>
      <td>TN</td>
      <td>37643</td>
      <td>CARTER</td>
      <td>600</td>
      <td>Long Term 15+ yrs</td>
      <td>Antenna/Tower</td>
      <td>Current Mission Need</td>
      <td>Utilized</td>
      <td>Not Evaluated</td>
      <td>Active</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10010001</td>
      <td>Carter County Residence</td>
      <td>2651 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td>CARTER</td>
      <td>1400</td>
      <td>Long Term 15+ yrs</td>
      <td>Residential</td>
      <td>Current Mission Need</td>
      <td>Utilized</td>
      <td>Not Evaluated</td>
      <td>Active</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10010002</td>
      <td>Carter County Outbuilding #2</td>
      <td>2651 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td>CARTER</td>
      <td>1600</td>
      <td>Long Term 15+ yrs</td>
      <td>Warehouse Space</td>
      <td>Current Mission Need</td>
      <td>Utilized</td>
      <td>Not Evaluated</td>
      <td>Active</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010003</td>
      <td>Carter County Shop</td>
      <td>2643 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td>CARTER</td>
      <td>1400</td>
      <td>Long Term 15+ yrs</td>
      <td>Public Safety</td>
      <td>Current Mission Need</td>
      <td>Utilized</td>
      <td>Not Evaluated</td>
      <td>Active</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01010001</td>
      <td>Bluebird Storage Building</td>
      <td>395 Firetower Lane</td>
      <td>HEISKELL</td>
      <td>TN</td>
      <td>37754</td>
      <td>ANDERSON</td>
      <td>48</td>
      <td>Long Term 15+ yrs</td>
      <td>Warehouse Space</td>
      <td>Current Mission Need</td>
      <td>Utilized</td>
      <td>Not Evaluated</td>
      <td>Active</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
</div>


## 4. Standardize fields

The output field names are stable, lowercase, and GIS-friendly. This makes future refreshes safer because hosted layer schemas are sensitive to field name changes.

This section also preserves `building_code` as an 8-character text value and `postal_code` as a 5-character text value so leading zeroes are not lost during CSV reads, joins, or hosted-layer refreshes.



```python
SOURCE_TO_OUTPUT_FIELD = {
    'Building Code': 'building_code',
    'Building Name': 'building_name',
    'Address 1': 'address',
    'City Code': 'city',
    'State Code': 'state',
    'Postal Code': 'postal_code',
    'County': 'county',
    'SF': 'square_feet',
    'Strategic Asset Status': 'strategic_asset_status',
    'Predominant Facility Use': 'predominant_facility_use',
    'Asset Status': 'asset_status',
    'Building Utilization': 'building_utilization',
    'Historical Status': 'historical_status',
    'Building Status': 'building_status',
    'Comments': 'comments',
}

missing_columns = [c for c in SOURCE_TO_OUTPUT_FIELD if c not in raw.columns]
if missing_columns:
    raise ValueError(f'Missing expected source columns: {missing_columns}')

df = raw.rename(columns=SOURCE_TO_OUTPUT_FIELD)[list(SOURCE_TO_OUTPUT_FIELD.values())].copy()

def clean_text(value) -> str:
    """Normalize a workbook cell to a stripped string without Excel-style .0 artifacts."""
    if pd.isna(value):
        return ''
    text = str(value).strip()
    text = re.sub(r'\.0$', '', text)
    text = re.sub(r'\s+', ' ', text)
    return text

def clean_identifier(value, width: int | None = None) -> str:
    """Clean an identifier and left-pad numeric identifiers to a stable text width."""
    text = clean_text(value).replace(' ', '')
    if width and text.isdigit():
        return text.zfill(width)
    return text

def clean_postal_code(value) -> str:
    """Clean ZIP values while preserving leading zeroes and the first five ZIP digits."""
    text = clean_text(value)
    if not text:
        return ''

    match = re.match(r'^(\d{1,5})(?:-?\d{4})?$', text)
    if match:
        return match.group(1).zfill(5)

    digits = re.sub(r'\D+', '', text)
    if 1 <= len(digits) <= 5:
        return digits.zfill(5)
    if len(digits) > 5:
        return digits[:5]
    return text

def parse_bool(value) -> bool:
    text = clean_text(value).upper()
    return text in {'1', 'TRUE', 'T', 'YES', 'Y', 'X'}

def clean_display_part(value) -> str:
    return clean_text(value)

def clean_address_key_part(value) -> str:
    """Create stable address-key components without trailing-space artifacts."""
    text = clean_text(value).upper()
    text = re.sub(r'[^A-Z0-9]+', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return text

def make_full_address(row) -> str:
    parts = [row['address'], row['city'], row['state'], row['postal_code']]
    return ', '.join(clean_display_part(p) for p in parts if clean_display_part(p))

def make_address_key(row) -> str:
    parts = [row['address'], row['city'], row['state'], row['postal_code']]
    return '|'.join(clean_address_key_part(p) for p in parts)

def make_address_city_key(row) -> str:
    parts = [row['address'], row['city'], row['state']]
    return '|'.join(clean_address_key_part(p) for p in parts)

def coordinate_in_configured_bounds(latitude, longitude) -> bool:
    if pd.isna(latitude) or pd.isna(longitude):
        return False
    return (
        CONFIG.coordinate_min_latitude <= float(latitude) <= CONFIG.coordinate_max_latitude
        and CONFIG.coordinate_min_longitude <= float(longitude) <= CONFIG.coordinate_max_longitude
    )

TEXT_FIELDS = [c for c in df.columns if c != 'square_feet']
for col in TEXT_FIELDS:
    df[col] = df[col].map(clean_text)

# Preserve key fields as text. Do not cast these fields to numeric.
df['building_code'] = df['building_code'].map(lambda value: clean_identifier(value, width=8))
df['postal_code'] = df['postal_code'].map(clean_postal_code)

df['square_feet_raw'] = df['square_feet'].map(clean_text)
df['square_feet'] = (
    df['square_feet_raw']
    .str.replace(',', '', regex=False)
    .str.strip()
)
df['square_feet'] = pd.to_numeric(df['square_feet'], errors='coerce')

# Preserve source values before applying reviewer-managed overrides.
SOURCE_VALUE_COLUMNS = {
    'address': 'source_address',
    'city': 'source_city',
    'state': 'source_state',
    'postal_code': 'source_postal_code',
    'county': 'source_county',
}
for source_col, preserved_col in SOURCE_VALUE_COLUMNS.items():
    df[preserved_col] = df[source_col]

MANUAL_OVERRIDE_COLUMNS = [
    'building_code',
    'building_name',
    'source_address', 'source_city', 'source_state', 'source_postal_code', 'source_county',
    'override_address', 'override_city', 'override_state', 'override_postal_code', 'override_county',
    'latitude', 'longitude',
    'accept_geocode_candidate', 'exclude_from_publish',
    'override_notes', 'override_source',
]
MANUAL_OVERRIDE_EDITABLE_COLUMNS = [
    'override_address', 'override_city', 'override_state', 'override_postal_code', 'override_county',
    'latitude', 'longitude',
    'accept_geocode_candidate', 'exclude_from_publish',
    'override_notes', 'override_source',
]

def read_manual_override_file() -> pd.DataFrame:
    """Read or create the reviewer-managed override CSV.

    The file is intentionally separate from the raw source workbook. It contains one row per
    building_code plus editable override fields. The notebook refreshes reference columns but
    preserves reviewer-entered override values.
    """
    if MANUAL_OVERRIDES_CSV.exists():
        overrides = pd.read_csv(MANUAL_OVERRIDES_CSV, dtype=str)
        logger.info('Loaded manual override file: %s', MANUAL_OVERRIDES_CSV)
    elif LEGACY_MANUAL_COORDINATE_OVERRIDES_CSV.exists():
        overrides = pd.read_csv(LEGACY_MANUAL_COORDINATE_OVERRIDES_CSV, dtype=str)
        logger.warning(
            'Migrating legacy coordinate override file into expanded manual override file: %s',
            MANUAL_OVERRIDES_CSV,
        )
    else:
        overrides = pd.DataFrame(columns=MANUAL_OVERRIDE_COLUMNS)
        logger.info('Creating new manual override file: %s', MANUAL_OVERRIDES_CSV)

    for col in MANUAL_OVERRIDE_COLUMNS:
        if col not in overrides.columns:
            overrides[col] = ''

    overrides['building_code'] = overrides['building_code'].map(lambda value: clean_identifier(value, width=8))
    overrides = overrides.loc[overrides['building_code'].ne('')].copy()

    if overrides['building_code'].duplicated().any():
        duplicate_codes = sorted(overrides.loc[overrides['building_code'].duplicated(keep=False), 'building_code'].unique())[:20]
        logger.warning(
            'Manual override file has duplicate building_code rows. Keeping the last row for each code. Examples: %s',
            duplicate_codes,
        )
        overrides = overrides.drop_duplicates('building_code', keep='last')

    current_reference = df[[
        'building_code', 'building_name', 'source_address', 'source_city', 'source_state', 'source_postal_code', 'source_county'
    ]].copy()

    # Preserve editable values from the existing override file and refresh reference columns from the source workbook.
    editable = overrides[['building_code'] + MANUAL_OVERRIDE_EDITABLE_COLUMNS].copy()
    merged = current_reference.merge(editable, on='building_code', how='left')

    # Keep any override rows that no longer exist in the source workbook, so reviewer work is not silently discarded.
    extra_override_rows = overrides.loc[~overrides['building_code'].isin(current_reference['building_code'])].copy()
    if not extra_override_rows.empty:
        extra_override_rows = extra_override_rows[MANUAL_OVERRIDE_COLUMNS].copy()
        extra_override_rows['building_name'] = extra_override_rows['building_name'].fillna('[not in current source workbook]')
        merged = pd.concat([merged, extra_override_rows], ignore_index=True)
        logger.warning('Manual override file includes %s building_code rows not found in the current source workbook.', len(extra_override_rows))

    for col in MANUAL_OVERRIDE_COLUMNS:
        if col not in merged.columns:
            merged[col] = ''
    merged = merged[MANUAL_OVERRIDE_COLUMNS].fillna('')
    merged.to_csv(MANUAL_OVERRIDES_CSV, index=False)
    logger.info('Wrote/refreshed manual override file without changing reviewer-entered override values: %s', MANUAL_OVERRIDES_CSV)
    return merged

manual_overrides = read_manual_override_file()
manual_override_current = manual_overrides.loc[manual_overrides['building_code'].isin(df['building_code'])].copy()
manual_override_by_code = manual_override_current.set_index('building_code', drop=False)

# Join override values onto the working dataframe.
override_join_columns = ['building_code'] + MANUAL_OVERRIDE_EDITABLE_COLUMNS
df = df.merge(
    manual_override_current[override_join_columns],
    on='building_code',
    how='left',
)
for col in MANUAL_OVERRIDE_EDITABLE_COLUMNS:
    if col not in df.columns:
        df[col] = ''
    df[col] = df[col].fillna('').map(clean_text)

# Clean override-specific fields.
df['override_postal_code'] = df['override_postal_code'].map(clean_postal_code)
df['manual_latitude'] = pd.to_numeric(df['latitude'], errors='coerce')
df['manual_longitude'] = pd.to_numeric(df['longitude'], errors='coerce')
df['manual_coordinate_entered'] = df['manual_latitude'].notna() | df['manual_longitude'].notna()
df['manual_coordinate_valid'] = [
    coordinate_in_configured_bounds(lat, lon)
    for lat, lon in zip(df['manual_latitude'], df['manual_longitude'])
]
df['manual_coordinate_invalid'] = df['manual_coordinate_entered'] & ~df['manual_coordinate_valid']
df['accepted_geocode_candidate'] = df['accept_geocode_candidate'].map(parse_bool)
df['exclude_from_publish_override'] = df['exclude_from_publish'].map(parse_bool)

override_field_pairs = [
    ('override_address', 'address'),
    ('override_city', 'city'),
    ('override_state', 'state'),
    ('override_postal_code', 'postal_code'),
    ('override_county', 'county'),
]
for override_col, target_col in override_field_pairs:
    mask = df[override_col].ne('')
    if target_col == 'postal_code':
        df.loc[mask, target_col] = df.loc[mask, override_col].map(clean_postal_code)
    else:
        df.loc[mask, target_col] = df.loc[mask, override_col].map(clean_text)

address_override_cols = ['override_address', 'override_city', 'override_state', 'override_postal_code', 'override_county']
df['has_address_override'] = df[address_override_cols].apply(lambda row: any(clean_text(v) for v in row), axis=1)
df['has_any_manual_override'] = (
    df['has_address_override']
    | df['manual_coordinate_entered']
    | df['accepted_geocode_candidate']
    | df['exclude_from_publish_override']
    | df['override_notes'].ne('')
    | df['override_source'].ne('')
)

# Build effective address keys after address/city/state/ZIP overrides are applied.
df['full_address'] = df.apply(make_full_address, axis=1)
df['address_key'] = df.apply(make_address_key, axis=1)
df['address_city_key'] = df.apply(make_address_city_key, axis=1)

# Add an automatic timestamp for when the record was processed.
df['last_processed_utc'] = RUN_STARTED_UTC.replace(microsecond=0).isoformat()

manual_override_audit = df.loc[df['has_any_manual_override'], [
    'building_code', 'building_name',
    'source_address', 'source_city', 'source_state', 'source_postal_code', 'source_county',
    'address', 'city', 'state', 'postal_code', 'county',
    'override_address', 'override_city', 'override_state', 'override_postal_code', 'override_county',
    'manual_latitude', 'manual_longitude', 'manual_coordinate_valid', 'manual_coordinate_invalid',
    'accepted_geocode_candidate', 'exclude_from_publish_override', 'override_notes', 'override_source',
]].copy()
manual_override_audit.to_csv(MANUAL_OVERRIDE_AUDIT_CSV, index=False)
logger.info('Saved manual override audit CSV: %s', MANUAL_OVERRIDE_AUDIT_CSV)

manual_override_summary = pd.DataFrame([
    {'metric': 'Manual override file rows', 'count': len(manual_overrides)},
    {'metric': 'Current source records with any override', 'count': int(df['has_any_manual_override'].sum())},
    {'metric': 'Records with address/city/state/ZIP/county override', 'count': int(df['has_address_override'].sum())},
    {'metric': 'Records with valid manual coordinates', 'count': int(df['manual_coordinate_valid'].sum())},
    {'metric': 'Records with invalid manual coordinates', 'count': int(df['manual_coordinate_invalid'].sum())},
    {'metric': 'Records accepting geocode candidate', 'count': int(df['accepted_geocode_candidate'].sum())},
    {'metric': 'Records excluded from publish by override', 'count': int(df['exclude_from_publish_override'].sum())},
])
display(manual_override_summary)

display(df.head())
logger.info('Dataframe dtypes:\n%s', df.dtypes)

```

    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | Loaded manual override file: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_manual_overrides.csv
    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | Wrote/refreshed manual override file without changing reviewer-entered override values: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_manual_overrides.csv
    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | Saved manual override audit CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_manual_override_audit.csv



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
      <th>metric</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Manual override file rows</td>
      <td>463</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Current source records with any override</td>
      <td>9</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Records with address/city/state/ZIP/county ove...</td>
      <td>9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Records with valid manual coordinates</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Records with invalid manual coordinates</td>
      <td>0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Records accepting geocode candidate</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Records excluded from publish by override</td>
      <td>0</td>
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
      <th>building_code</th>
      <th>building_name</th>
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>county</th>
      <th>square_feet</th>
      <th>strategic_asset_status</th>
      <th>predominant_facility_use</th>
      <th>...</th>
      <th>manual_coordinate_valid</th>
      <th>manual_coordinate_invalid</th>
      <th>accepted_geocode_candidate</th>
      <th>exclude_from_publish_override</th>
      <th>has_address_override</th>
      <th>has_any_manual_override</th>
      <th>full_address</th>
      <th>address_key</th>
      <th>address_city_key</th>
      <th>last_processed_utc</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10000006</td>
      <td>Holston Mtn Radio Building</td>
      <td>National Forest 56</td>
      <td>ELIZABETHTON</td>
      <td>TN</td>
      <td>37643</td>
      <td>CARTER</td>
      <td>600.0</td>
      <td>Long Term 15+ yrs</td>
      <td>Antenna/Tower</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>National Forest 56, ELIZABETHTON, TN, 37643</td>
      <td>NATIONAL FOREST 56|ELIZABETHTON|TN|37643</td>
      <td>NATIONAL FOREST 56|ELIZABETHTON|TN</td>
      <td>2026-07-08T19:25:06+00:00</td>
    </tr>
    <tr>
      <th>1</th>
      <td>10010001</td>
      <td>Carter County Residence</td>
      <td>2651 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td>CARTER</td>
      <td>1400.0</td>
      <td>Long Term 15+ yrs</td>
      <td>Residential</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2651 Gap Creek Road, HAMPTON, TN, 37658</td>
      <td>2651 GAP CREEK ROAD|HAMPTON|TN|37658</td>
      <td>2651 GAP CREEK ROAD|HAMPTON|TN</td>
      <td>2026-07-08T19:25:06+00:00</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10010002</td>
      <td>Carter County Outbuilding #2</td>
      <td>2651 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td>CARTER</td>
      <td>1600.0</td>
      <td>Long Term 15+ yrs</td>
      <td>Warehouse Space</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2651 Gap Creek Road, HAMPTON, TN, 37658</td>
      <td>2651 GAP CREEK ROAD|HAMPTON|TN|37658</td>
      <td>2651 GAP CREEK ROAD|HAMPTON|TN</td>
      <td>2026-07-08T19:25:06+00:00</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010003</td>
      <td>Carter County Shop</td>
      <td>2643 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td>CARTER</td>
      <td>1400.0</td>
      <td>Long Term 15+ yrs</td>
      <td>Public Safety</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>2643 Gap Creek Road, HAMPTON, TN, 37658</td>
      <td>2643 GAP CREEK ROAD|HAMPTON|TN|37658</td>
      <td>2643 GAP CREEK ROAD|HAMPTON|TN</td>
      <td>2026-07-08T19:25:06+00:00</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01010001</td>
      <td>Bluebird Storage Building</td>
      <td>395 Firetower Lane</td>
      <td>HEISKELL</td>
      <td>TN</td>
      <td>37754</td>
      <td>ANDERSON</td>
      <td>48.0</td>
      <td>Long Term 15+ yrs</td>
      <td>Warehouse Space</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>395 Firetower Lane, HEISKELL, TN, 37754</td>
      <td>395 FIRETOWER LANE|HEISKELL|TN|37754</td>
      <td>395 FIRETOWER LANE|HEISKELL|TN</td>
      <td>2026-07-08T19:25:06+00:00</td>
    </tr>
  </tbody>
</table>
<p>5 rows × 45 columns</p>
</div>


    2026-07-08 19:25:07 | INFO | tdf_buildings_pipeline | Dataframe dtypes:
    building_code                        str
    building_name                        str
    address                              str
    city                                 str
    state                                str
    postal_code                          str
    county                               str
    square_feet                      float64
    strategic_asset_status               str
    predominant_facility_use             str
    asset_status                         str
    building_utilization                 str
    historical_status                    str
    building_status                      str
    comments                             str
    square_feet_raw                      str
    source_address                       str
    source_city                          str
    source_state                         str
    source_postal_code                   str
    source_county                        str
    override_address                     str
    override_city                        str
    override_state                       str
    override_postal_code                 str
    override_county                      str
    latitude                             str
    longitude                            str
    accept_geocode_candidate             str
    exclude_from_publish                 str
    override_notes                       str
    override_source                      str
    manual_latitude                  float64
    manual_longitude                 float64
    manual_coordinate_entered           bool
    manual_coordinate_valid             bool
    manual_coordinate_invalid           bool
    accepted_geocode_candidate          bool
    exclude_from_publish_override       bool
    has_address_override                bool
    has_any_manual_override             bool
    full_address                         str
    address_key                          str
    address_city_key                     str
    last_processed_utc                   str
    dtype: object


## 5. Address and attribute quality checks

Review these tables before enabling geocoding. The notebook flags conditions that typically create weak geocoding results, including blank or placeholder addresses, PO Boxes, addresses that do not start with a physical site/house number, legacy route/box addresses, missing city/state/postal values, duplicate building codes, shared site addresses, ZIP conflicts, and likely typo candidates.



```python
PLACEHOLDER_VALUES = {'', 'NONE', 'UNIDENTIFIED', 'UNKNOWN', 'N/A', 'NA', 'TBD'}
PO_BOX_PATTERN = r'\bP\.?\s*O\.?\s*BOX\b|\bPOST\s+OFFICE\s+BOX\b'
HOUSE_NUMBER_PATTERN = r'^\s*\d+[A-Z]?(?:[-/]\d+[A-Z]?)?\b'

# Target legacy rural-route patterns without treating valid numbered road addresses
# such as "962 Old State Route 48" as legacy route/box addresses.
LEGACY_ROUTE_PATTERN = (
    r'^\s*(?:HCR|HC|RR|RFD)\b'
    r'|^\s*(?:RT|RTE|ROUTE)\s+\d+\b'
    r'|\b(?:BOX|BX)\s+\d+\b'
)

address_upper = df['address'].fillna('').astype(str).str.upper().str.strip()

df['is_placeholder_address'] = (
    address_upper.isin(PLACEHOLDER_VALUES)
    | address_upper.str.contains(r'UNIDENTIFIED|UNKNOWN|NO ADDRESS', regex=True, na=False)
)
df['is_po_box_address'] = address_upper.str.contains(PO_BOX_PATTERN, regex=True, na=False)
df['legacy_route_or_box_address'] = (
    address_upper.str.contains(LEGACY_ROUTE_PATTERN, regex=True, na=False)
    & ~df['is_po_box_address']
)
df['address_lacks_digit'] = ~address_upper.str.contains(r'\d', regex=True, na=False)
df['address_starts_with_site_number'] = address_upper.str.match(HOUSE_NUMBER_PATTERN, na=False)
df['address_does_not_start_with_site_number'] = ~df['address_starts_with_site_number']
df['missing_city'] = df['city'].eq('')
df['missing_state'] = df['state'].eq('')
df['missing_postal_code'] = df['postal_code'].eq('')
df['missing_building_code'] = df['building_code'].eq('')
df['duplicate_building_code'] = df['building_code'].ne('') & df.duplicated('building_code', keep=False)

duplicate_address_counts = df.groupby('address_key')['building_code'].transform('count')
df['shared_address'] = duplicate_address_counts.gt(1)

# This is intentionally stricter than address_lacks_digit. A road name such as "Hwy 100"
# contains a digit but does not identify a physical building/site location.
df['manual_location_review_needed'] = (
    df['is_placeholder_address']
    | df['is_po_box_address']
    | df['legacy_route_or_box_address']
    | df['address_does_not_start_with_site_number']
    | df['missing_city']
    | df['missing_state']
    | df['missing_postal_code']
)

def manual_location_review_reason(row) -> str:
    reasons = []
    if row['is_placeholder_address']:
        reasons.append('placeholder_or_unknown_address')
    if row['is_po_box_address']:
        reasons.append('po_box_address')
    if row['legacy_route_or_box_address']:
        reasons.append('legacy_route_or_box_address')
    if row['address_does_not_start_with_site_number']:
        reasons.append('address_does_not_start_with_site_number')
    if row['missing_city']:
        reasons.append('missing_city')
    if row['missing_state']:
        reasons.append('missing_state')
    if row['missing_postal_code']:
        reasons.append('missing_postal_code')
    return '; '.join(reasons)

df['manual_location_review_reason'] = df.apply(manual_location_review_reason, axis=1)

zip_conflict_keys = (
    df.loc[df['postal_code'].ne('')]
    .groupby('address_city_key')['postal_code']
    .nunique()
)
zip_conflict_key_set = set(zip_conflict_keys.loc[zip_conflict_keys.gt(1)].index)
df['zip_conflict_for_address_city'] = df['address_city_key'].isin(zip_conflict_key_set)

address_number = address_upper.str.extract(r'^\s*(\d+)')[0].fillna('')
df['address_leading_number_digits'] = address_number.str.len()

TYPO_PATTERNS = [
    (r'\bMURFRESSBORO\b', 'verify_street_spelling_murfressboro'),
    (r'\bSULPHER\b', 'verify_street_spelling_sulpher'),
    (r'\bCUMBERLAND\s+FUR\.?\b', 'verify_city_abbreviation_cumberland_fur'),
]

def potential_typo_reason(row) -> str:
    text = f"{row['address']} {row['city']}".upper()
    reasons = []
    if row['address_leading_number_digits'] >= 6:
        reasons.append('verify_long_leading_address_number')
    for pattern, reason in TYPO_PATTERNS:
        if re.search(pattern, text):
            reasons.append(reason)
    return '; '.join(reasons)

df['potential_typo_reason'] = df.apply(potential_typo_reason, axis=1)
df['potential_typo_review_needed'] = df['potential_typo_reason'].ne('')

qa_summary = pd.DataFrame([
    {'check': 'Total records', 'count': len(df)},
    {'check': 'Unique building codes', 'count': df['building_code'].nunique(dropna=True)},
    {'check': 'Records with any manual override', 'count': int(df['has_any_manual_override'].sum())},
    {'check': 'Records with address/city/state/ZIP/county override', 'count': int(df['has_address_override'].sum())},
    {'check': 'Records with valid manual coordinates', 'count': int(df['manual_coordinate_valid'].sum())},
    {'check': 'Records accepting geocode candidate', 'count': int(df['accepted_geocode_candidate'].sum())},
    {'check': 'Records excluded from publish by override', 'count': int(df['exclude_from_publish_override'].sum())},
    {'check': 'Blank or placeholder addresses', 'count': int(df['is_placeholder_address'].sum())},
    {'check': 'PO Box addresses', 'count': int(df['is_po_box_address'].sum())},
    {'check': 'Legacy route or box addresses', 'count': int(df['legacy_route_or_box_address'].sum())},
    {'check': 'Addresses lacking any digit', 'count': int(df['address_lacks_digit'].sum())},
    {'check': 'Addresses not starting with site number', 'count': int(df['address_does_not_start_with_site_number'].sum())},
    {'check': 'Manual location review needed', 'count': int(df['manual_location_review_needed'].sum())},
    {'check': 'Missing city', 'count': int(df['missing_city'].sum())},
    {'check': 'Missing state', 'count': int(df['missing_state'].sum())},
    {'check': 'Missing postal code', 'count': int(df['missing_postal_code'].sum())},
    {'check': 'Address/city combinations with ZIP conflicts', 'count': int(len(zip_conflict_key_set))},
    {'check': 'Potential typo review records', 'count': int(df['potential_typo_review_needed'].sum())},
    {'check': 'Duplicate building codes', 'count': int(df['duplicate_building_code'].sum())},
    {'check': 'Records sharing an address with another building', 'count': int(df['shared_address'].sum())},
])

display(qa_summary)
qa_summary.to_csv(QA_REPORT_CSV, index=False)
logger.info('Saved QA report: %s', QA_REPORT_CSV)

review_cols = [
    'building_code', 'building_name',
    'source_address', 'source_city', 'source_state', 'source_postal_code',
    'address', 'city', 'state', 'postal_code', 'county',
    'has_address_override', 'manual_coordinate_valid', 'accepted_geocode_candidate', 'exclude_from_publish_override',
    'manual_location_review_needed', 'manual_location_review_reason', 'potential_typo_reason',
]
address_review = df.loc[
    df['manual_location_review_needed']
    | df['potential_typo_review_needed']
    | df['zip_conflict_for_address_city']
    | df['manual_coordinate_invalid'],
    review_cols + [
        'is_placeholder_address', 'is_po_box_address', 'legacy_route_or_box_address',
        'address_lacks_digit', 'address_starts_with_site_number',
        'address_does_not_start_with_site_number', 'missing_city', 'missing_state',
        'missing_postal_code', 'zip_conflict_for_address_city', 'manual_coordinate_invalid',
    ],
].copy().sort_values(['manual_location_review_needed', 'building_code'], ascending=[False, True])

address_review.to_csv(ADDRESS_REVIEW_CSV, index=False)
logger.info('Saved address review CSV: %s', ADDRESS_REVIEW_CSV)
display(address_review.head(75))

```


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
      <th>check</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Total records</td>
      <td>463</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Unique building codes</td>
      <td>463</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Records with any manual override</td>
      <td>9</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Records with address/city/state/ZIP/county ove...</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Records with valid manual coordinates</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Records accepting geocode candidate</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Records excluded from publish by override</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Blank or placeholder addresses</td>
      <td>4</td>
    </tr>
    <tr>
      <th>8</th>
      <td>PO Box addresses</td>
      <td>1</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Legacy route or box addresses</td>
      <td>24</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Addresses lacking any digit</td>
      <td>37</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Addresses not starting with site number</td>
      <td>65</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Manual location review needed</td>
      <td>65</td>
    </tr>
    <tr>
      <th>13</th>
      <td>Missing city</td>
      <td>0</td>
    </tr>
    <tr>
      <th>14</th>
      <td>Missing state</td>
      <td>0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>Missing postal code</td>
      <td>0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>Address/city combinations with ZIP conflicts</td>
      <td>3</td>
    </tr>
    <tr>
      <th>17</th>
      <td>Potential typo review records</td>
      <td>6</td>
    </tr>
    <tr>
      <th>18</th>
      <td>Duplicate building codes</td>
      <td>0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>Records sharing an address with another building</td>
      <td>400</td>
    </tr>
  </tbody>
</table>
</div>


    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved QA report: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_quality_report.csv
    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved address review CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_address_review.csv



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
      <th>building_code</th>
      <th>building_name</th>
      <th>source_address</th>
      <th>source_city</th>
      <th>source_state</th>
      <th>source_postal_code</th>
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>...</th>
      <th>is_po_box_address</th>
      <th>legacy_route_or_box_address</th>
      <th>address_lacks_digit</th>
      <th>address_starts_with_site_number</th>
      <th>address_does_not_start_with_site_number</th>
      <th>missing_city</th>
      <th>missing_state</th>
      <th>missing_postal_code</th>
      <th>zip_conflict_for_address_city</th>
      <th>manual_coordinate_invalid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>12</th>
      <td>04000007</td>
      <td>Hendon Fire Tower</td>
      <td>Fire Tower Road</td>
      <td>GRAYSVILLE</td>
      <td>TN</td>
      <td>37338</td>
      <td>Fire Tower Road</td>
      <td>GRAYSVILLE</td>
      <td>TN</td>
      <td>37338</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>13</th>
      <td>04010001</td>
      <td>Bledsoe SF Smoke House</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>...</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>14</th>
      <td>04010002</td>
      <td>Bledsoe SF Storm Shelter</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>...</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>15</th>
      <td>04010005</td>
      <td>Bledsoe SF Sawmill Shed</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>...</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>16</th>
      <td>04010006</td>
      <td>Bledsoe SF Implement Shed</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>...</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>173</th>
      <td>36020004</td>
      <td>Olive Hill Shop</td>
      <td>1485 Firetower Rd</td>
      <td>SAVANNAH</td>
      <td>TN</td>
      <td>38372</td>
      <td>1485 Firetower Rd</td>
      <td>SAVANNAH</td>
      <td>TN</td>
      <td>38372</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
    </tr>
    <tr>
      <th>266</th>
      <td>55040001</td>
      <td>Ramer Fire Tower Storage Shed</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>267</th>
      <td>55040002</td>
      <td>Ramer Fire Tower</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>268</th>
      <td>55040003</td>
      <td>Ramer Fire Tower Crew Shed</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>318</th>
      <td>63000030</td>
      <td>Cunningham Office</td>
      <td>4275 Louise Road</td>
      <td>CUMBERLAND FUR.</td>
      <td>TN</td>
      <td>37051</td>
      <td>4275 Louise Road</td>
      <td>CUMBERLAND FUR.</td>
      <td>TN</td>
      <td>37051</td>
      <td>...</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
<p>75 rows × 29 columns</p>
</div>



```python
duplicate_address_summary = (
    df.groupby('address_key', dropna=False)
    .agg(
        building_count=('building_code', 'count'),
        full_address=('full_address', 'first'),
        building_codes=('building_code', lambda s: ', '.join(sorted(set(x for x in s if x))))
    )
    .query('building_count > 1')
    .sort_values(['building_count', 'full_address'], ascending=[False, True])
    .reset_index(drop=True)
)

duplicate_address_summary.to_csv(SHARED_ADDRESS_SUMMARY_CSV, index=False)
logger.info('Duplicate/shared address groups: %s', len(duplicate_address_summary))
logger.info('Saved shared address summary: %s', SHARED_ADDRESS_SUMMARY_CSV)
display(duplicate_address_summary.head(50))

zip_conflicts = (
    df.loc[df['zip_conflict_for_address_city']]
    .groupby('address_city_key', dropna=False)
    .agg(
        address=('address', 'first'),
        city=('city', 'first'),
        state=('state', 'first'),
        postal_codes=('postal_code', lambda s: ', '.join(sorted(set(x for x in s if x)))),
        building_count=('building_code', 'count'),
        building_codes=('building_code', lambda s: ', '.join(sorted(set(x for x in s if x))))
    )
    .sort_values(['city', 'address'])
    .reset_index(drop=True)
)
zip_conflicts.to_csv(ZIP_CONFLICTS_CSV, index=False)
logger.info('Saved ZIP conflicts CSV: %s', ZIP_CONFLICTS_CSV)
display(zip_conflicts)

potential_typos = (
    df.loc[df['potential_typo_review_needed'], [
        'building_code', 'building_name', 'address', 'city', 'state', 'postal_code',
        'potential_typo_reason', 'full_address'
    ]]
    .sort_values(['potential_typo_reason', 'building_code'])
    .reset_index(drop=True)
)
potential_typos.to_csv(POTENTIAL_TYPOS_CSV, index=False)
logger.info('Saved potential typo review CSV: %s', POTENTIAL_TYPOS_CSV)
display(potential_typos)

square_feet_review = (
    df.loc[
        df['square_feet'].isna() | df['square_feet'].fillna(0).eq(0),
        ['building_code', 'building_name', 'address', 'city', 'state', 'postal_code', 'square_feet_raw', 'square_feet']
    ]
    .sort_values('building_code')
    .reset_index(drop=True)
)
square_feet_review.to_csv(SQUARE_FEET_REVIEW_CSV, index=False)
logger.info('Saved square feet review CSV: %s', SQUARE_FEET_REVIEW_CSV)
display(square_feet_review.head(50))

# Save normalized pre-geocode data with all QA flags included.
df.to_csv(NORMALIZED_CSV, index=False)
logger.info('Saved normalized CSV: %s', NORMALIZED_CSV)

# Optional consolidated QA workbook for easier review outside the notebook.
with pd.ExcelWriter(QA_REVIEW_XLSX, engine=CONFIG.excel_engine) as writer:
    qa_summary.to_excel(writer, sheet_name='QA Summary', index=False)
    address_review.to_excel(writer, sheet_name='Address Review', index=False)
    zip_conflicts.to_excel(writer, sheet_name='ZIP Conflicts', index=False)
    potential_typos.to_excel(writer, sheet_name='Potential Typos', index=False)
    duplicate_address_summary.to_excel(writer, sheet_name='Shared Addresses', index=False)
    square_feet_review.to_excel(writer, sheet_name='Square Feet Review', index=False)
logger.info('Saved QA review workbook: %s', QA_REVIEW_XLSX)

```

    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Duplicate/shared address groups: 105
    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved shared address summary: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_shared_address_summary.csv



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
      <th>building_count</th>
      <th>full_address</th>
      <th>building_codes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>15</td>
      <td>Route 3 Box 180, PIKEVILLE, TN, 37367</td>
      <td>04010001, 04010002, 04010005, 04010006, 040100...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>12</td>
      <td>420 Ozier Rd., PINSON, TN, 38366</td>
      <td>57020001, 57020002, 57020003, 57020004, 570200...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10</td>
      <td>400 Firetower Ln., LIVINGSTON, TN, 38570</td>
      <td>67010001, 67010002, 67010003, 67010004, 670100...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>9</td>
      <td>16965 South Pittsburg Road, SEWANEE, TN, 37380</td>
      <td>26010001, 26010002, 26020001, 26020002, 260200...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>9</td>
      <td>4266 Pickett Park Hwy, JAMESTOWN, TN, 38556</td>
      <td>25040001, 25040002, 25040003, 25040004, 250400...</td>
    </tr>
    <tr>
      <th>5</th>
      <td>8</td>
      <td>9063 Highway 411, DELANO, TN, 37325</td>
      <td>70010001, 70010002, 70010003, 70010004, 700100...</td>
    </tr>
    <tr>
      <th>6</th>
      <td>7</td>
      <td>1865 Fire Tower Rd., MEDON, TN, 38356</td>
      <td>57010001, 57010002, 57010003, 57010004, 570100...</td>
    </tr>
    <tr>
      <th>7</th>
      <td>7</td>
      <td>22117 Natchez Trace Rd, WILDERSVILLE, TN, 38388</td>
      <td>39040001, 39040002, 39040003, 39040004, 390600...</td>
    </tr>
    <tr>
      <th>8</th>
      <td>7</td>
      <td>3497 Church Street, BURNS, TN, 37029</td>
      <td>22020001, 22020002, 22020003, 22020004, 220200...</td>
    </tr>
    <tr>
      <th>9</th>
      <td>6</td>
      <td>1327 Montlake Road, SODDY DAISY, TN, 37379</td>
      <td>33030001, 33030002, 33030003, 33030004, 330300...</td>
    </tr>
    <tr>
      <th>10</th>
      <td>6</td>
      <td>1745 Leach Road, CEDAR GROVE, TN, 38321</td>
      <td>09010001, 09010002, 09010003, 09010005, 090100...</td>
    </tr>
    <tr>
      <th>11</th>
      <td>6</td>
      <td>195 Macedonia Road, HENRY, TN, 38231</td>
      <td>40010001, 40010002, 40010003, 40010004, 400100...</td>
    </tr>
    <tr>
      <th>12</th>
      <td>6</td>
      <td>310 Fire Tower Road, SEWANEE, TN, 37375</td>
      <td>26030001, 26030002, 26030003, 26030004, 260300...</td>
    </tr>
    <tr>
      <th>13</th>
      <td>6</td>
      <td>500 FireTower Road, DICKSON, TN, 37055</td>
      <td>22010001, 22010002, 22010003, 22010004, 220100...</td>
    </tr>
    <tr>
      <th>14</th>
      <td>5</td>
      <td>1454 Waynesboro Hwy, HOHENWALD, TN, 38462</td>
      <td>51010001, 51010002, 51010003, 51010004, 51010005</td>
    </tr>
    <tr>
      <th>15</th>
      <td>5</td>
      <td>147 Forest Tower Road, DOVER, TN, 37058</td>
      <td>81000002, 81010001, 81010002, 81010003, 81010004</td>
    </tr>
    <tr>
      <th>16</th>
      <td>5</td>
      <td>1657 Lower Fire Tower Road, SEQUATCHIE, TN, 37374</td>
      <td>58020001, 58020002, 58020003, 58020004, 58020005</td>
    </tr>
    <tr>
      <th>17</th>
      <td>5</td>
      <td>1855 Hiwassee Road, MADISONVILLE, TN, 37354</td>
      <td>62010001, 62010002, 62010003, 62010004, 62010005</td>
    </tr>
    <tr>
      <th>18</th>
      <td>5</td>
      <td>20111 Clarkrange Hwy., MONTEREY, TN, 38574</td>
      <td>71010001, 71010002, 71010003, 71010004, 71010005</td>
    </tr>
    <tr>
      <th>19</th>
      <td>5</td>
      <td>38 Snoddy Road, KELSO, TN, 37348</td>
      <td>52010001, 52010002, 52010003, 52010004, 52010005</td>
    </tr>
    <tr>
      <th>20</th>
      <td>5</td>
      <td>4377 Firetower Rd., COLUMBIA, TN, 38401</td>
      <td>60010001, 60010002, 60010003, 60010004, 60010005</td>
    </tr>
    <tr>
      <th>21</th>
      <td>5</td>
      <td>4766 Hollow Springs Road, BRADYVILLE, TN, 37026</td>
      <td>08010001, 08010002, 08010003, 08020001, 08020002</td>
    </tr>
    <tr>
      <th>22</th>
      <td>5</td>
      <td>6101 Millsfield Highway, DYERSBURG, TN, 38024</td>
      <td>23010001, 23010002, 23010003, 23010004, 23010005</td>
    </tr>
    <tr>
      <th>23</th>
      <td>5</td>
      <td>757 Ridgeview Road, BEAN STATION, TN, 37708</td>
      <td>29010001, 29010002, 29010003, 29010004, 29010005</td>
    </tr>
    <tr>
      <th>24</th>
      <td>5</td>
      <td>774 Mousetail Rd, PARSONS, TN, 38364</td>
      <td>20010001, 20010002, 20010003, 20010004, 20010005</td>
    </tr>
    <tr>
      <th>25</th>
      <td>5</td>
      <td>884 Highway 70 W, LENOIR CITY, TN, 37771</td>
      <td>53010001, 53010002, 53010003, 53010004, 53010005</td>
    </tr>
    <tr>
      <th>26</th>
      <td>5</td>
      <td>927 Firetower Road, MARTIN, TN, 38237</td>
      <td>92010001, 92010002, 92010003, 92010004, 92010005</td>
    </tr>
    <tr>
      <th>27</th>
      <td>5</td>
      <td>962 Old State Route 48, CENTERVILLE, TN, 37033</td>
      <td>41020001, 41020002, 41020003, 41020004, 41020005</td>
    </tr>
    <tr>
      <th>28</th>
      <td>5</td>
      <td>HCR 77 Box 66, ALTAMONT, TN, 37301</td>
      <td>31020001, 31020002, 31020003, 31020004, 31020005</td>
    </tr>
    <tr>
      <th>29</th>
      <td>4</td>
      <td>1084 Westel Road, ROCKWOOD, TN, 37854</td>
      <td>73010001, 73010002, 73010003, 73010004</td>
    </tr>
    <tr>
      <th>30</th>
      <td>4</td>
      <td>1100 Jones Road, HENDERSONVILLE, TN, 37075</td>
      <td>83010001, 83010002, 83010003, 83010004</td>
    </tr>
    <tr>
      <th>31</th>
      <td>4</td>
      <td>121 Pig Ranch Road, GORDONSVILLE, TN, 37030</td>
      <td>80010001, 80010002, 80030001, 80030002</td>
    </tr>
    <tr>
      <th>32</th>
      <td>4</td>
      <td>195 Fire Tower Lane, MORRISON, TN, 37357</td>
      <td>89010001, 89010002, 89010003, 89010004</td>
    </tr>
    <tr>
      <th>33</th>
      <td>4</td>
      <td>2030 Riley Creek Road, WHITLEYVILLE, TN, 38588</td>
      <td>44010001, 44010002, 44030001, 44030002</td>
    </tr>
    <tr>
      <th>34</th>
      <td>4</td>
      <td>220 Eastern Shores Dr., LEXINGTON, TN, 38351</td>
      <td>39050001, 39050002, 39050003, 39050004</td>
    </tr>
    <tr>
      <th>35</th>
      <td>4</td>
      <td>2880 Petway Road, ASHLAND CITY, TN, 37015</td>
      <td>11050001, 11050002, 11050003, 11050004</td>
    </tr>
    <tr>
      <th>36</th>
      <td>4</td>
      <td>3476 Sharps Chapel Road, SHARPS CHAPEL, TN, 37866</td>
      <td>87020001, 87020002, 87020004, 87040001</td>
    </tr>
    <tr>
      <th>37</th>
      <td>4</td>
      <td>395 Firetower Lane, HEISKELL, TN, 37754</td>
      <td>01010001, 01010002, 01010003, 01010004</td>
    </tr>
    <tr>
      <th>38</th>
      <td>4</td>
      <td>3998 Game Reserve Road, CHATTANOOGA, TN, 37405</td>
      <td>58030001, 58030002, 58030004, 58030005</td>
    </tr>
    <tr>
      <th>39</th>
      <td>4</td>
      <td>423 Fire Tower Drive, DUNLAP, TN, 37327</td>
      <td>77020001, 77020002, 77020003, 77020004</td>
    </tr>
    <tr>
      <th>40</th>
      <td>4</td>
      <td>444 Fox Hunter Road, MAYNARDVILLE, TN, 37807</td>
      <td>87010001, 87010002, 87010003, 87010004</td>
    </tr>
    <tr>
      <th>41</th>
      <td>4</td>
      <td>460 Firetower Road, NEW MARKET, TN, 37820</td>
      <td>45040001, 45040002, 45040003, 45040004</td>
    </tr>
    <tr>
      <th>42</th>
      <td>4</td>
      <td>490 Fire Tower Rd, LINDEN, TN, 37096</td>
      <td>68020001, 68020002, 68020004, 68020005</td>
    </tr>
    <tr>
      <th>43</th>
      <td>4</td>
      <td>5098 U.S. 127, SIGNAL MOUNTAIN, TN, 37377</td>
      <td>77040001, 77040002, 77040003, 77040004</td>
    </tr>
    <tr>
      <th>44</th>
      <td>4</td>
      <td>7691 Pewitt Road, FRANKLIN, TN, 37064</td>
      <td>94010001, 94010002, 94010003, 94010004</td>
    </tr>
    <tr>
      <th>45</th>
      <td>4</td>
      <td>Cabo Rd., ENVILLE, TN, 38332</td>
      <td>12010001, 12010002, 12010003, 12010004</td>
    </tr>
    <tr>
      <th>46</th>
      <td>3</td>
      <td>100 Clinch Ridge Rd, THORN HILL, TN, 37881</td>
      <td>37020001, 37020002, 37020003</td>
    </tr>
    <tr>
      <th>47</th>
      <td>3</td>
      <td>115 Fire Tower Rd, SELMER, TN, 38375</td>
      <td>55010001, 55010002, 55010003</td>
    </tr>
    <tr>
      <th>48</th>
      <td>3</td>
      <td>1336 County Road 875, ETOWAH, TN, 37331</td>
      <td>54010001, 54010002, 54010003</td>
    </tr>
    <tr>
      <th>49</th>
      <td>3</td>
      <td>1342 Hurricane Bridge Rd., SMITHVILLE, TN, 37166</td>
      <td>21010001, 21010002, 21010003</td>
    </tr>
  </tbody>
</table>
</div>


    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved ZIP conflicts CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_zip_conflicts.csv



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
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>postal_codes</th>
      <th>building_count</th>
      <th>building_codes</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>2643 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37643, 37658</td>
      <td>2</td>
      <td>10010003, 10010004</td>
    </tr>
    <tr>
      <th>1</th>
      <td>400 Firetower Ln.</td>
      <td>LIVINGSTON</td>
      <td>TN</td>
      <td>38570, 38571</td>
      <td>11</td>
      <td>67010001, 67010002, 67010003, 67010004, 670100...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1485 Firetower Rd</td>
      <td>SAVANNAH</td>
      <td>TN</td>
      <td>38367, 38372</td>
      <td>4</td>
      <td>36020001, 36020002, 36020003, 36020004</td>
    </tr>
  </tbody>
</table>
</div>


    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved potential typo review CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_potential_typos.csv



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
      <th>building_code</th>
      <th>building_name</th>
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>potential_typo_reason</th>
      <th>full_address</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>63000030</td>
      <td>Cunningham Office</td>
      <td>4275 Louise Road</td>
      <td>CUMBERLAND FUR.</td>
      <td>TN</td>
      <td>37051</td>
      <td>verify_city_abbreviation_cumberland_fur</td>
      <td>4275 Louise Road, CUMBERLAND FUR., TN, 37051</td>
    </tr>
    <tr>
      <th>1</th>
      <td>63000031</td>
      <td>Cunningham Lookout Tower</td>
      <td>4275 Louise Road</td>
      <td>CUMBERLAND FUR.</td>
      <td>TN</td>
      <td>37051</td>
      <td>verify_city_abbreviation_cumberland_fur</td>
      <td>4275 Louise Road, CUMBERLAND FUR., TN, 37051</td>
    </tr>
    <tr>
      <th>2</th>
      <td>95000007</td>
      <td>Cedars Storage Shed</td>
      <td>5112 Murfressboro Rd.</td>
      <td>LEBANON</td>
      <td>TN</td>
      <td>37090</td>
      <td>verify_street_spelling_murfressboro</td>
      <td>5112 Murfressboro Rd., LEBANON, TN, 37090</td>
    </tr>
    <tr>
      <th>3</th>
      <td>55040001</td>
      <td>Ramer Fire Tower Storage Shed</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>verify_street_spelling_sulpher</td>
      <td>9001 Sulpher Springs Rd., RAMER, TN, 38367</td>
    </tr>
    <tr>
      <th>4</th>
      <td>55040002</td>
      <td>Ramer Fire Tower</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>verify_street_spelling_sulpher</td>
      <td>9001 Sulpher Springs Rd., RAMER, TN, 38367</td>
    </tr>
    <tr>
      <th>5</th>
      <td>55040003</td>
      <td>Ramer Fire Tower Crew Shed</td>
      <td>9001 Sulpher Springs Rd.</td>
      <td>RAMER</td>
      <td>TN</td>
      <td>38367</td>
      <td>verify_street_spelling_sulpher</td>
      <td>9001 Sulpher Springs Rd., RAMER, TN, 38367</td>
    </tr>
  </tbody>
</table>
</div>


    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved square feet review CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_square_feet_review.csv



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
      <th>building_code</th>
      <th>building_name</th>
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>square_feet_raw</th>
      <th>square_feet</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>03010004</td>
      <td>Camden Storage Building</td>
      <td>625 Fire Tower Rd</td>
      <td>CAMDEN</td>
      <td>TN</td>
      <td>38320</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>04010016</td>
      <td>Bledsoe SF Sawmill Residence</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>09010003</td>
      <td>Leach Storage Building #3</td>
      <td>1745 Leach Road</td>
      <td>CEDAR GROVE</td>
      <td>TN</td>
      <td>38321</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010004</td>
      <td>Carter County Outbuilding #1</td>
      <td>2643 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37643</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>4</th>
      <td>11010001</td>
      <td>Sycamore Crew Cabin</td>
      <td>1106 Bryant Road</td>
      <td>ASHLAND CITY</td>
      <td>TN</td>
      <td>37015</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>5</th>
      <td>11010002</td>
      <td>Sycamore Fire Tower</td>
      <td>1106 Bryant Road</td>
      <td>ASHLAND CITY</td>
      <td>TN</td>
      <td>37015</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>11050001</td>
      <td>Petway Cabin</td>
      <td>2880 Petway Road</td>
      <td>ASHLAND CITY</td>
      <td>TN</td>
      <td>37015</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>11050003</td>
      <td>Petway House</td>
      <td>2880 Petway Road</td>
      <td>ASHLAND CITY</td>
      <td>TN</td>
      <td>37015</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>12010002</td>
      <td>Jacks Creek Fire Tower Quonset Hut</td>
      <td>Cabo Rd.</td>
      <td>ENVILLE</td>
      <td>TN</td>
      <td>38332</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>9</th>
      <td>12010004</td>
      <td>Jacks Creek Fire Tower Crew Shed</td>
      <td>Cabo Rd.</td>
      <td>ENVILLE</td>
      <td>TN</td>
      <td>38332</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>10</th>
      <td>15010002</td>
      <td>English Mtn Radio Tower Building</td>
      <td>1400 Carson Springs Road</td>
      <td>NEWPORT</td>
      <td>TN</td>
      <td>37821</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>11</th>
      <td>19000098</td>
      <td>Forestry Radio Warehouse</td>
      <td>1134 Menzler Road</td>
      <td>NASHVILLE</td>
      <td>TN</td>
      <td>37210</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>12</th>
      <td>19010027</td>
      <td>Nashville Main Forestry Office, Bruer Building</td>
      <td>406 Hogan Road</td>
      <td>NASHVILLE</td>
      <td>TN</td>
      <td>37220</td>
      <td></td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>13</th>
      <td>19010028</td>
      <td>Porter Building</td>
      <td>436 Hogan Road</td>
      <td>NASHVILLE</td>
      <td>TN</td>
      <td>37220</td>
      <td></td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>14</th>
      <td>20010003</td>
      <td>Parsons Firetower Shed #1</td>
      <td>774 Mousetail Rd</td>
      <td>PARSONS</td>
      <td>TN</td>
      <td>38364</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>15</th>
      <td>23010002</td>
      <td>Millsfield Office</td>
      <td>6101 Millsfield Highway</td>
      <td>DYERSBURG</td>
      <td>TN</td>
      <td>38024</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>16</th>
      <td>23010004</td>
      <td>Millsfield Radio Building</td>
      <td>6101 Millsfield Highway</td>
      <td>DYERSBURG</td>
      <td>TN</td>
      <td>38024</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>17</th>
      <td>23010005</td>
      <td>Millsfield Radio Tower</td>
      <td>6101 Millsfield Highway</td>
      <td>DYERSBURG</td>
      <td>TN</td>
      <td>38024</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>18</th>
      <td>24010002</td>
      <td>Williston Fire Tower Residence</td>
      <td>3570 Ebenezer Loop</td>
      <td>SOMERVILLE</td>
      <td>TN</td>
      <td>38068</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>19</th>
      <td>24010003</td>
      <td>Williston Fire Tower Radio Repeater Building</td>
      <td>3570 Ebenezer Loop</td>
      <td>SOMERVILLE</td>
      <td>TN</td>
      <td>38068</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>20</th>
      <td>25080001</td>
      <td>Wolf River Forestry Building</td>
      <td>Unidentified</td>
      <td>JAMESTOWN</td>
      <td>TN</td>
      <td>38556</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>21</th>
      <td>25080002</td>
      <td>Wolf River Tower</td>
      <td>Unidentified</td>
      <td>JAMESTOWN</td>
      <td>TN</td>
      <td>38556</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>22</th>
      <td>28000011</td>
      <td>Brooklyn Plaza Area Office</td>
      <td>57 Firetower Road</td>
      <td>PULASKI</td>
      <td>TN</td>
      <td>38478</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>23</th>
      <td>33030001</td>
      <td>Montlake Shop/Office</td>
      <td>1327 Montlake Road</td>
      <td>SODDY DAISY</td>
      <td>TN</td>
      <td>37379</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>24</th>
      <td>33080001</td>
      <td>District 3 Dispatch Office</td>
      <td>5530 Hwy 153</td>
      <td>HIXSON</td>
      <td>TN</td>
      <td>37343</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>25</th>
      <td>33080002</td>
      <td>District 3 Office</td>
      <td>5530 Hwy 153</td>
      <td>HIXSON</td>
      <td>TN</td>
      <td>37343</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>26</th>
      <td>35000003</td>
      <td>Whiteville Radio Tower</td>
      <td>134 West Main Street</td>
      <td>WHITEVILLE</td>
      <td>TN</td>
      <td>38075</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>27</th>
      <td>35040001</td>
      <td>Hickory Valley Crew Shed</td>
      <td>185 Buster Woods Road</td>
      <td>HICKORY VALLEY</td>
      <td>TN</td>
      <td>38042</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>28</th>
      <td>35050003</td>
      <td>Hornsby Office</td>
      <td>435 Fire Tower Lane</td>
      <td>BOLIVAR</td>
      <td>TN</td>
      <td>38008</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>29</th>
      <td>36030001</td>
      <td>Savannah Radio Tower &amp; Building</td>
      <td>1680 Irvin Rd</td>
      <td>SAVANNAH</td>
      <td>TN</td>
      <td>38372</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>30</th>
      <td>36030002</td>
      <td>Savannah Office</td>
      <td>1680 Irvin Rd</td>
      <td>SAVANNAH</td>
      <td>TN</td>
      <td>38372</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>31</th>
      <td>36040001</td>
      <td>Pickwick Firetower</td>
      <td>339 Turkey Knob Ln.</td>
      <td>COUNCE</td>
      <td>TN</td>
      <td>38326</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>32</th>
      <td>36040002</td>
      <td>Pickwick Office</td>
      <td>339 Turkey Knob Ln.</td>
      <td>COUNCE</td>
      <td>TN</td>
      <td>38326</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>33</th>
      <td>36040003</td>
      <td>Pickwick Shop</td>
      <td>339 Turkey Knob Ln.</td>
      <td>COUNCE</td>
      <td>TN</td>
      <td>38326</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>34</th>
      <td>37020001</td>
      <td>Mooresburg Building</td>
      <td>100 Clinch Ridge Rd</td>
      <td>THORN HILL</td>
      <td>TN</td>
      <td>37881</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>35</th>
      <td>39040001</td>
      <td>Natchez Trace State Forest Office</td>
      <td>22117 Natchez Trace Rd</td>
      <td>WILDERSVILLE</td>
      <td>TN</td>
      <td>38388</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>36</th>
      <td>39040002</td>
      <td>Natchez Trace State Forest Equipment Shed</td>
      <td>22117 Natchez Trace Rd</td>
      <td>WILDERSVILLE</td>
      <td>TN</td>
      <td>38388</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>37</th>
      <td>39040003</td>
      <td>Natchez Trace State Forest Tractor Shed</td>
      <td>22117 Natchez Trace Rd</td>
      <td>WILDERSVILLE</td>
      <td>TN</td>
      <td>38388</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>38</th>
      <td>39060002</td>
      <td>Natchez Trace Storage Shed #2</td>
      <td>22117 Natchez Trace Rd</td>
      <td>WILDERSVILLE</td>
      <td>TN</td>
      <td>38388</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>39</th>
      <td>40010002</td>
      <td>Paris Storage Building</td>
      <td>195 Macedonia Road</td>
      <td>HENRY</td>
      <td>TN</td>
      <td>38231</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>40</th>
      <td>40010004</td>
      <td>Paris Office</td>
      <td>195 Macedonia Road</td>
      <td>HENRY</td>
      <td>TN</td>
      <td>38231</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>41</th>
      <td>44030002</td>
      <td>Haydenburg Tower House</td>
      <td>2030 Riley Creek Road</td>
      <td>WHITLEYVILLE</td>
      <td>TN</td>
      <td>38588</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>42</th>
      <td>45040001</td>
      <td>Jefferson County Storage Building</td>
      <td>460 Firetower Road</td>
      <td>NEW MARKET</td>
      <td>TN</td>
      <td>37820</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>43</th>
      <td>45040002</td>
      <td>Jefferson County 911 Radio System</td>
      <td>460 Firetower Road</td>
      <td>NEW MARKET</td>
      <td>TN</td>
      <td>37820</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>44</th>
      <td>45040003</td>
      <td>Jefferson County Ag Crime Unit Office</td>
      <td>460 Firetower Road</td>
      <td>NEW MARKET</td>
      <td>TN</td>
      <td>37820</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>45</th>
      <td>45040004</td>
      <td>Jefferson County Fire Tower</td>
      <td>460 Firetower Road</td>
      <td>NEW MARKET</td>
      <td>TN</td>
      <td>37820</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>46</th>
      <td>47000013</td>
      <td>DOA Regulatory Services, Solar House</td>
      <td>3211 Alcoa Hwy, Solar House #3</td>
      <td>KNOXVILLE</td>
      <td>TN</td>
      <td>37920</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>47</th>
      <td>49010001</td>
      <td>Ripley Fire Tower Shop</td>
      <td>247 Joe Critchfield Rd.</td>
      <td>KNOXVILLE</td>
      <td>TN</td>
      <td>38063</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>48</th>
      <td>50070001</td>
      <td>Westpoint Office</td>
      <td>Westpoint Road</td>
      <td>LAWRENCEBURG</td>
      <td>TN</td>
      <td>38486</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
    <tr>
      <th>49</th>
      <td>50070002</td>
      <td>Westpoint Fire Tower</td>
      <td>Westpoint Road</td>
      <td>LAWRENCEBURG</td>
      <td>TN</td>
      <td>38486</td>
      <td>0</td>
      <td>0.0</td>
    </tr>
  </tbody>
</table>
</div>


    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved normalized CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_normalized.csv
    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Saved QA review workbook: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_qa_review.xlsx


## 6. Manual override control file

Use `tdf_buildings_manual_overrides.csv` as the editable correction layer. The notebook creates/refreses this file with one row per `building_code`, preserves reviewer-entered override values, and applies those overrides before QA/geocoding or before publishing.

Editable columns:

- `override_address`, `override_city`, `override_state`, `override_postal_code`, `override_county`: replace source address components for geocoding and publishing.
- `latitude`, `longitude`: manually verified WGS 84 coordinates. A valid coordinate override bypasses geocoder-quality blockers.
- `accept_geocode_candidate`: set to `TRUE` only after visually verifying the geocoder result; this allows a flagged geocoder candidate to publish.
- `exclude_from_publish`: set to `TRUE` to intentionally keep a record out of the hosted layer.
- `override_notes`, `override_source`: reviewer notes and source of the decision.

Reference columns such as `building_name` and `source_address` are refreshed from the source workbook. Do not use them for edits.



```python
manual_override_display_cols = [
    'building_code', 'building_name',
    'source_address', 'source_city', 'source_state', 'source_postal_code',
    'override_address', 'override_city', 'override_state', 'override_postal_code',
    'latitude', 'longitude', 'accept_geocode_candidate', 'exclude_from_publish',
    'override_notes', 'override_source',
]

print(f'Manual override file: {MANUAL_OVERRIDES_CSV}')
print('Edit this file for manual corrections. The notebook will preserve editable override columns on reruns.')

manual_override_work_queue = df.loc[
    df['manual_location_review_needed']
    | df['potential_typo_review_needed']
    | df['zip_conflict_for_address_city']
    | df['manual_coordinate_invalid']
    | df['has_any_manual_override'],
    manual_override_display_cols + [
        'full_address', 'manual_location_review_reason', 'potential_typo_reason',
        'zip_conflict_for_address_city', 'manual_coordinate_valid', 'manual_coordinate_invalid',
    ],
].copy()

logger.info('Manual override file rows: %s', len(manual_overrides))
logger.info('Manual override current QA/edit queue rows: %s', len(manual_override_work_queue))
display(manual_override_work_queue.head(100))

```

    Manual override file: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_manual_overrides.csv
    Edit this file for manual corrections. The notebook will preserve editable override columns on reruns.
    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Manual override file rows: 463
    2026-07-08 19:25:08 | INFO | tdf_buildings_pipeline | Manual override current QA/edit queue rows: 97



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
      <th>building_code</th>
      <th>building_name</th>
      <th>source_address</th>
      <th>source_city</th>
      <th>source_state</th>
      <th>source_postal_code</th>
      <th>override_address</th>
      <th>override_city</th>
      <th>override_state</th>
      <th>override_postal_code</th>
      <th>...</th>
      <th>accept_geocode_candidate</th>
      <th>exclude_from_publish</th>
      <th>override_notes</th>
      <th>override_source</th>
      <th>full_address</th>
      <th>manual_location_review_reason</th>
      <th>potential_typo_reason</th>
      <th>zip_conflict_for_address_city</th>
      <th>manual_coordinate_valid</th>
      <th>manual_coordinate_invalid</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10000006</td>
      <td>Holston Mtn Radio Building</td>
      <td>National Forest 56</td>
      <td>ELIZABETHTON</td>
      <td>TN</td>
      <td>37643</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>National Forest 56, ELIZABETHTON, TN, 37643</td>
      <td>address_does_not_start_with_site_number</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010003</td>
      <td>Carter County Shop</td>
      <td>2643 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>TN</td>
      <td>37658</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>2643 Gap Creek Road, HAMPTON, TN, 37658</td>
      <td></td>
      <td></td>
      <td>True</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>12</th>
      <td>04000007</td>
      <td>Hendon Fire Tower</td>
      <td>Fire Tower Road</td>
      <td>GRAYSVILLE</td>
      <td>TN</td>
      <td>37338</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Fire Tower Road, GRAYSVILLE, TN, 37338</td>
      <td>address_does_not_start_with_site_number</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>13</th>
      <td>04010001</td>
      <td>Bledsoe SF Smoke House</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Route 3 Box 180, PIKEVILLE, TN, 37367</td>
      <td>legacy_route_or_box_address; address_does_not_...</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>14</th>
      <td>04010002</td>
      <td>Bledsoe SF Storm Shelter</td>
      <td>Route 3 Box 180</td>
      <td>PIKEVILLE</td>
      <td>TN</td>
      <td>37367</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Route 3 Box 180, PIKEVILLE, TN, 37367</td>
      <td>legacy_route_or_box_address; address_does_not_...</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>407</th>
      <td>81000003</td>
      <td>Lookout Tower (Model) LBL</td>
      <td>Ginger Bay Road</td>
      <td>DOVER</td>
      <td>TN</td>
      <td>37058</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Ginger Bay Road, DOVER, TN, 37058</td>
      <td>address_does_not_start_with_site_number</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>412</th>
      <td>81020001</td>
      <td>Model Storage Building (LBL)</td>
      <td>Fire Tower Road</td>
      <td>DOVER</td>
      <td>TN</td>
      <td>37058</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Fire Tower Road, DOVER, TN, 37058</td>
      <td>address_does_not_start_with_site_number</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>413</th>
      <td>81020002</td>
      <td>Model Shop And Office</td>
      <td>Fire Tower Road</td>
      <td>DOVER</td>
      <td>TN</td>
      <td>37058</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Fire Tower Road, DOVER, TN, 37058</td>
      <td>address_does_not_start_with_site_number</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>461</th>
      <td>95000006</td>
      <td>Lebanon Tower</td>
      <td>Dead End of Firetower Rd.</td>
      <td>LEBANON</td>
      <td>TN</td>
      <td>37090</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>Dead End of Firetower Rd., LEBANON, TN, 37090</td>
      <td>address_does_not_start_with_site_number</td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
    <tr>
      <th>462</th>
      <td>95000007</td>
      <td>Cedars Storage Shed</td>
      <td>5112 Murfressboro Rd.</td>
      <td>LEBANON</td>
      <td>TN</td>
      <td>37090</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>...</td>
      <td></td>
      <td></td>
      <td></td>
      <td></td>
      <td>5112 Murfressboro Rd., LEBANON, TN, 37090</td>
      <td></td>
      <td>verify_street_spelling_murfressboro</td>
      <td>False</td>
      <td>False</td>
      <td>False</td>
    </tr>
  </tbody>
</table>
<p>97 rows × 22 columns</p>
</div>


## 7. Geocode unique addresses

This section geocodes unique effective address records only, then joins the results back to individual buildings. Existing results are read from `tdf_buildings_geocode_cache.csv` and only uncached addresses are submitted to the locator.

Address overrides in `tdf_buildings_manual_overrides.csv` are applied before this step, so corrected address values create new address keys and can be geocoded without changing the raw workbook. Records with valid manual coordinates or `exclude_from_publish = TRUE` are not submitted to the locator.

This section displays a cache-status table. If `run_geocoding = False` and no current-version cache rows are found, downstream geocoded outputs will contain no coordinates. Set `run_geocoding = True` to rebuild the cache.



```python
def coerce_positive_int(value):
    try:
        value = int(value)
    except (TypeError, ValueError):
        return None
    return value if value > 0 else None

def get_locator_batch_limits(geocoder):
    locator_props = getattr(geocoder.properties, 'locatorProperties', None)
    if not locator_props:
        return None, None
    suggested = coerce_positive_int(getattr(locator_props, 'SuggestedBatchSize', None))
    maximum = coerce_positive_int(getattr(locator_props, 'MaxBatchSize', None))
    return suggested, maximum

def effective_geocode_batch_size():
    requested = coerce_positive_int(CONFIG.geocode_batch_size)
    if requested:
        size = requested
    elif LOCATOR_SUGGESTED_BATCH_SIZE:
        size = LOCATOR_SUGGESTED_BATCH_SIZE
    else:
        size = 100

    if LOCATOR_MAX_BATCH_SIZE and size > LOCATOR_MAX_BATCH_SIZE:
        size = LOCATOR_MAX_BATCH_SIZE
    return size

geocoders = get_geocoders(gis)
if not geocoders:
    raise RuntimeError('No geocoders are configured for this GIS.')

geocoder = geocoders[0]
LOCATOR_SUGGESTED_BATCH_SIZE, LOCATOR_MAX_BATCH_SIZE = get_locator_batch_limits(geocoder)

logger.info('Using geocoder: %s', geocoder.properties.get('name', geocoder.url))
logger.info('SuggestedBatchSize: %s', LOCATOR_SUGGESTED_BATCH_SIZE or 'not reported')
logger.info('MaxBatchSize: %s', LOCATOR_MAX_BATCH_SIZE or 'not reported')
logger.info('Effective batch size: %s', effective_geocode_batch_size())
logger.info('Geocode mode: %s', CONFIG.geocode_mode)

```

    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Using geocoder: https://geocode.arcgis.com/arcgis/rest/services/World/GeocodeServer
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | SuggestedBatchSize: 150
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | MaxBatchSize: 1000
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Effective batch size: 150
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Geocode mode: single



```python
GEOCODE_CACHE_COLUMNS = [
    'cache_version', 'address_key', 'input_address', 'input_city', 'input_state',
    'input_postal_code', 'full_address', 'longitude', 'latitude', 'score',
    'match_address', 'addr_type', 'geocode_status', 'geocode_source',
    'geocode_run_utc', 'raw_result'
]

# If force_regeocode is True and run_geocoding is True, the previous cache is ignored
# and overwritten with new validated rows. When run_geocoding is False, the current
# versioned cache can still be loaded for downstream review or publishing.
ignore_existing_cache = bool(CONFIG.force_regeocode and CONFIG.run_geocoding)

if GEOCODE_CACHE_CSV.exists() and not ignore_existing_cache:
    geocode_cache = pd.read_csv(
        GEOCODE_CACHE_CSV,
        dtype={
            'cache_version': 'string',
            'address_key': 'string',
            'input_address': 'string',
            'input_city': 'string',
            'input_state': 'string',
            'input_postal_code': 'string',
            'full_address': 'string',
            'match_address': 'string',
            'addr_type': 'string',
            'geocode_status': 'string',
            'geocode_source': 'string',
            'geocode_run_utc': 'string',
            'raw_result': 'string',
        },
    )
    for col in GEOCODE_CACHE_COLUMNS:
        if col not in geocode_cache.columns:
            geocode_cache[col] = ''
    geocode_cache = geocode_cache[GEOCODE_CACHE_COLUMNS].copy()
    geocode_cache = geocode_cache.loc[
        geocode_cache['cache_version'].fillna('').eq(CONFIG.geocode_cache_version)
    ].copy()
    logger.info('Loaded versioned geocode cache rows: %s', len(geocode_cache))
elif GEOCODE_CACHE_CSV.exists() and ignore_existing_cache:
    logger.warning(
        'Ignoring existing geocode cache for this run because CONFIG.force_regeocode is True. '
        'The cache will be rebuilt if CONFIG.run_geocoding is True.'
    )
    geocode_cache = pd.DataFrame(columns=GEOCODE_CACHE_COLUMNS)
else:
    geocode_cache = pd.DataFrame(columns=GEOCODE_CACHE_COLUMNS)

# Do not spend geocoding credits on known placeholder/PO Box addresses, valid manual coordinates,
# or records that are intentionally excluded from publishing.
geocode_eligible_mask = (
    ~df['is_placeholder_address']
    & ~df['is_po_box_address']
    & ~df['manual_coordinate_valid']
    & ~df['exclude_from_publish_override']
)
unique_addresses = (
    df.loc[
        geocode_eligible_mask,
        ['address_key', 'address', 'city', 'state', 'postal_code', 'full_address']
    ]
    .drop_duplicates('address_key')
    .sort_values('full_address')
    .reset_index(drop=True)
)

cached_keys = set(geocode_cache['address_key'].dropna()) if not geocode_cache.empty else set()
to_geocode = unique_addresses.loc[~unique_addresses['address_key'].isin(cached_keys)].copy()

logger.info('Geocode cache version: %s', CONFIG.geocode_cache_version)
logger.info('Force regeocode: %s', CONFIG.force_regeocode)
logger.info('Unique geocode-eligible addresses: %s', len(unique_addresses))
logger.info('Already cached with current version: %s', len(unique_addresses) - len(to_geocode))
logger.info('Remaining to geocode: %s', len(to_geocode))

geocode_cache_status = pd.DataFrame([
    {'metric': 'run_geocoding', 'value': CONFIG.run_geocoding},
    {'metric': 'run_publish', 'value': CONFIG.run_publish},
    {'metric': 'geocode_cache_file_exists', 'value': GEOCODE_CACHE_CSV.exists()},
    {'metric': 'current_version_cache_rows', 'value': len(geocode_cache)},
    {'metric': 'records_skipped_due_valid_manual_coordinates', 'value': int(df['manual_coordinate_valid'].sum())},
    {'metric': 'records_skipped_due_manual_exclusion', 'value': int(df['exclude_from_publish_override'].sum())},
    {'metric': 'unique_geocode_eligible_addresses', 'value': len(unique_addresses)},
    {'metric': 'addresses_still_needing_geocoding', 'value': len(to_geocode)},
    {'metric': 'force_regeocode', 'value': CONFIG.force_regeocode},
    {'metric': 'geocode_cache_version', 'value': CONFIG.geocode_cache_version},
])
display(geocode_cache_status)

if not CONFIG.run_geocoding and len(geocode_cache) == 0 and len(unique_addresses) > 0:
    logger.warning(
        'No current-version geocode cache rows were loaded. This is expected for a QA-only run, '
        'but geocoded outputs will contain no coordinates until CONFIG.run_geocoding is True '
        'or a valid tdf_buildings_geocode_cache.csv exists in the output folder.'
    )

if CONFIG.run_publish and not CONFIG.run_geocoding and len(geocode_cache) == 0 and len(unique_addresses) > 0:
    raise RuntimeError(
        'Publishing stopped before geocoding. CONFIG.run_publish is True, CONFIG.run_geocoding is False, '
        'and no current-version geocode cache rows were loaded. Set run_geocoding=True or provide a valid cache.'
    )

display(to_geocode.head(20))

```

    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Loaded versioned geocode cache rows: 166
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Geocode cache version: v2_result_validation
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Force regeocode: False
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Unique geocode-eligible addresses: 164
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Already cached with current version: 164
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Remaining to geocode: 0



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
      <th>metric</th>
      <th>value</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>run_geocoding</td>
      <td>False</td>
    </tr>
    <tr>
      <th>1</th>
      <td>run_publish</td>
      <td>True</td>
    </tr>
    <tr>
      <th>2</th>
      <td>geocode_cache_file_exists</td>
      <td>True</td>
    </tr>
    <tr>
      <th>3</th>
      <td>current_version_cache_rows</td>
      <td>166</td>
    </tr>
    <tr>
      <th>4</th>
      <td>records_skipped_due_valid_manual_coordinates</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>records_skipped_due_manual_exclusion</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>unique_geocode_eligible_addresses</td>
      <td>164</td>
    </tr>
    <tr>
      <th>7</th>
      <td>addresses_still_needing_geocoding</td>
      <td>0</td>
    </tr>
    <tr>
      <th>8</th>
      <td>force_regeocode</td>
      <td>False</td>
    </tr>
    <tr>
      <th>9</th>
      <td>geocode_cache_version</td>
      <td>v2_result_validation</td>
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
      <th>address_key</th>
      <th>address</th>
      <th>city</th>
      <th>state</th>
      <th>postal_code</th>
      <th>full_address</th>
    </tr>
  </thead>
  <tbody>
  </tbody>
</table>
</div>



```python
def first_nonempty(*values):
    for value in values:
        if value is not None and value != '':
            return value
    return None

def make_geocode_address(row, object_id=None):
    payload = {
        'Address': row['address'],
        'City': row['city'],
        'Region': row['state'],
        'Postal': row['postal_code'],
    }
    if object_id is not None:
        payload['OBJECTID'] = int(object_id)
    return payload

def cache_context_from_row(row):
    return {
        'cache_version': CONFIG.geocode_cache_version,
        'address_key': row['address_key'],
        'input_address': row['address'],
        'input_city': row['city'],
        'input_state': row['state'],
        'input_postal_code': row['postal_code'],
        'full_address': row['full_address'],
    }

def parse_geocode_result(row, result, source, run_time):
    base = cache_context_from_row(row)

    if not result:
        return {
            **base,
            'longitude': None,
            'latitude': None,
            'score': 0,
            'match_address': '',
            'addr_type': '',
            'geocode_status': 'no_match',
            'geocode_source': source,
            'geocode_run_utc': run_time,
            'raw_result': '',
        }

    attrs = result.get('attributes', {}) or {}
    location = result.get('location', {}) or {}
    score = first_nonempty(result.get('score'), attrs.get('Score'))
    longitude = first_nonempty(location.get('x'), attrs.get('X'), attrs.get('DisplayX'))
    latitude = first_nonempty(location.get('y'), attrs.get('Y'), attrs.get('DisplayY'))
    match_address = first_nonempty(result.get('address'), attrs.get('Match_addr'), attrs.get('LongLabel'), '')
    addr_type = first_nonempty(attrs.get('Addr_type'), attrs.get('Type'), '')

    status = attrs.get('Status')
    if longitude is None or latitude is None or pd.isna(score) or float(score or 0) <= 0:
        geocode_status = 'no_match'
    elif status:
        geocode_status = f'matched_{status}'
    else:
        geocode_status = 'matched'

    return {
        **base,
        'longitude': longitude,
        'latitude': latitude,
        'score': score,
        'match_address': match_address,
        'addr_type': addr_type,
        'geocode_status': geocode_status,
        'geocode_source': source,
        'geocode_run_utc': run_time,
        'raw_result': json.dumps(result, default=str),
    }

def geocode_one_address(row):
    run_time = datetime.now(timezone.utc).isoformat()

    try:
        candidates = geocode(
            address=make_geocode_address(row),
            max_locations=1,
            out_fields='*',
            for_storage=True,
            source_country=CONFIG.source_country,
            category=CONFIG.geocode_category,
            out_sr={'wkid': 4326},
            location_type=CONFIG.geocode_location_type,
            geocoder=geocoder,
        )
    except Exception as exc:
        return {
            **cache_context_from_row(row),
            'longitude': None,
            'latitude': None,
            'score': None,
            'match_address': '',
            'addr_type': '',
            'geocode_status': f'error: {exc}',
            'geocode_source': 'arcgis.geocoding.geocode',
            'geocode_run_utc': run_time,
            'raw_result': '',
        }

    best = candidates[0] if candidates else None
    return parse_geocode_result(row, best, 'arcgis.geocoding.geocode', run_time)

def geocode_single_sequence(rows_df):
    results = []
    total = len(rows_df)
    for i, (_, row) in enumerate(rows_df.iterrows(), start=1):
        if i == 1 or i % 25 == 0 or i == total:
            logger.info('Geocoding %s of %s: %s', i, total, row['full_address'])
        results.append(geocode_one_address(row))
        time.sleep(CONFIG.geocode_delay_seconds)
    return results

def extract_result_id(result):
    attrs = result.get('attributes', {}) or {}
    for key in ('ResultID', 'OBJECTID', 'ObjectID', 'InputID'):
        value = result.get(key, attrs.get(key))
        if value is not None and str(value).strip() != '':
            try:
                return int(value)
            except (TypeError, ValueError):
                return None
    return None

def normalize_batch_results(batch_results):
    if hasattr(batch_results, 'features'):
        return [
            {
                'address': feature.attributes.get('Match_addr', ''),
                'location': feature.geometry,
                'score': feature.attributes.get('Score'),
                'attributes': feature.attributes,
            }
            for feature in batch_results.features
        ]
    return batch_results

def geocode_batch(rows_df):
    """Batch geocode with ResultID/OBJECTID mapping.

    This intentionally avoids assigning results back to input rows by list position.
    If returned results cannot be safely mapped by ResultID or OBJECTID, the function
    falls back to single-address requests for the batch.
    """
    run_time = datetime.now(timezone.utc).isoformat()
    rows_by_result_id = {}
    address_payload = []

    for result_id, (_, row) in enumerate(rows_df.iterrows()):
        rows_by_result_id[result_id] = row
        address_payload.append(make_geocode_address(row, object_id=result_id))

    try:
        batch_results = batch_geocode(
            addresses=address_payload,
            source_country=CONFIG.source_country,
            category=CONFIG.geocode_category,
            out_sr={'wkid': 4326},
            geocoder=geocoder,
            as_featureset=False,
            match_out_of_range=True,
            location_type=CONFIG.geocode_location_type,
            out_fields='*',
        )
    except Exception as exc:
        logger.warning(
            'Batch geocode failed for %s addresses. Falling back to single-address requests. Error: %s',
            len(rows_df), exc,
        )
        return geocode_single_sequence(rows_df)

    batch_results = normalize_batch_results(batch_results)
    if not isinstance(batch_results, list):
        logger.warning('Batch geocode returned a non-list result. Falling back to single-address requests for this batch.')
        return geocode_single_sequence(rows_df)

    parsed_results = []
    seen_result_ids = set()
    missing_or_unmapped = 0

    for result in batch_results:
        result_id = extract_result_id(result)
        if result_id is None or result_id not in rows_by_result_id or result_id in seen_result_ids:
            missing_or_unmapped += 1
            continue
        seen_result_ids.add(result_id)
        parsed_results.append(
            parse_geocode_result(
                rows_by_result_id[result_id],
                result,
                'arcgis.geocoding.batch_geocode_resultid',
                run_time,
            )
        )

    if missing_or_unmapped or len(seen_result_ids) != len(rows_by_result_id):
        logger.warning(
            'Batch geocode results could not be safely mapped by ResultID/OBJECTID '
            '(%s unmapped; %s mapped of %s input rows). Falling back to single-address requests.',
            missing_or_unmapped, len(seen_result_ids), len(rows_by_result_id),
        )
        return geocode_single_sequence(rows_df)

    return parsed_results

if CONFIG.run_geocoding:
    new_results = []
    total = len(to_geocode)

    if total == 0:
        logger.info('No uncached addresses need geocoding.')
    elif CONFIG.geocode_mode == 'single':
        new_results.extend(geocode_single_sequence(to_geocode))
    elif CONFIG.geocode_mode == 'batch':
        batch_size = effective_geocode_batch_size()
        for start in range(0, total, batch_size):
            stop = min(start + batch_size, total)
            batch_df = to_geocode.iloc[start:stop].copy()
            logger.info('Batch geocoding records %s-%s of %s', start + 1, stop, total)
            new_results.extend(geocode_batch(batch_df))
    else:
        raise ValueError("CONFIG.geocode_mode must be either 'batch' or 'single'.")

    if new_results:
        new_cache_rows = pd.DataFrame(new_results, columns=GEOCODE_CACHE_COLUMNS)
        geocode_cache = (
            pd.concat([geocode_cache, new_cache_rows], ignore_index=True)
            .drop_duplicates('address_key', keep='last')
        )
        geocode_cache = geocode_cache[GEOCODE_CACHE_COLUMNS]
        geocode_cache.to_csv(GEOCODE_CACHE_CSV, index=False)
        logger.info('Updated geocode cache: %s', GEOCODE_CACHE_CSV)
else:
    logger.info('CONFIG.run_geocoding is False. Toggle it to True in the configuration cell to geocode uncached addresses.')

# Reload from disk if the cache exists, so downstream cells use the latest current-version cache.
if GEOCODE_CACHE_CSV.exists():
    geocode_cache = pd.read_csv(GEOCODE_CACHE_CSV, dtype=str)
    for col in GEOCODE_CACHE_COLUMNS:
        if col not in geocode_cache.columns:
            geocode_cache[col] = ''
    geocode_cache = geocode_cache[GEOCODE_CACHE_COLUMNS].copy()
    geocode_cache = geocode_cache.loc[
        geocode_cache['cache_version'].fillna('').eq(CONFIG.geocode_cache_version)
    ].copy()

for numeric_col in ['longitude', 'latitude', 'score']:
    if numeric_col in geocode_cache.columns:
        geocode_cache[numeric_col] = pd.to_numeric(geocode_cache[numeric_col], errors='coerce')

display(geocode_cache.head())

```

    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | CONFIG.run_geocoding is False. Toggle it to True in the configuration cell to geocode uncached addresses.



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
      <th>cache_version</th>
      <th>address_key</th>
      <th>input_address</th>
      <th>input_city</th>
      <th>input_state</th>
      <th>input_postal_code</th>
      <th>full_address</th>
      <th>longitude</th>
      <th>latitude</th>
      <th>score</th>
      <th>match_address</th>
      <th>addr_type</th>
      <th>geocode_status</th>
      <th>geocode_source</th>
      <th>geocode_run_utc</th>
      <th>raw_result</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>v2_result_validation</td>
      <td>100 CLINCH RIDGE RD|THORN HILL|TN|37881</td>
      <td>100 Clinch Ridge Rd</td>
      <td>THORN HILL</td>
      <td>TN</td>
      <td>37881</td>
      <td>100 Clinch Ridge Rd, THORN HILL, TN, 37881</td>
      <td>-83.267914</td>
      <td>36.464212</td>
      <td>86.69</td>
      <td>Ridge Rd, Sneedville, Tennessee, 37869</td>
      <td>StreetName</td>
      <td>matched_M</td>
      <td>arcgis.geocoding.geocode</td>
      <td>2026-07-08T17:10:43.204738+00:00</td>
      <td>{"address": "Ridge Rd, Sneedville, Tennessee, ...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>v2_result_validation</td>
      <td>101 FIRETOWER DR|COLLINWOOD|TN|38450</td>
      <td>101 Firetower Dr.</td>
      <td>COLLINWOOD</td>
      <td>TN</td>
      <td>38450</td>
      <td>101 Firetower Dr., COLLINWOOD, TN, 38450</td>
      <td>-87.742596</td>
      <td>35.148928</td>
      <td>100.00</td>
      <td>101 Fire Tower Dr, Collinwood, Tennessee, 38450</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>arcgis.geocoding.geocode</td>
      <td>2026-07-08T17:10:43.797947+00:00</td>
      <td>{"address": "101 Fire Tower Dr, Collinwood, Te...</td>
    </tr>
    <tr>
      <th>2</th>
      <td>v2_result_validation</td>
      <td>10150 CORINTH RD|WILDERSVILLE|TN|38388</td>
      <td>10150 Corinth Rd</td>
      <td>WILDERSVILLE</td>
      <td>TN</td>
      <td>38388</td>
      <td>10150 Corinth Rd, WILDERSVILLE, TN, 38388</td>
      <td>-88.276893</td>
      <td>35.767041</td>
      <td>100.00</td>
      <td>10150 Corinth Rd, Wildersville, Tennessee, 38388</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>arcgis.geocoding.geocode</td>
      <td>2026-07-08T17:10:44.411247+00:00</td>
      <td>{"address": "10150 Corinth Rd, Wildersville, T...</td>
    </tr>
    <tr>
      <th>3</th>
      <td>v2_result_validation</td>
      <td>1050 FIRETOWER LN|LUTTS|TN|38471</td>
      <td>1050 Firetower Ln</td>
      <td>LUTTS</td>
      <td>TN</td>
      <td>38471</td>
      <td>1050 Firetower Ln, LUTTS, TN, 38471</td>
      <td>-87.919313</td>
      <td>35.089074</td>
      <td>98.04</td>
      <td>1050 Firetower Rd, Lutts, Tennessee, 38471</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>arcgis.geocoding.geocode</td>
      <td>2026-07-08T17:10:45.244125+00:00</td>
      <td>{"address": "1050 Firetower Rd, Lutts, Tenness...</td>
    </tr>
    <tr>
      <th>4</th>
      <td>v2_result_validation</td>
      <td>1084 WESTEL ROAD|ROCKWOOD|TN|37854</td>
      <td>1084 Westel Road</td>
      <td>ROCKWOOD</td>
      <td>TN</td>
      <td>37854</td>
      <td>1084 Westel Road, ROCKWOOD, TN, 37854</td>
      <td>-84.743105</td>
      <td>35.872672</td>
      <td>100.00</td>
      <td>1084 Westel Rd, Rockwood, Tennessee, 37854</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>arcgis.geocoding.geocode</td>
      <td>2026-07-08T17:10:45.636093+00:00</td>
      <td>{"address": "1084 Westel Rd, Rockwood, Tenness...</td>
    </tr>
  </tbody>
</table>
</div>


## 8. Join geocode results and apply manual overrides

This cell creates geocode QA fields and applies reviewer decisions from `tdf_buildings_manual_overrides.csv`.

A valid manual coordinate override marks the record as `ManualOverride` and can publish even when the original/effective address would otherwise need review. `accept_geocode_candidate = TRUE` lets a visually verified geocoder candidate publish even if the automated parser flags city/ZIP/score/type issues. `exclude_from_publish = TRUE` keeps the record out of the hosted layer.

The validation-review CSV includes all records needing geocode review and all records manually excluded from publishing.



```python
# Remove any previous geocode columns before joining.
geocode_cols_to_drop = [
    'cache_version', 'input_address', 'input_city', 'input_state', 'input_postal_code',
    'longitude', 'latitude', 'score', 'match_address', 'addr_type', 'geocode_status',
    'geocode_source', 'geocode_run_utc', 'raw_result',
    'matched_street_part', 'matched_city_part', 'matched_zip_part', 'matched_address_number',
    'input_address_number', 'input_zip_part', 'matched_city_agrees', 'matched_zip_agrees',
    'matched_address_number_agrees', 'geocode_assignment_suspect'
]
working = df.drop(columns=[c for c in geocode_cols_to_drop if c in df.columns], errors='ignore').copy()

cache_for_join = geocode_cache.drop(columns=['full_address'], errors='ignore').copy()
working = working.merge(cache_for_join, on='address_key', how='left')

for col in ['longitude', 'latitude', 'score']:
    working[col] = pd.to_numeric(working[col], errors='coerce')

# Apply building-level manual coordinate overrides when present and within broad configured bounds.
if 'manual_coordinate_valid' not in working.columns:
    working['manual_coordinate_valid'] = False

if working['manual_coordinate_valid'].any():
    manual_coord_mask = working['manual_coordinate_valid']
    working.loc[manual_coord_mask, 'latitude'] = working.loc[manual_coord_mask, 'manual_latitude']
    working.loc[manual_coord_mask, 'longitude'] = working.loc[manual_coord_mask, 'manual_longitude']
    working.loc[manual_coord_mask, 'score'] = 100
    working.loc[manual_coord_mask, 'match_address'] = 'Manual coordinate override'
    working.loc[manual_coord_mask, 'addr_type'] = 'ManualOverride'
    working.loc[manual_coord_mask, 'geocode_status'] = 'manual_override'
    working.loc[manual_coord_mask, 'geocode_source'] = 'manual_override_csv'
    working.loc[manual_coord_mask, 'raw_result'] = working.loc[manual_coord_mask, 'override_notes']

WEAK_ADDR_TYPES = {'StreetName', 'Postal', 'PostalLoc', 'Locality', 'Subregion', 'Region', 'POI'}
working['has_coordinates'] = working['latitude'].notna() & working['longitude'].notna()
working['has_manual_coordinate_override'] = working['manual_coordinate_valid'].fillna(False)
working['has_manual_override'] = working['has_any_manual_override'].fillna(False)
working['meets_score_threshold'] = working['score'].fillna(0).ge(CONFIG.geocode_score_min)
working['weak_match_type'] = working['addr_type'].fillna('').isin(WEAK_ADDR_TYPES)
working['manual_location_review_unresolved'] = (
    working['manual_location_review_needed']
    & ~working['has_manual_coordinate_override']
    & ~working['accepted_geocode_candidate']
)

CITY_NAME_ALIASES = {
    # Source workbook abbreviations or formatting that are equivalent to geocoder output.
    'CUMBERLAND FUR': 'CUMBERLAND FURNACE',
    'CUMBERLAND FURN': 'CUMBERLAND FURNACE',
}

def normalize_compare_text(value):
    if pd.isna(value):
        return ''
    text = str(value).upper().strip()
    text = re.sub(r'[^A-Z0-9\s]', ' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    return CITY_NAME_ALIASES.get(text, text)

def compact_compare_text(value):
    return re.sub(r'\s+', '', normalize_compare_text(value))

def city_values_agree(input_city, matched_city):
    """Compare source/effective and matched city names while allowing harmless spacing/alias differences.

    Examples treated as equivalent:
      MCDONALD == Mc Donald
      LAFOLLETTE == La Follette
      CUMBERLAND FUR. == Cumberland Furnace
    """
    input_norm = normalize_compare_text(input_city)
    matched_norm = normalize_compare_text(matched_city)
    if input_norm == matched_norm:
        return True
    return compact_compare_text(input_city) == compact_compare_text(matched_city)

def first_address_number(value):
    if pd.isna(value):
        return ''
    match = re.search(r'\b\d+\b', str(value))
    return match.group(0) if match else ''

def parse_match_city_zip(match_address):
    """Best-effort parser for Esri match_address strings.

    Notes:
    - Use the last ZIP-like value in the match string, because street addresses can
      begin with five-digit house numbers such as '22117 Natchez Trace Rd'.
    - If the first comma-separated component is only a ZIP, treat the result as a
      postal-level match rather than as a street address.
    """
    if pd.isna(match_address) or not str(match_address).strip():
        return pd.Series({
            'matched_street_part': '',
            'matched_city_part': '',
            'matched_zip_part': '',
            'matched_address_number': '',
        })

    match_text = str(match_address)
    parts = [p.strip() for p in match_text.split(',')]

    street = parts[0] if len(parts) >= 1 else ''
    city = parts[1] if len(parts) >= 2 else ''

    zip_matches = re.findall(r'\b\d{5}(?:-\d{4})?\b', match_text)
    matched_zip = zip_matches[-1][:5] if zip_matches else ''

    # Postal-level matches often look like '37380, Sewanee, Tennessee'.
    # Do not treat the leading ZIP as a street address or address number.
    if re.fullmatch(r'\d{5}(?:-\d{4})?', street):
        street = ''
        city = parts[1] if len(parts) >= 2 else ''

    return pd.Series({
        'matched_street_part': street,
        'matched_city_part': city,
        'matched_zip_part': matched_zip,
        'matched_address_number': first_address_number(street),
    })

matched_parts = working['match_address'].apply(parse_match_city_zip)
working = pd.concat([working, matched_parts], axis=1)

working['input_address_number'] = working['address'].apply(first_address_number)
working['input_zip_part'] = (
    working['postal_code']
    .astype(str)
    .str.extract(r'(\d{5})', expand=False)
    .fillna('')
)
working['matched_city_agrees'] = [
    city_values_agree(input_city, matched_city)
    for input_city, matched_city in zip(working['city'], working['matched_city_part'])
]
working['matched_zip_agrees'] = (
    working['input_zip_part']
    == working['matched_zip_part'].astype(str).str.extract(r'(\d{5})', expand=False).fillna('')
)
working['matched_address_number_agrees'] = (
    (working['input_address_number'] == '')
    | (working['matched_address_number'] == '')
    | (working['input_address_number'] == working['matched_address_number'])
)

working['geocode_assignment_suspect'] = (
    working['has_coordinates']
    & ~working['has_manual_coordinate_override']
    & ~working['accepted_geocode_candidate']
    & (
        ~working['matched_city_agrees']
        | ~working['matched_zip_agrees']
        | ~working['matched_address_number_agrees']
    )
)
assignment_suspect_count = int(working['geocode_assignment_suspect'].sum())

manual_review_bypass = working['has_manual_coordinate_override'] | working['accepted_geocode_candidate']
working['geocode_quality_blocker'] = (
    ~working['meets_score_threshold']
    | working['weak_match_type']
    | working['manual_location_review_unresolved']
    | working['zip_conflict_for_address_city']
    | working['potential_typo_review_needed']
    | working['geocode_assignment_suspect']
)
working['geocode_review_needed'] = (
    ~working['has_coordinates']
    | working['manual_coordinate_invalid']
    | (~manual_review_bypass & working['geocode_quality_blocker'])
)
working['publish_ready'] = (
    working['has_coordinates']
    & ~working['geocode_review_needed']
    & ~working['exclude_from_publish_override']
)

def build_geocode_review_reason(row):
    reasons = []
    if bool(row.get('exclude_from_publish_override', False)):
        reasons.append('excluded_from_publish_by_manual_override')
    if bool(row.get('manual_coordinate_invalid', False)):
        reasons.append('manual_coordinate_invalid_or_out_of_bounds')
    if not bool(row.get('has_coordinates', False)):
        reasons.append('no_coordinates')
    if bool(row.get('accepted_geocode_candidate', False)):
        reasons.append('geocode_candidate_manually_accepted')
    if bool(row.get('has_manual_coordinate_override', False)):
        reasons.append('manual_coordinate_override')
    if not bool(row.get('accepted_geocode_candidate', False)) and not bool(row.get('has_manual_coordinate_override', False)):
        if not bool(row.get('meets_score_threshold', False)):
            reasons.append('below_score_threshold_or_no_score')
        if bool(row.get('weak_match_type', False)):
            reasons.append('weak_match_type')
        if bool(row.get('manual_location_review_unresolved', False)):
            reasons.append('manual_location_review_unresolved')
        if bool(row.get('zip_conflict_for_address_city', False)):
            reasons.append('zip_conflict_for_address_city')
        if bool(row.get('potential_typo_review_needed', False)):
            reasons.append('potential_typo_review_needed')
        if bool(row.get('geocode_assignment_suspect', False)):
            reasons.append('geocode_assignment_suspect')
    return '; '.join(reasons)

working['geocode_review_reason'] = working.apply(build_geocode_review_reason, axis=1)

coordinate_count = int(working['has_coordinates'].sum())
publish_ready_count = int(working['publish_ready'].sum())
geocode_review_count = int(working['geocode_review_needed'].sum())
manual_excluded_count = int(working['exclude_from_publish_override'].sum())

geocode_summary = pd.DataFrame([
    {'metric': 'Total records', 'count': len(working)},
    {'metric': 'Records with coordinates', 'count': coordinate_count},
    {'metric': 'Records without coordinates', 'count': int((~working['has_coordinates']).sum())},
    {'metric': 'Records with any manual override', 'count': int(working['has_manual_override'].sum())},
    {'metric': 'Records with valid manual coordinate overrides', 'count': int(working['has_manual_coordinate_override'].sum())},
    {'metric': 'Records accepting geocode candidate', 'count': int(working['accepted_geocode_candidate'].sum())},
    {'metric': 'Records excluded from publish by manual override', 'count': manual_excluded_count},
    {'metric': 'Records meeting score threshold', 'count': int(working['meets_score_threshold'].sum())},
    {'metric': 'Records with weak match type', 'count': int(working['weak_match_type'].sum())},
    {'metric': 'Records with suspect geocode assignment', 'count': assignment_suspect_count},
    {'metric': 'Records with unresolved manual location review', 'count': int(working['manual_location_review_unresolved'].sum())},
    {'metric': 'Records needing geocode review', 'count': geocode_review_count},
    {'metric': 'Publish-ready records', 'count': publish_ready_count},
])

display(geocode_summary)

if coordinate_count == 0 and len(unique_addresses) > 0:
    logger.warning(
        'No coordinates are present in the geocoded output. This means no current-version cache rows were joined '
        'and geocoding did not run during this notebook execution.'
    )

if CONFIG.run_publish and coordinate_count == 0:
    raise RuntimeError(
        'Publishing stopped. The geocoded dataframe contains zero coordinate-bearing records. '
        'Set run_geocoding=True or restore a valid current-version geocode cache before publishing.'
    )

review_fields = [
    'building_code', 'building_name', 'full_address', 'latitude', 'longitude', 'score',
    'match_address', 'addr_type', 'geocode_status', 'geocode_review_reason',
    'source_address', 'source_city', 'source_state', 'source_postal_code',
    'override_address', 'override_city', 'override_state', 'override_postal_code',
    'has_address_override', 'manual_latitude', 'manual_longitude', 'manual_coordinate_valid',
    'accepted_geocode_candidate', 'exclude_from_publish_override', 'override_notes', 'override_source',
    'manual_location_review_reason', 'potential_typo_reason', 'has_coordinates',
    'meets_score_threshold', 'weak_match_type', 'has_manual_override', 'has_manual_coordinate_override',
    'manual_location_review_unresolved', 'zip_conflict_for_address_city',
    'potential_typo_review_needed', 'geocode_assignment_suspect',
    'matched_street_part', 'matched_city_part', 'input_zip_part', 'matched_zip_part',
    'input_address_number', 'matched_address_number', 'matched_city_agrees',
    'matched_zip_agrees', 'matched_address_number_agrees',
]
review_fields = [field for field in review_fields if field in working.columns]

if CONFIG.write_full_geocode_review:
    geocode_review = working.loc[
        working['geocode_review_needed'] | working['exclude_from_publish_override'],
        review_fields,
    ].copy()
else:
    geocode_review = working.loc[
        working['geocode_assignment_suspect'] | working['exclude_from_publish_override'],
        review_fields,
    ].copy()

geocode_review.to_csv(GEOCODE_VALIDATION_CSV, index=False)
logger.info('Saved geocode validation review CSV: %s', GEOCODE_VALIDATION_CSV)
display(geocode_review.head(100))

# The full geocoded output includes records that are not publish-ready. The publish step can filter them.
working.to_csv(GEOCODED_CSV, index=False)
logger.info('Saved geocoded CSV: %s', GEOCODED_CSV)

df_geocoded = working.copy()

```


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
      <th>metric</th>
      <th>count</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Total records</td>
      <td>463</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Records with coordinates</td>
      <td>458</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Records without coordinates</td>
      <td>5</td>
    </tr>
    <tr>
      <th>3</th>
      <td>Records with any manual override</td>
      <td>9</td>
    </tr>
    <tr>
      <th>4</th>
      <td>Records with valid manual coordinate overrides</td>
      <td>9</td>
    </tr>
    <tr>
      <th>5</th>
      <td>Records accepting geocode candidate</td>
      <td>0</td>
    </tr>
    <tr>
      <th>6</th>
      <td>Records excluded from publish by manual override</td>
      <td>0</td>
    </tr>
    <tr>
      <th>7</th>
      <td>Records meeting score threshold</td>
      <td>450</td>
    </tr>
    <tr>
      <th>8</th>
      <td>Records with weak match type</td>
      <td>69</td>
    </tr>
    <tr>
      <th>9</th>
      <td>Records with suspect geocode assignment</td>
      <td>37</td>
    </tr>
    <tr>
      <th>10</th>
      <td>Records with unresolved manual location review</td>
      <td>65</td>
    </tr>
    <tr>
      <th>11</th>
      <td>Records needing geocode review</td>
      <td>125</td>
    </tr>
    <tr>
      <th>12</th>
      <td>Publish-ready records</td>
      <td>338</td>
    </tr>
  </tbody>
</table>
</div>


    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Saved geocode validation review CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_geocode_validation_review.csv



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
      <th>building_code</th>
      <th>building_name</th>
      <th>full_address</th>
      <th>latitude</th>
      <th>longitude</th>
      <th>score</th>
      <th>match_address</th>
      <th>addr_type</th>
      <th>geocode_status</th>
      <th>geocode_review_reason</th>
      <th>...</th>
      <th>geocode_assignment_suspect</th>
      <th>matched_street_part</th>
      <th>matched_city_part</th>
      <th>input_zip_part</th>
      <th>matched_zip_part</th>
      <th>input_address_number</th>
      <th>matched_address_number</th>
      <th>matched_city_agrees</th>
      <th>matched_zip_agrees</th>
      <th>matched_address_number_agrees</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>10000006</td>
      <td>Holston Mtn Radio Building</td>
      <td>National Forest 56, ELIZABETHTON, TN, 37643</td>
      <td>36.382348</td>
      <td>-82.148698</td>
      <td>96.42</td>
      <td>56 Forest Dr, Elizabethton, Tennessee, 37643</td>
      <td>StreetAddress</td>
      <td>matched_M</td>
      <td>manual_location_review_unresolved</td>
      <td>...</td>
      <td>False</td>
      <td>56 Forest Dr</td>
      <td>Elizabethton</td>
      <td>37643</td>
      <td>37643</td>
      <td>56</td>
      <td>56</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>3</th>
      <td>10010003</td>
      <td>Carter County Shop</td>
      <td>2643 Gap Creek Road, HAMPTON, TN, 37658</td>
      <td>36.260423</td>
      <td>-82.214202</td>
      <td>100.00</td>
      <td>2643 Gap Creek Rd, Hampton, Tennessee, 37658</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>zip_conflict_for_address_city</td>
      <td>...</td>
      <td>False</td>
      <td>2643 Gap Creek Rd</td>
      <td>Hampton</td>
      <td>37658</td>
      <td>37658</td>
      <td>2643</td>
      <td>2643</td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>12</th>
      <td>04000007</td>
      <td>Hendon Fire Tower</td>
      <td>Fire Tower Road, GRAYSVILLE, TN, 37338</td>
      <td>35.448263</td>
      <td>-85.246788</td>
      <td>88.27</td>
      <td>Hendon Fire Tower Rd, Graysville, Tennessee, 3...</td>
      <td>StreetName</td>
      <td>matched_M</td>
      <td>weak_match_type; manual_location_review_unreso...</td>
      <td>...</td>
      <td>False</td>
      <td>Hendon Fire Tower Rd</td>
      <td>Graysville</td>
      <td>37338</td>
      <td>37338</td>
      <td></td>
      <td></td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>13</th>
      <td>04010001</td>
      <td>Bledsoe SF Smoke House</td>
      <td>Route 3 Box 180, PIKEVILLE, TN, 37367</td>
      <td>35.579069</td>
      <td>-85.145580</td>
      <td>94.03</td>
      <td>3rd St, Pikeville, Tennessee, 37367</td>
      <td>StreetName</td>
      <td>matched_M</td>
      <td>weak_match_type; manual_location_review_unreso...</td>
      <td>...</td>
      <td>False</td>
      <td>3rd St</td>
      <td>Pikeville</td>
      <td>37367</td>
      <td>37367</td>
      <td>3</td>
      <td></td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>14</th>
      <td>04010002</td>
      <td>Bledsoe SF Storm Shelter</td>
      <td>Route 3 Box 180, PIKEVILLE, TN, 37367</td>
      <td>35.579069</td>
      <td>-85.145580</td>
      <td>94.03</td>
      <td>3rd St, Pikeville, Tennessee, 37367</td>
      <td>StreetName</td>
      <td>matched_M</td>
      <td>weak_match_type; manual_location_review_unreso...</td>
      <td>...</td>
      <td>False</td>
      <td>3rd St</td>
      <td>Pikeville</td>
      <td>37367</td>
      <td>37367</td>
      <td>3</td>
      <td></td>
      <td>True</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>...</th>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
      <td>...</td>
    </tr>
    <tr>
      <th>363</th>
      <td>71000015</td>
      <td>Cookeville District Office</td>
      <td>390 SOUTH LOWE, SUITE 10, COOKEVILLE, TN, 38501</td>
      <td>36.153647</td>
      <td>-85.499045</td>
      <td>98.84</td>
      <td>390 S Lowe Ave, Suite 10, Cookeville, Tennesse...</td>
      <td>Subaddress</td>
      <td>matched_M</td>
      <td>geocode_assignment_suspect</td>
      <td>...</td>
      <td>True</td>
      <td>390 S Lowe Ave</td>
      <td>Suite 10</td>
      <td>38501</td>
      <td>38501</td>
      <td>390</td>
      <td>390</td>
      <td>False</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>369</th>
      <td>71020001</td>
      <td>Cookeville Tower Storage</td>
      <td>NONE, COOKEVILLE, TN, 38501</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>no_coordinates; below_score_threshold_or_no_sc...</td>
      <td>...</td>
      <td>False</td>
      <td></td>
      <td></td>
      <td>38501</td>
      <td></td>
      <td></td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>370</th>
      <td>71020002</td>
      <td>Cookeville Tower</td>
      <td>NONE, COOKEVILLE, TN, 38501</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>no_coordinates; below_score_threshold_or_no_sc...</td>
      <td>...</td>
      <td>False</td>
      <td></td>
      <td></td>
      <td>38501</td>
      <td></td>
      <td></td>
      <td></td>
      <td>False</td>
      <td>False</td>
      <td>True</td>
    </tr>
    <tr>
      <th>371</th>
      <td>72010001</td>
      <td>Smyrna Shop</td>
      <td>208 Fire Tower Road, EVANSVILLE, TN, 37332</td>
      <td>35.580564</td>
      <td>-84.921875</td>
      <td>89.27</td>
      <td>208 Smyrna Firetower Rd, Evensville, Tennessee...</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>geocode_assignment_suspect</td>
      <td>...</td>
      <td>True</td>
      <td>208 Smyrna Firetower Rd</td>
      <td>Evensville</td>
      <td>37332</td>
      <td>37332</td>
      <td>208</td>
      <td>208</td>
      <td>False</td>
      <td>True</td>
      <td>True</td>
    </tr>
    <tr>
      <th>372</th>
      <td>72010002</td>
      <td>Smyrna Fire Tower</td>
      <td>208 Fire Tower Road, EVANSVILLE, TN, 37332</td>
      <td>35.580564</td>
      <td>-84.921875</td>
      <td>89.27</td>
      <td>208 Smyrna Firetower Rd, Evensville, Tennessee...</td>
      <td>PointAddress</td>
      <td>matched_M</td>
      <td>geocode_assignment_suspect</td>
      <td>...</td>
      <td>True</td>
      <td>208 Smyrna Firetower Rd</td>
      <td>Evensville</td>
      <td>37332</td>
      <td>37332</td>
      <td>208</td>
      <td>208</td>
      <td>False</td>
      <td>True</td>
      <td>True</td>
    </tr>
  </tbody>
</table>
<p>100 rows × 46 columns</p>
</div>


    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Saved geocoded CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_geocoded.csv


## 9. Create a Spatially Enabled DataFrame

This step filters to publishable records, trims the outgoing schema to the operational field list, writes a publish-field preview CSV, and creates the Spatially Enabled DataFrame used for previewing and publishing.



```python
# Choose publication filter.
# Recommended for first production publish: publish_ready == True.
# For internal QA layers, you may prefer has_coordinates == True so questionable records can be mapped and reviewed.
PUBLISH_ONLY_READY_RECORDS = True

if PUBLISH_ONLY_READY_RECORDS:
    publish_source_df = working.loc[working['publish_ready']].copy()
else:
    publish_source_df = working.loc[working['has_coordinates']].copy()

publish_candidate_suspect_count = int(
    publish_source_df['geocode_assignment_suspect'].sum()
) if 'geocode_assignment_suspect' in publish_source_df.columns else 0

if CONFIG.run_publish and publish_candidate_suspect_count > CONFIG.max_allowed_suspect_assignments:
    raise RuntimeError(
        f'Publishing stopped. Found {publish_candidate_suspect_count:,} suspect geocode assignments '
        'inside the selected publish dataset. '
        f'The configured maximum is {CONFIG.max_allowed_suspect_assignments:,}. '
        'Use PUBLISH_ONLY_READY_RECORDS = True or review tdf_buildings_geocode_validation_review.csv.'
    )

if CONFIG.run_publish and publish_source_df.empty:
    raise RuntimeError(
        'Publishing stopped. The selected publish dataframe is empty. '
        'Review geocoding output, cache status, and PUBLISH_ONLY_READY_RECORDS before publishing.'
    )

missing_publish_fields = [field for field in PUBLISH_ATTRIBUTE_FIELDS if field not in publish_source_df.columns]
if missing_publish_fields:
    raise ValueError(f'Missing fields required for publication schema: {missing_publish_fields}')

if CONFIG.sync_key_field not in PUBLISH_ATTRIBUTE_FIELDS:
    raise ValueError(
        f'CONFIG.sync_key_field ({CONFIG.sync_key_field}) must be included in PUBLISH_ATTRIBUTE_FIELDS '
        'so the upsert refresh strategy can match existing records.'
    )

publish_df = publish_source_df.loc[:, list(PUBLISH_ATTRIBUTE_FIELDS)].copy()

# Normalize key text fields before publishing. The generated QA/geocode outputs are not altered here.
publish_df['building_code'] = publish_df['building_code'].map(lambda value: clean_identifier(value, width=8))
publish_df['postal_code'] = publish_df['postal_code'].map(clean_postal_code)

text_publish_fields = [
    field for field in PUBLISH_ATTRIBUTE_FIELDS
    if field not in {'longitude', 'latitude', 'square_feet'}
]
for field in text_publish_fields:
    publish_df[field] = publish_df[field].map(clean_text)

publish_df['longitude'] = pd.to_numeric(publish_df['longitude'], errors='coerce')
publish_df['latitude'] = pd.to_numeric(publish_df['latitude'], errors='coerce')
publish_df['square_feet'] = pd.to_numeric(publish_df['square_feet'], errors='coerce')

invalid_publish_coordinates = publish_df['longitude'].isna() | publish_df['latitude'].isna()
if invalid_publish_coordinates.any():
    invalid_count = int(invalid_publish_coordinates.sum())
    raise ValueError(
        f'{invalid_count:,} selected publish records are missing latitude or longitude. '
        'Review publish_ready logic and manual overrides before publishing.'
    )

publish_df.to_csv(PUBLISH_FIELDS_PREVIEW_CSV, index=False)
logger.info('Wrote publish-field preview CSV: %s', PUBLISH_FIELDS_PREVIEW_CSV)
logger.info('Publish attribute field count: %s', len(PUBLISH_ATTRIBUTE_FIELDS))
logger.info('Publish attribute fields: %s', ', '.join(PUBLISH_ATTRIBUTE_FIELDS))

if publish_df.empty:
    logger.warning('No records met the current spatial publication filter.')
    sdf = publish_df.copy()
else:
    sdf = pd.DataFrame.spatial.from_xy(
        publish_df,
        x_column='longitude',
        y_column='latitude',
        sr=4326,
    )

logger.info('Spatial dataframe records: %s', len(sdf))
logger.info('PUBLISH_ONLY_READY_RECORDS: %s', PUBLISH_ONLY_READY_RECORDS)
display(sdf.head())

```

    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Wrote publish-field preview CSV: /arcgis/home/tdf_facilities/tdf_buildings_outputs/tdf_buildings_publish_fields_preview.csv
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Publish attribute field count: 18
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Publish attribute fields: building_code, building_name, address, city, county, state, postal_code, longitude, latitude, square_feet, predominant_facility_use, building_utilization, asset_status, strategic_asset_status, building_status, geocode_source, override_notes, override_source
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | Spatial dataframe records: 338
    2026-07-08 19:25:10 | INFO | tdf_buildings_pipeline | PUBLISH_ONLY_READY_RECORDS: True



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
      <th>building_code</th>
      <th>building_name</th>
      <th>address</th>
      <th>city</th>
      <th>county</th>
      <th>state</th>
      <th>postal_code</th>
      <th>longitude</th>
      <th>latitude</th>
      <th>square_feet</th>
      <th>predominant_facility_use</th>
      <th>building_utilization</th>
      <th>asset_status</th>
      <th>strategic_asset_status</th>
      <th>building_status</th>
      <th>geocode_source</th>
      <th>override_notes</th>
      <th>override_source</th>
      <th>SHAPE</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>1</th>
      <td>10010001</td>
      <td>Carter County Residence</td>
      <td>2651 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>CARTER</td>
      <td>TN</td>
      <td>37658</td>
      <td>-82.213546</td>
      <td>36.260446</td>
      <td>1400.0</td>
      <td>Residential</td>
      <td>Utilized</td>
      <td>Current Mission Need</td>
      <td>Long Term 15+ yrs</td>
      <td>Active</td>
      <td>arcgis.geocoding.geocode</td>
      <td></td>
      <td></td>
      <td>{"spatialReference": {"wkid": 4326}, "x": -82....</td>
    </tr>
    <tr>
      <th>2</th>
      <td>10010002</td>
      <td>Carter County Outbuilding #2</td>
      <td>2651 Gap Creek Road</td>
      <td>HAMPTON</td>
      <td>CARTER</td>
      <td>TN</td>
      <td>37658</td>
      <td>-82.213546</td>
      <td>36.260446</td>
      <td>1600.0</td>
      <td>Warehouse Space</td>
      <td>Utilized</td>
      <td>Current Mission Need</td>
      <td>Long Term 15+ yrs</td>
      <td>Active</td>
      <td>arcgis.geocoding.geocode</td>
      <td></td>
      <td></td>
      <td>{"spatialReference": {"wkid": 4326}, "x": -82....</td>
    </tr>
    <tr>
      <th>4</th>
      <td>01010001</td>
      <td>Bluebird Storage Building</td>
      <td>395 Firetower Lane</td>
      <td>HEISKELL</td>
      <td>ANDERSON</td>
      <td>TN</td>
      <td>37754</td>
      <td>-84.063614</td>
      <td>36.139897</td>
      <td>48.0</td>
      <td>Warehouse Space</td>
      <td>Utilized</td>
      <td>Current Mission Need</td>
      <td>Long Term 15+ yrs</td>
      <td>Active</td>
      <td>arcgis.geocoding.geocode</td>
      <td></td>
      <td></td>
      <td>{"spatialReference": {"wkid": 4326}, "x": -84....</td>
    </tr>
    <tr>
      <th>5</th>
      <td>01010002</td>
      <td>Bluebird Shop</td>
      <td>395 Firetower Lane</td>
      <td>HEISKELL</td>
      <td>ANDERSON</td>
      <td>TN</td>
      <td>37754</td>
      <td>-84.063614</td>
      <td>36.139897</td>
      <td>800.0</td>
      <td>Public Safety</td>
      <td>Utilized</td>
      <td>Current Mission Need</td>
      <td>Long Term 15+ yrs</td>
      <td>Active</td>
      <td>arcgis.geocoding.geocode</td>
      <td></td>
      <td></td>
      <td>{"spatialReference": {"wkid": 4326}, "x": -84....</td>
    </tr>
    <tr>
      <th>6</th>
      <td>01010003</td>
      <td>Bluebird Office</td>
      <td>395 Firetower Lane</td>
      <td>HEISKELL</td>
      <td>ANDERSON</td>
      <td>TN</td>
      <td>37754</td>
      <td>-84.063614</td>
      <td>36.139897</td>
      <td>120.0</td>
      <td>Office Space</td>
      <td>Utilized</td>
      <td>Current Mission Need</td>
      <td>Long Term 15+ yrs</td>
      <td>Active</td>
      <td>arcgis.geocoding.geocode</td>
      <td></td>
      <td></td>
      <td>{"spatialReference": {"wkid": 4326}, "x": -84....</td>
    </tr>
  </tbody>
</table>
</div>


## 10. Optional map preview

Run this after geocoding. It previews the in-memory points before publishing.


```python
if len(sdf) > 0:
    preview_map = gis.map('Tennessee')
    sdf.spatial.plot(preview_map)
    display(preview_map)
else:
    logger.info('No spatial records available for preview.')

```


    Map()


## 11. Publish or refresh hosted feature layer

Publishing uses the trimmed operational schema prepared in the previous step.

New publishes are created by directly creating an empty hosted feature service, adding a single named point layer, and then adding features. This avoids the extra File Geodatabase/source item that can be created by the Spatially Enabled DataFrame `to_featurelayer()` helper. The new layer name is controlled by `CONFIG.publish_layer_name`.

Existing hosted layers can be refreshed only when they contain the required published fields. Extra fields on an existing layer are not removed by upsert. To physically remove old fields from the hosted layer schema, publish a new layer with `feature_layer_item_id = ""`.


```python
def chunked(sequence, size):
    for start in range(0, len(sequence), size):
        yield sequence[start:start + size]

def normalize_sync_key(value):
    # Building codes are the sync key for this workflow and must remain 8-character text.
    return clean_identifier(value, width=8)

def get_attr_case_insensitive(attributes, field_name, default=None):
    if not attributes:
        return default
    target = field_name.lower()
    for key, value in attributes.items():
        if str(key).lower() == target:
            return value
    return default

def get_layer_from_item(item_id):
    item = gis.content.get(item_id)
    if item is None:
        raise ValueError(f'No ArcGIS Online item found for item ID: {item_id}')
    if not getattr(item, 'layers', None):
        raise ValueError(f'Item has no layers: {item_id}')
    return item, item.layers[0]

def update_item_metadata(item):
    """Update only stable item properties that the notebook is allowed to manage.

    Metadata policy for this project:
      - The notebook manages the item title and summary/snippet only.
      - Description, tags, credits, terms of use, thumbnail, categories, sharing,
        formal metadata, pop-ups, and other AGOL-authored item details are
        intentionally preserved after the initial hosted feature layer is created.
    """
    item.update(item_properties={
        'title': CONFIG.publish_title,
        'snippet': CONFIG.publish_summary,
    })
    return item

def sanitize_feature_service_name(value):
    """Return a URL-safe hosted FeatureServer service name."""
    text = clean_text(value)
    text = re.sub(r'[^A-Za-z0-9_]+', '_', text)
    text = re.sub(r'_+', '_', text).strip('_')
    if not text:
        raise ValueError('Feature service name cannot be blank after sanitizing.')
    if not re.match(r'^[A-Za-z]', text):
        text = f'TDF_{text}'
    return text[:120]

def resolve_new_service_name():
    requested = CONFIG.publish_service_name or CONFIG.publish_title
    service_name = sanitize_feature_service_name(requested)

    if gis.content.is_service_name_available(service_name, 'featureService'):
        return service_name

    if CONFIG.allow_service_name_suffix_if_unavailable:
        suffixed = sanitize_feature_service_name(f'{service_name}_{RUN_ID}')
        if gis.content.is_service_name_available(suffixed, 'featureService'):
            logger.warning(
                'Requested feature service name is unavailable: %s. Using suffixed name: %s',
                service_name,
                suffixed,
            )
            return suffixed

    raise ValueError(
        f'Feature service name is already unavailable: {service_name}. '
        'Delete/rename the existing service, set CONFIG.publish_service_name to a new URL-safe name, '
        'or set CONFIG.allow_service_name_suffix_if_unavailable = True for test publishes.'
    )

def assert_valid_sync_key(sdf):
    if CONFIG.sync_key_field not in sdf.columns:
        raise ValueError(f'Sync key field is missing from publish dataframe: {CONFIG.sync_key_field}')

    keys = sdf[CONFIG.sync_key_field].map(normalize_sync_key)
    missing_count = int(keys.eq('').sum())
    duplicate_count = int(keys.duplicated(keep=False).sum())

    if missing_count:
        raise ValueError(f'{missing_count:,} publish records have blank {CONFIG.sync_key_field}; upsert requires a populated key.')
    if duplicate_count:
        duplicate_keys = sorted(keys[keys.duplicated(keep=False)].unique())[:20]
        raise ValueError(
            f'{duplicate_count:,} publish records have duplicate {CONFIG.sync_key_field} values. '
            f'Examples: {duplicate_keys}'
        )

def layer_field_lookup(layer):
    return {field['name'].lower(): field['name'] for field in layer.properties.fields}

def layer_system_field_names(layer):
    object_id_field = layer.properties.objectIdField
    geometry_field = getattr(layer.properties, 'geometryField', None)
    system_fields = {object_id_field.lower(), 'shape'}
    if geometry_field:
        system_fields.add(str(geometry_field).lower())
    return system_fields

def warn_existing_layer_has_extra_fields(layer, sdf):
    fields_by_lower = layer_field_lookup(layer)
    system_fields = layer_system_field_names(layer)
    incoming_fields = {str(col).lower() for col in sdf.columns}

    extra_fields = [
        actual_name for lower_name, actual_name in fields_by_lower.items()
        if lower_name not in system_fields and lower_name not in incoming_fields
    ]
    if extra_fields:
        logger.warning(
            'Existing hosted layer contains %s fields that are not in the current publish schema. '
            'The upsert refresh strategy will not remove fields from an existing layer. '
            'Publish a new layer with CONFIG.feature_layer_item_id blank if you need the hosted layer schema trimmed. '
            'Extra fields: %s',
            len(extra_fields),
            extra_fields,
        )

def validate_layer_schema(layer, sdf):
    fields_by_lower = layer_field_lookup(layer)
    system_fields = layer_system_field_names(layer)

    missing_fields = [
        col for col in sdf.columns
        if col.lower() not in system_fields and col.lower() not in fields_by_lower
    ]
    if missing_fields:
        raise ValueError(
            'The existing hosted layer is missing fields that are present in the publish dataframe: '
            f'{missing_fields}. Add these fields to the layer or republish a new layer.'
        )

    if CONFIG.sync_key_field.lower() not in fields_by_lower:
        raise ValueError(f'The existing hosted layer does not contain sync key field: {CONFIG.sync_key_field}')

    warn_existing_layer_has_extra_fields(layer, sdf)

def get_feature_list_from_sdf(sdf):
    return sdf.spatial.to_featureset().features

def align_feature_attributes_to_layer(layer, features):
    fields_by_lower = layer_field_lookup(layer)
    object_id_field = layer.properties.objectIdField

    for feature in features:
        attrs = feature.attributes or {}
        aligned = {}
        for key, value in attrs.items():
            if str(key).lower() == 'shape':
                continue
            target_field = fields_by_lower.get(str(key).lower())
            if target_field:
                aligned[target_field] = value
        attrs.clear()
        attrs.update(aligned)

    return features, fields_by_lower[CONFIG.sync_key_field.lower()], object_id_field

def publish_field_definition(field_name):
    alias = PUBLISH_FIELD_ALIASES.get(field_name, field_name.replace('_', ' ').title())

    string_lengths = {
        'building_code': 32,
        'building_name': 255,
        'address': 255,
        'city': 100,
        'county': 100,
        'state': 16,
        'postal_code': 20,
        'predominant_facility_use': 255,
        'building_utilization': 255,
        'asset_status': 255,
        'strategic_asset_status': 255,
        'building_status': 255,
        'geocode_source': 100,
        'override_notes': 1000,
        'override_source': 255,
    }

    if field_name in {'longitude', 'latitude', 'square_feet'}:
        return {
            'name': field_name,
            'type': 'esriFieldTypeDouble',
            'alias': alias,
            'nullable': True,
            'editable': True,
        }

    return {
        'name': field_name,
        'type': 'esriFieldTypeString',
        'alias': alias,
        'length': string_lengths.get(field_name, 255),
        'nullable': True,
        'editable': True,
    }

def build_publish_layer_definition(sdf):
    xmin = float(sdf['longitude'].min())
    xmax = float(sdf['longitude'].max())
    ymin = float(sdf['latitude'].min())
    ymax = float(sdf['latitude'].max())

    fields = [
        {
            'name': 'OBJECTID',
            'type': 'esriFieldTypeOID',
            'alias': 'OBJECTID',
            'nullable': False,
            'editable': False,
        }
    ]
    fields.extend(publish_field_definition(field_name) for field_name in PUBLISH_ATTRIBUTE_FIELDS)

    default_attributes = {field_name: None for field_name in PUBLISH_ATTRIBUTE_FIELDS}

    return {
        'id': 0,
        'name': CONFIG.publish_layer_name,
        'type': 'Feature Layer',
        'displayField': 'building_name',
        'description': CONFIG.publish_summary,
        'geometryType': 'esriGeometryPoint',
        'hasZ': False,
        'hasM': False,
        'objectIdField': 'OBJECTID',
        'uniqueField': {
            'name': 'OBJECTID',
            'isSystemMaintained': True,
        },
        'spatialReference': {'wkid': 4326, 'latestWkid': 4326},
        'extent': {
            'xmin': xmin,
            'ymin': ymin,
            'xmax': xmax,
            'ymax': ymax,
            'spatialReference': {'wkid': 4326, 'latestWkid': 4326},
        },
        'fields': fields,
        'indexes': [
            {
                'name': 'PK_OBJECTID',
                'fields': 'OBJECTID',
                'isAscending': True,
                'isUnique': True,
                'description': 'Primary key index',
            },
        ],
        'types': [],
        'templates': [
            {
                'name': CONFIG.publish_layer_name,
                'description': '',
                'drawingTool': 'esriFeatureEditToolPoint',
                'prototype': {'attributes': default_attributes},
            }
        ],
        'drawingInfo': {
            'renderer': {
                'type': 'simple',
                'symbol': {
                    'type': 'esriSMS',
                    'style': 'esriSMSCircle',
                    'size': 7,
                    'color': [0, 112, 255, 180],
                    'outline': {'color': [255, 255, 255, 255], 'width': 0.75},
                },
            },
        },
        'capabilities': 'Query,Create,Update,Delete,Editing',
    }

def add_to_definition_succeeded(result):
    if isinstance(result, dict):
        if result.get('success') is True:
            return True
        if str(result.get('status', '')).lower() == 'completed':
            return True
        if 'error' in result:
            return False
    return bool(result)

def wait_for_first_layer(item, timeout_seconds=60):
    deadline = time.time() + timeout_seconds
    refreshed_item = item
    while time.time() < deadline:
        refreshed_item = gis.content.get(item.id)
        if refreshed_item is not None and getattr(refreshed_item, 'layers', None):
            return refreshed_item, refreshed_item.layers[0]
        time.sleep(2)
    raise RuntimeError(
        f'Hosted feature service was created, but no layer appeared within {timeout_seconds} seconds. '
        f'Item ID: {item.id}'
    )

def apply_publish_field_aliases(layer):
    """Best-effort alias update for the operational fields.

    Field order comes from the publish layer definition when creating a new layer. Existing hosted layers keep their
    current field order; upsert edits records but does not restructure the layer schema.
    """
    fields_by_lower = layer_field_lookup(layer)
    field_updates = []
    for field_name, alias in PUBLISH_FIELD_ALIASES.items():
        actual_name = fields_by_lower.get(field_name.lower())
        if actual_name:
            field_updates.append({'name': actual_name, 'alias': alias})

    if not field_updates:
        return None

    try:
        result = layer.manager.update_definition({'fields': field_updates})
        logger.info('Applied publish field aliases: %s', result)
        return result
    except Exception as exc:
        logger.warning('Could not apply field aliases automatically: %s', exc)
        return None

def apply_publish_layer_name(layer):
    """Best-effort update for Layer (0) display name."""
    try:
        current_name = clean_text(getattr(layer.properties, 'name', ''))
        if current_name != CONFIG.publish_layer_name:
            result = layer.manager.update_definition({'name': CONFIG.publish_layer_name})
            logger.info('Applied layer name %r: %s', CONFIG.publish_layer_name, result)
            return result
    except Exception as exc:
        logger.warning('Could not apply layer name automatically: %s', exc)
    return None

def apply_publish_layer_metadata(item, layer):
    apply_publish_layer_name(layer)
    apply_publish_field_aliases(layer)
    update_item_metadata(item)
    return item

def create_empty_hosted_feature_service():
    service_name = resolve_new_service_name()
    logger.info('Creating hosted feature service. Item title: %s', CONFIG.publish_title)
    logger.info('FeatureServer service name: %s', service_name)

    item = gis.content.create_service(
        name=service_name,
        service_type='featureService',
        service_description=CONFIG.publish_summary,
        description=CONFIG.publish_description,
        capabilities='Query,Create,Update,Delete,Editing',
        max_record_count=CONFIG.max_record_count,
        supported_query_formats='JSON,geoJSON',
        wkid=4326,
        item_properties={
            'title': CONFIG.publish_title,
            'snippet': CONFIG.publish_summary,
            'description': CONFIG.publish_description,
            'tags': ','.join(CONFIG.publish_tags),
        },
        tags=','.join(CONFIG.publish_tags),
        snippet=CONFIG.publish_summary,
    )
    if item is None:
        raise RuntimeError('gis.content.create_service() returned None. The hosted feature service was not created.')
    return item, service_name

def publish_new_feature_layer(sdf):
    """Create a named hosted feature service/layer directly, avoiding a separate File Geodatabase item."""
    assert_valid_sync_key(sdf)
    item, service_name = create_empty_hosted_feature_service()

    try:
        flc = FeatureLayerCollection.fromitem(item)
        layer_definition = build_publish_layer_definition(sdf)
        add_layer_result = flc.manager.add_to_definition({'layers': [layer_definition]})
        logger.info('Add layer definition result: %s', add_layer_result)
        if not add_to_definition_succeeded(add_layer_result):
            raise RuntimeError(f'Could not add layer definition to hosted feature service: {add_layer_result}')

        item, layer = wait_for_first_layer(item)
        validate_layer_schema(layer, sdf)
        features, _, _ = align_feature_attributes_to_layer(layer, get_feature_list_from_sdf(sdf))
        add_results = add_features_in_batches(layer, features)
        apply_publish_layer_metadata(item, layer)

        return item, {
            'new_publish': True,
            'item_id': item.id,
            'feature_service_name': service_name,
            'layer_name': CONFIG.publish_layer_name,
            'add_layer_result': add_layer_result,
            'add_results': add_results,
            'created_without_source_file_geodatabase': True,
        }
    except Exception:
        logger.exception('New hosted feature service publish failed. Attempting to delete the partially created service item: %s', item.id)
        try:
            item.delete()
        except Exception as cleanup_exc:
            logger.warning('Could not delete partially created service item %s: %s', item.id, cleanup_exc)
        raise

def add_features_in_batches(layer, features, batch_size=CONFIG.publish_batch_size):
    add_results = []
    for batch_number, feature_batch in enumerate(chunked(features, batch_size), start=1):
        logger.info('Adding batch %s with %s features', batch_number, len(feature_batch))
        result = layer.edit_features(adds=feature_batch)
        add_results.append(result)
    return add_results

def refresh_existing_feature_layer_delete_add(item_id, sdf):
    item, layer = get_layer_from_item(item_id)
    validate_layer_schema(layer, sdf)
    features, _, _ = align_feature_attributes_to_layer(layer, get_feature_list_from_sdf(sdf))

    logger.info('Deleting existing features from layer: %s', layer.properties.name)
    delete_result = layer.delete_features(where='1=1')
    logger.info('Delete result: %s', delete_result)

    add_results = add_features_in_batches(layer, features)
    apply_publish_layer_metadata(item, layer)
    return item, {'delete_result': delete_result, 'add_results': add_results}

def refresh_existing_feature_layer_truncate_add(item_id, sdf):
    item, layer = get_layer_from_item(item_id)
    validate_layer_schema(layer, sdf)
    features, _, _ = align_feature_attributes_to_layer(layer, get_feature_list_from_sdf(sdf))

    logger.info('Truncating layer: %s', layer.properties.name)
    truncate_result = layer.manager.truncate(asynchronous=CONFIG.truncate_async, wait=True)
    logger.info('Truncate result: %s', truncate_result)

    add_results = add_features_in_batches(layer, features)
    apply_publish_layer_metadata(item, layer)
    return item, {'truncate_result': truncate_result, 'add_results': add_results}

def query_existing_sync_keys(layer, sync_field_name, object_id_field):
    out_fields = f'{object_id_field},{sync_field_name}'
    existing = layer.query(
        where='1=1',
        out_fields=out_fields,
        return_geometry=False,
        return_all_records=True,
    )

    existing_by_key = {}
    duplicate_existing_keys = set()
    for feature in existing.features:
        attrs = feature.attributes or {}
        key = normalize_sync_key(get_attr_case_insensitive(attrs, sync_field_name))
        oid = get_attr_case_insensitive(attrs, object_id_field)
        if not key:
            continue
        if key in existing_by_key:
            duplicate_existing_keys.add(key)
        existing_by_key[key] = oid

    if duplicate_existing_keys:
        examples = sorted(duplicate_existing_keys)[:20]
        raise ValueError(
            f'The existing hosted layer has duplicate {CONFIG.sync_key_field} values. '
            f'Upsert would be ambiguous. Examples: {examples}'
        )

    return existing_by_key

def refresh_existing_feature_layer_upsert(item_id, sdf):
    assert_valid_sync_key(sdf)

    item, layer = get_layer_from_item(item_id)
    validate_layer_schema(layer, sdf)
    features, sync_field_name, object_id_field = align_feature_attributes_to_layer(layer, get_feature_list_from_sdf(sdf))

    existing_by_key = query_existing_sync_keys(layer, sync_field_name, object_id_field)
    incoming_by_key = {}
    for feature in features:
        key = normalize_sync_key(get_attr_case_insensitive(feature.attributes, sync_field_name))
        incoming_by_key[key] = feature

    updates = []
    adds = []
    for key, feature in incoming_by_key.items():
        if key in existing_by_key:
            feature.attributes[object_id_field] = existing_by_key[key]
            updates.append(feature)
        else:
            feature.attributes.pop(object_id_field, None)
            adds.append(feature)

    delete_oids = []
    if CONFIG.upsert_delete_missing_records:
        delete_oids = [oid for key, oid in existing_by_key.items() if key not in incoming_by_key]

    logger.info('Existing layer records: %s', len(existing_by_key))
    logger.info('Incoming records: %s', len(incoming_by_key))
    logger.info('Updates: %s', len(updates))
    logger.info('Adds: %s', len(adds))
    logger.info('Deletes: %s', len(delete_oids))

    edit_results = {'update_results': [], 'add_results': [], 'delete_results': []}

    for batch_number, feature_batch in enumerate(chunked(updates, CONFIG.publish_batch_size), start=1):
        logger.info('Updating batch %s with %s features', batch_number, len(feature_batch))
        edit_results['update_results'].append(layer.edit_features(updates=feature_batch))

    for batch_number, feature_batch in enumerate(chunked(adds, CONFIG.publish_batch_size), start=1):
        logger.info('Adding batch %s with %s features', batch_number, len(feature_batch))
        edit_results['add_results'].append(layer.edit_features(adds=feature_batch))

    for batch_number, oid_batch in enumerate(chunked(delete_oids, CONFIG.publish_batch_size), start=1):
        logger.info('Deleting batch %s with %s features', batch_number, len(oid_batch))
        edit_results['delete_results'].append(layer.edit_features(deletes=','.join(map(str, oid_batch))))

    apply_publish_layer_metadata(item, layer)
    return item, edit_results

def refresh_existing_feature_layer(item_id, sdf):
    strategy = CONFIG.refresh_strategy.strip().lower()
    if strategy == 'upsert':
        return refresh_existing_feature_layer_upsert(item_id, sdf)
    if strategy == 'truncate_add':
        return refresh_existing_feature_layer_truncate_add(item_id, sdf)
    if strategy == 'delete_add':
        return refresh_existing_feature_layer_delete_add(item_id, sdf)
    raise ValueError("CONFIG.refresh_strategy must be 'upsert', 'truncate_add', or 'delete_add'.")

if CONFIG.run_publish:
    if len(sdf) == 0:
        raise ValueError('No spatial records are available to publish.')

    if CONFIG.feature_layer_item_id.strip():
        published_item, edit_results = refresh_existing_feature_layer(CONFIG.feature_layer_item_id.strip(), sdf)
        logger.info('Refreshed existing hosted feature layer item ID: %s', published_item.id)
        logger.info('Refresh strategy: %s', CONFIG.refresh_strategy)
        display(published_item)
    else:
        published_item, edit_results = publish_new_feature_layer(sdf)
        logger.info('Created new hosted feature layer item ID: %s', published_item.id)
        logger.info('FeatureServer service name: %s', edit_results.get('feature_service_name'))
        logger.info('Layer (0) name: %s', edit_results.get('layer_name'))
        logger.info('Paste this item ID into CONFIG.feature_layer_item_id for future refreshes.')
        display(published_item)
else:
    logger.info('CONFIG.run_publish is False. Toggle it to True in the configuration cell to publish or refresh the hosted feature layer.')

```

    2026-07-08 19:25:19 | INFO | tdf_buildings_pipeline | Creating hosted feature service. Item title: TDF Buildings and Facilities
    2026-07-08 19:25:19 | INFO | tdf_buildings_pipeline | FeatureServer service name: TDF_Buildings_and_Facilities
    2026-07-08 19:25:38 | INFO | tdf_buildings_pipeline | Add layer definition result: {'success': True, 'layers': [{'name': 'TDF Buildings and Facilities', 'id': 0}]}
    2026-07-08 19:25:39 | INFO | tdf_buildings_pipeline | Adding batch 1 with 200 features
    2026-07-08 19:25:42 | INFO | tdf_buildings_pipeline | Adding batch 2 with 138 features
    2026-07-08 19:25:46 | INFO | tdf_buildings_pipeline | Applied publish field aliases: {'success': True}
    2026-07-08 19:25:48 | INFO | tdf_buildings_pipeline | Created new hosted feature layer item ID: a2f0811ed0e44e3e8933411739302e92
    2026-07-08 19:25:48 | INFO | tdf_buildings_pipeline | FeatureServer service name: TDF_Buildings_and_Facilities
    2026-07-08 19:25:48 | INFO | tdf_buildings_pipeline | Layer (0) name: TDF Buildings and Facilities
    2026-07-08 19:25:48 | INFO | tdf_buildings_pipeline | Paste this item ID into CONFIG.feature_layer_item_id for future refreshes.



<div class="item_container" style="height: auto; overflow: hidden; border: 1px solid #cfcfcf; border-radius: 2px; background: #f6fafa; line-height: 1.21429em; padding: 10px;">
                    <div class="item_left" style="width: 210px; float: left;">
                       <a href='https://www.arcgis.com/home/item.html?id=a2f0811ed0e44e3e8933411739302e92' target='_blank'>
                        <img src='http://static.arcgis.com/images/desktopapp.png' class="itemThumbnail">
                       </a>
                    </div>

                    <div class="item_right"     style="float: none; width: auto; overflow: hidden;">
                        <a href='https://www.arcgis.com/home/item.html?id=a2f0811ed0e44e3e8933411739302e92' target='_blank'><b>TDF Buildings and Facilities</b>
                        </a>
                        <br/>Point locations for buildings owned and managed by Tennessee Division of Forestry.<br/><img src='https://www.arcgis.com/home/js/jsapi/esri/css/images/item_type_icons/featureshosted16.png' style="vertical-align:middle;" width=16 height=16>Feature Layer Collection by colin.stiles_tndof
                        <br/>Last Modified: July 08, 2026
                        <br/>0 comments, 3 views
                    </div>
                </div>



## 12. Run Summary

Write a JSON summary for troubleshooting and auditability.


```python
def write_run_summary(summary: dict[str, object], config: NotebookConfig = CONFIG) -> Path:
    """Write a JSON run summary to the logs folder."""
    summary_path = config.log_dir / f"{config.project_name}_{RUN_ID}_summary.json"
    with summary_path.open("w", encoding="utf-8") as file:
        json.dump(summary, file, indent=2, default=str)
    logger.info("Wrote run summary: %s", summary_path)
    return summary_path

run_summary = {
    "project_name": CONFIG.project_name,
    "run_id": RUN_ID,
    "started_utc": RUN_STARTED_UTC.replace(microsecond=0).isoformat(),
    "completed_utc": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
    "run_geocoding": CONFIG.run_geocoding,
    "run_publish": CONFIG.run_publish,
    "geocode_mode": CONFIG.geocode_mode,
    "geocode_cache_version": CONFIG.geocode_cache_version,
    "force_regeocode": CONFIG.force_regeocode,
    "max_allowed_suspect_assignments": CONFIG.max_allowed_suspect_assignments,
    "minimum_publish_ready_records": CONFIG.minimum_publish_ready_records,
    "write_full_geocode_review": CONFIG.write_full_geocode_review,
    "source_path": str(CONFIG.source_path),
    "manual_overrides_csv": str(MANUAL_OVERRIDES_CSV),
    "manual_override_audit_csv": str(MANUAL_OVERRIDE_AUDIT_CSV),
    "normalized_csv": str(NORMALIZED_CSV),
    "qa_report_csv": str(QA_REPORT_CSV),
    "address_review_csv": str(ADDRESS_REVIEW_CSV),
    "qa_review_xlsx": str(QA_REVIEW_XLSX),
    "geocode_cache_csv": str(GEOCODE_CACHE_CSV),
    "geocoded_csv": str(GEOCODED_CSV),
    "geocode_validation_csv": str(GEOCODE_VALIDATION_CSV),
    "publish_fields_preview_csv": str(PUBLISH_FIELDS_PREVIEW_CSV),
    "publish_title": CONFIG.publish_title,
    "publish_service_name": CONFIG.publish_service_name,
    "publish_layer_name": CONFIG.publish_layer_name,
    "publish_attribute_fields": list(PUBLISH_ATTRIBUTE_FIELDS),
}

if 'df' in locals():
    run_summary['total_records'] = len(df)
    run_summary['records_with_any_manual_override'] = int(df['has_any_manual_override'].sum()) if 'has_any_manual_override' in df.columns else None
    run_summary['records_with_address_overrides'] = int(df['has_address_override'].sum()) if 'has_address_override' in df.columns else None
    run_summary['records_with_valid_manual_coordinates'] = int(df['manual_coordinate_valid'].sum()) if 'manual_coordinate_valid' in df.columns else None
    run_summary['records_accepting_geocode_candidate'] = int(df['accepted_geocode_candidate'].sum()) if 'accepted_geocode_candidate' in df.columns else None
    run_summary['records_excluded_from_publish_by_override'] = int(df['exclude_from_publish_override'].sum()) if 'exclude_from_publish_override' in df.columns else None
    run_summary['manual_location_review_needed'] = int(df['manual_location_review_needed'].sum()) if 'manual_location_review_needed' in df.columns else None
    run_summary['potential_typo_review_needed'] = int(df['potential_typo_review_needed'].sum()) if 'potential_typo_review_needed' in df.columns else None
    run_summary['zip_conflict_records'] = int(df['zip_conflict_for_address_city'].sum()) if 'zip_conflict_for_address_city' in df.columns else None
    run_summary['unique_address_keys'] = int(df['address_key'].nunique()) if 'address_key' in df.columns else None
    run_summary['unique_geocode_eligible_addresses'] = len(unique_addresses) if 'unique_addresses' in locals() else None
    
if 'geocode_cache_status' in locals():
    run_summary['geocode_cache_status'] = geocode_cache_status.to_dict(orient='records')

if 'geocode_summary' in locals():
    run_summary['geocode_summary'] = geocode_summary.to_dict(orient='records')
    run_summary['assignment_suspect_count'] = assignment_suspect_count if 'assignment_suspect_count' in locals() else None
    run_summary['records_with_coordinates'] = coordinate_count if 'coordinate_count' in locals() else None
    run_summary['records_needing_geocode_review'] = geocode_review_count if 'geocode_review_count' in locals() else None
    run_summary['publish_ready_records'] = publish_ready_count if 'publish_ready_count' in locals() else None
    run_summary['manually_excluded_records'] = manual_excluded_count if 'manual_excluded_count' in locals() else None
    
if 'sdf' in locals():
    run_summary['publish_candidate_records'] = len(sdf)
    run_summary['publish_candidate_fields'] = [str(col) for col in sdf.columns if str(col).lower() != 'shape']

if CONFIG.run_publish and 'edit_results' in locals():
    run_summary['publish_strategy'] = CONFIG.refresh_strategy
    run_summary['publish_item_id'] = getattr(published_item, 'id', '')
    run_summary['edit_results'] = edit_results

summary_path = write_run_summary(run_summary)

```

    2026-07-08 19:25:48 | INFO | tdf_buildings_pipeline | Wrote run summary: /arcgis/home/tdf_facilities/tdf_buildings_outputs/logs/tdf_buildings_pipeline_20260708T192506Z_summary.json


## 13. Final output checklist

Review these generated files after each run:

- `tdf_buildings_geocoded.csv` - full diagnostic output with all QA/geocoding fields.
- `tdf_buildings_geocode_validation_review.csv` - records still blocked from production publishing.
- `tdf_buildings_manual_overrides.csv` - reviewer-managed correction file; preserve this between runs.
- `tdf_buildings_publish_fields_preview.csv` - exact attribute columns selected for the hosted feature layer.

For new publishes, v10 creates the hosted feature service directly and adds a named Layer (0), avoiding the extra source File Geodatabase item. If you need the hosted layer schema physically reduced, publish a new layer with `feature_layer_item_id = ""`. Upsert can refresh records in an existing layer, but it does not remove fields that already exist on that hosted layer.

