# NISAR Project Metrics Extractor

This directory contains NISAR-specific tools for analyzing PGE job performance metrics.

## Directory Structure

```
nisar/
├── README.md                                    # This file
├── README_ENHANCED.md                           # Technical documentation for enhanced extractor
├── NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json  # NISAR beam mode configurations
├── hysds_metrics_es_extractor_enhanced.py       # Enhanced metrics extractor with hierarchical breakdown
├── job_execution_time_extractor.py              # Legacy RSLC execution time analyzer
├── pge_execution_time_extractor.py              # Multi-PGE execution time extractor
└── compare_pge_versions.py                      # PGE version comparison tool
```

## Tools

### 1. PGE Execution Time Extractor

Extracts job execution times for any NISAR PGE type filtered by data day.

**Supported PGE Types:** L0B, RSLC, GSLC, GCOV, INSAR, L3_SM

**Features:**
- Extracts PGE container execution time (lesser of two wall_time values)
- Filters jobs by data day (extracted from product timestamps)
- Generates match_id using all product timestamps for version comparison
- Supports credential caching via environment variables or system keychain
- Outputs CSV with summary statistics and individual job details

**Usage:**
```bash
# List available data days
python pge_execution_time_extractor.py \
  --pge_type=RSLC \
  --list-data-days \
  --es_url="https://venue/mozart_es/logstash-*/_search"

# Extract metrics for a specific data day
python pge_execution_time_extractor.py \
  --pge_type=RSLC \
  --data_day=2025-11-10 \
  --es_url="https://venue/mozart_es/logstash-*/_search"

# Extract metrics for a specific PGE version
python pge_execution_time_extractor.py \
  --pge_type=RSLC \
  --data_day=2025-11-10 \
  --job_type="job-SCIFLO_RSLC:release-r05.00.0" \
  --es_url="https://venue/mozart_es/logstash-*/_search"
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `--pge_type` | PGE type: L0B, RSLC, GSLC, GCOV, INSAR, L3_SM (required) |
| `--es_url` | Elasticsearch URL endpoint (required) |
| `--data_day` | Filter by data day in YYYY-MM-DD format |
| `--job_type` | Override job type pattern for specific version |
| `--days_back` | Days to search back (default: 30) |
| `--list-data-days` | List available data days and counts |
| `-v, --verbose` | Enable verbose logging |
| `-d, --debug` | Enable debug logging |

**Output CSV Format:**
```
# Summary Statistics
pge_type,job_type,data_day,instance_type,count,avg_pge_time_min,...

# Individual Job Details
job_id,match_id,data_day,instance_type,pge_execution_time_min,pcm_container_time_min
```

**Credential Handling:**
- Environment variables: `ES_USERNAME`, `ES_PASSWORD`
- System keychain (requires `keyring` package)
- Interactive prompt (cached to keychain if available)

---

### 2. PGE Version Comparison

Compares execution times between two PGE versions and produces an Excel report.

**Features:**
- Matches jobs by product timestamps (match_id)
- Calculates performance differences and percentage changes
- Generates Excel workbook with multiple sheets:
  - Summary: Overall statistics and breakdown by instance type
  - Matched Comparison: Side-by-side comparison of matched jobs
  - Raw Data - Old: All jobs from baseline version
  - Raw Data - New: All jobs from new version
  - Chart: Bar chart comparing average execution times

**Usage:**
```bash
# Compare two versions
python compare_pge_versions.py \
  --old_csv=RSLC_execution_times_2025-11-10_venue1.csv \
  --new_csv=RSLC_execution_times_2025-11-10_venue2.csv \
  --output=RSLC_comparison.xlsx

# With custom version labels
python compare_pge_versions.py \
  --old_csv=old.csv \
  --new_csv=new.csv \
  --old_version="r05.00.0" \
  --new_version="r05.00.5.1" \
  --output=comparison.xlsx
```

**Arguments:**
| Argument | Description |
|----------|-------------|
| `--old_csv` | CSV file for baseline version (required) |
| `--new_csv` | CSV file for new version (required) |
| `--output` | Output Excel file (default: pge_comparison.xlsx) |
| `--old_version` | Label for old version (auto-detected if not specified) |
| `--new_version` | Label for new version (auto-detected if not specified) |

---

### 3. Enhanced Metrics Extractor (Hierarchical Breakdown)

Extends the base metrics extractor with NISAR-specific hierarchical breakdown by beam modes.

**Usage:**
```bash
python hysds_metrics_es_extractor_enhanced.py \
  -u https://venue/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0" \
  --nisar_config NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json
```

See [README_ENHANCED.md](README_ENHANCED.md) for detailed documentation.

---

### 4. Legacy Job Execution Time Extractor

Original RSLC-specific execution time analyzer with beam mode breakdown.

**Usage:**
```bash
python job_execution_time_extractor.py \
  -u https://venue/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0"
```

---

## Workflow Example

Extract and compare PGE performance between two venues/versions:

```bash
# 1. Extract metrics for old version
python pge_execution_time_extractor.py \
  --pge_type=RSLC \
  --data_day=2025-11-10 \
  --job_type="job-SCIFLO_RSLC:release-r05.00.0" \
  --es_url="https://venue1/mozart_es/logstash-*/_search"

# 2. Extract metrics for new version
python pge_execution_time_extractor.py \
  --pge_type=RSLC \
  --data_day=2025-11-10 \
  --job_type="job-SCIFLO_RSLC:pcm_r05.00.1_pge_r05.00.5.1" \
  --es_url="https://venue2/mozart_es/logstash-*/_search"

# 3. Compare the two versions
python compare_pge_versions.py \
  --old_csv=RSLC_execution_times_2025-11-10_venue1.csv \
  --new_csv=RSLC_execution_times_2025-11-10_venue2.csv \
  --output=RSLC_comparison.xlsx
```

## Job Matching

Jobs are matched between versions using the `match_id` field, which contains all product timestamps:

| PGE Type | match_id Format | Example |
|----------|-----------------|---------|
| L0B | start_end | `20251110T041507_20251110T042725` |
| RSLC | start_end | `20251110T120000_20251110T120500` |
| GSLC | start_end | `20251110T120000_20251110T120500` |
| GCOV | start_end | `20251110T120000_20251110T120500` |
| L3_SM | start_end | `20251110T120000_20251110T120500` |
| INSAR | ref_start_ref_end_sec_start_sec_end | `20251108T120000_20251108T120500_20251110T120000_20251110T120500` |

## Prerequisites

- Python 3.8+
- Access to Elasticsearch endpoint with HySDS metrics
- Required packages: `requests`, `openpyxl` (for Excel output), `keyring` (optional, for credential caching)

## Support

For questions about NISAR-specific tools, refer to:
- This README for usage instructions
- [README_ENHANCED.md](README_ENHANCED.md) for hierarchical breakdown details
- Base script documentation in parent directory
