# NISAR Project Metrics Extractor

This directory contains NISAR-specific tools for analyzing PGE job performance metrics.

## Quickstart (CRID Report)

For a full CRID metrics report (per-PGE three-level breakdowns + unified CSV + combined Excel workbook), use the orchestrator:

```bash
# After setting up the SSH tunnel (see § 7):
./run_crid_report.sh \
  --crid X05013 \
  --cluster POP1 \
  --es_url "https://localhost:9202/logstash-*/_search" \
  --netrc_os /path/to/netrc-os-<cluster> \
  --version r05.01.3 \
  --days_back 200 \
  --output_dir ./output
```

See § 7 below for full details.

## Directory Structure

```
nisar/
├── README.md                                    # This file
├── README_ENHANCED.md                           # Technical documentation for enhanced extractor
├── NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json  # NISAR beam mode configurations
├── hysds_metrics_es_extractor_enhanced.py       # Three-level breakdown (RSLC/GSLC/GCOV/INSAR/L3_SM)
├── l0b_three_level_breakdown.py                 # Three-level breakdown for L0B (rcid/beam/diag)
├── combine_breakdowns.py                        # Merge per-PGE breakdown CSVs into unified CSV
├── build_crid_workbook.py                       # Build combined multi-sheet Excel workbook
├── run_crid_report.sh                           # Orchestrator for full CRID metrics report
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

# Extract L0B metrics (auto-detects NISAR_MIXED_MODES_CONFIG in script dir)
python pge_execution_time_extractor.py \
  --pge_type=L0B \
  --data_day=2025-11-10 \
  --es_url="https://venue/mozart_es/logstash-*/_search"

# Extract L0B metrics with explicit modes config
python pge_execution_time_extractor.py \
  --pge_type=L0B \
  --data_day=2025-11-10 \
  --modes_config=NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json \
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
| `--modes_config` | Path to NISAR_MIXED_MODES_CONFIG JSON (auto-detected for L0B if not specified) |
| `--list-data-days` | List available data days and counts |
| `-v, --verbose` | Enable verbose logging |
| `-d, --debug` | Enable debug logging |

**Output CSV Format:**
```
# Summary Statistics
pge_type,job_type,data_day,instance_type,version,rcid,diagnostic_mode_flag,beam_mode,count,avg_pge_time_min,...

# Individual Job Details
job_id,match_id,data_day,instance_type,version,rcid,diagnostic_mode_flag,beam_mode,pge_execution_time_min,pcm_container_time_min
```

For L0B jobs, the additional columns are populated:
- `rcid`: Radar Config ID extracted from the product filename (e.g. `131` from `_131S_`)
- `diagnostic_mode_flag`: `0` (science), `1` (DM1), `2` (DM2), or `cal`
- `beam_mode`: Mode string from config (e.g. `L_40_DH_05_DH`)
- `version`: PCM/PGE version from job_type (e.g. `pcm_r05.00.1_pge_r05.00.5.1`)

For non-L0B PGE types, these columns are present but empty.

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

### 3. Enhanced Metrics Extractor (Three-Level Hierarchical Breakdown)

Three-level hierarchical breakdown of PGE jobs by:

1. **beam_name** (e.g., `L_40_DH_05_DH`)
2. **coverage** (`full` / `partial`)
3. **acquisition_mode** (`individual` / `mixed`)

Extracts these dimensions from the job_id, then aggregates PGE runtime and stage I/O metrics for each group.

**Supported PGE Types:** RSLC, INSAR, GSLC, GCOV, L3_SM. Their job_ids all carry the `_<coverage>_<acquisition_mode>_<beam_name>_` pattern, so the breakdown regex applies uniformly. For L0B, see § 4 below.

**Usage:**
```bash
python hysds_metrics_es_extractor_enhanced.py \
  -u https://<cluster>/mozart_es/logstash-*/_search \
  -b 200 \
  --breakdown_job "job-SCIFLO_RSLC:release-r05.01.3" \
  --nisar_config NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json
```

**Output:** `job_three_level_breakdown_<job_type>_<hostname>_<timerange>_spanning_<N>.0_days.csv` with columns:

```
job_type, instance_type, beam_name, coverage, acquisition_mode,
job_runtime_m, container_runtime_m,
stage_in_size_gb, stage_out_size_gb, stage_in_rate_mbps, stage_out_rate_mbps,
count, daily_count_avg, duration_days
```

See [README_ENHANCED.md](README_ENHANCED.md) for detailed documentation.

---

### 4. L0B Three-Level Breakdown

L0B runs before focusing and its job_id does not contain beam info. This script derives the breakdown from the L0B product filename and maps `rcid` (radar config ID) to a beam mode via the NISAR_MIXED_MODES_CONFIG.

**Breakdown levels:**
1. **rcid** (integer from L0B product filename, e.g. `156` from `_156S_`)
2. **beam_mode** (string from config, e.g., `L_40_DH_05_DH`)
3. **diagnostic_mode** (`science` / `diagnostic` / `cal`)

**Usage:**
```bash
ES_USERNAME=hysdsops ES_PASSWORD=<pw> \
python l0b_three_level_breakdown.py -v \
  -u https://<cluster>/mozart_es/logstash-*/_search \
  --job_type "job-SCIFLO_L0B:release-r05.01.3" \
  -b 200 \
  --nisar_config NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json
```

**Output CSV columns:**
```
job_type, instance_type, rcid, beam_mode, diagnostic_mode,
job_runtime_m, container_runtime_m,
count, daily_count_avg, duration_days
```
(Stage I/O metrics are not produced for L0B because the breakdown is product-ID-based.)

---

### 5. Combine Breakdowns (Unified CSV)

Merges per-PGE three-level breakdown CSVs into a single aggregated CSV with a unified schema. The two breakdown conventions (standard vs L0B) are projected onto generic `level_1`/`level_2`/`level_3` columns while preserving the native columns.

**Usage:**
```bash
python combine_breakdowns.py \
  --input_dir ./output \
  --crid X05013 \
  --cluster POP1 \
  --version r05.01.3
```

Output: `all_pge_three_level_breakdown_<crid>_<cluster>_<date>.csv` with 20 columns including `pge_type`, `level_1/2/3`, native per-convention columns, and all metrics.

---

### 6. Build CRID Workbook (Combined Excel)

Assembles a multi-sheet Excel workbook from the per-PGE CSVs and the unified CSV:

- **Summary** — per-PGE totals, weighted avg container/job runtime, bar chart
- **All PGEs (unified)** — the aggregated CSV (normalized `level_1/2/3` columns, pivot-ready)
- **<PGE>** — one sheet per PGE type with its native schema

**Usage:**
```bash
python build_crid_workbook.py \
  --input_dir ./output \
  --crid X05013 \
  --cluster POP1 \
  --version r05.01.3 \
  --days_back 200
```

---

### 7. CRID Report Workflow (Orchestrator)

`run_crid_report.sh` chains all of the above into a single command that produces the full deliverable set (per-PGE CSVs, unified CSV, combined Excel workbook) for a given CRID.

**Prerequisites:**
1. **SSH tunnel** to the cluster's Mozart OpenSearch. The cluster's public HTTPS endpoint goes through a reverse proxy that rotates credentials frequently, so tunneling directly to OpenSearch using an SSH key and the cluster's internal netrc-os credentials is the reliable path. Example:

   ```bash
   ssh -i <your-pem> -o StrictHostKeyChecking=no -N \
     -L 9202:es-mozart:9200 hysdsops@<mozart-ec2-ip> &
   ```

2. **netrc-os credential file** for the cluster (fetched from `~/.netrc-os` on the Mozart host). Single line, format:
   ```
   default login hysdsops password <token>
   ```

**Usage:**
```bash
./run_crid_report.sh \
  --crid X05013 \
  --cluster POP1 \
  --es_url "https://localhost:9202/logstash-*/_search" \
  --netrc_os /path/to/netrc-os-<cluster> \
  --version r05.01.3 \
  --days_back 200 \
  --output_dir ./output
```

**What it does:**
1. Reads credentials from `--netrc_os` and exports `ES_USERNAME` / `ES_PASSWORD`
2. Runs `hysds_metrics_es_extractor_enhanced.py` for RSLC/GSLC/GCOV/INSAR/L3_SM
3. Runs `l0b_three_level_breakdown.py` for L0B
4. Runs `combine_breakdowns.py` to aggregate into a unified CSV
5. Runs `build_crid_workbook.py` to produce the combined Excel workbook

**Job-type suffix:** defaults to `release-<version>`. If a specific PGE has a different suffix (e.g., L3_SM often ships with a `-1` patch suffix), override per-PGE with `--job_suffix_l3_sm "release-r05.01.3-1"` etc.

**Output files** (in `--output_dir`):
```
job_three_level_breakdown_<job_type>_<hostname>_*.csv   (6 per-PGE CSVs)
all_pge_three_level_breakdown_<crid>_<cluster>_<date>.csv
ALL_PGE_three_level_breakdown_<crid>_<cluster>_<date>.xlsx
```

---

### 8. Legacy Job Execution Time Extractor

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
