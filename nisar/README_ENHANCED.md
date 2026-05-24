# Enhanced HySDS Metrics Extractor - NISAR Job ID Breakdown

This enhanced version of the HySDS Metrics Extractor adds the capability to break down SCIFLO_RSLC jobs by parsing NISAR mode patterns from the `job.job_id` field in ElasticSearch documents.

## Overview

The enhanced extractor allows you to:
1. **Load NISAR mode configurations** from JSON config files
2. **Analyze job_id patterns** to understand NISAR mode structures
3. **Break down jobs by NISAR modes** (e.g., L_20_DH_05_DH, S_37_QP_00_NA)
4. **Generate detailed metrics** for each NISAR mode category
5. **Export results to CSV** for further analysis

## NISAR Mode Patterns

The extractor focuses on NISAR mode patterns that match the regex `[LS]_\d{2}_\w{2}_\d{2}_\w{2}`, such as:
- `L_20_DH_05_DH` (L-band, 20m resolution, DH polarization)
- `S_37_QP_00_NA` (S-band, 37m resolution, QP polarization)
- `L_40_SH_05_SH` (L-band, 40m resolution, SH polarization)

## Files

- `hysds_metrics_es_extractor_enhanced.py` - Main enhanced extractor script
- `analyze_job_id_patterns.py` - Script to analyze job_id patterns and suggest breakdown fields
- `run_enhanced_extractor_example.py` - Example usage script

## Usage

### Step 1: Analyze Job ID Patterns

First, analyze the job_id patterns to understand what NISAR modes are available for breakdown:

```bash
python analyze_job_id_patterns.py \
  --verbose \
  --es_url="https://your-es-host/mozart_es/logstash-*/_search" \
  --days_back=56 \
  --job_type="job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0" \
  --nisar_config="../NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json"
```

This will:
- Load NISAR mode patterns from the config file
- Query ES for sample job_ids
- Analyze NISAR mode patterns (band, resolution, polarization, etc.)
- Show which NISAR modes are found in your job_ids
- Suggest breakdown patterns based on NISAR modes

### Step 2: Run Enhanced Extractor

Once you understand the patterns, run the enhanced extractor:

```bash
python hysds_metrics_es_extractor_enhanced.py \
  --verbose \
  --es_url="https://your-es-host/mozart_es/logstash-*/_search" \
  --days_back=56 \
  --breakdown_job="job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0" \
  --nisar_config="../NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json"
```

This will:
- Load NISAR mode patterns from the config file
- Query ES for all job_ids matching the specified job type
- Parse job_ids using NISAR mode patterns
- Calculate metrics for each NISAR mode category
- Export results to a CSV file

### Step 3: Review Results

The enhanced extractor generates a CSV file with the following columns:
- `job_type` - The original job type
- `breakdown_field` - The parsed field (e.g., mission, track, frame)
- `breakdown_value` - The specific value (e.g., S1A, T001, F001)
- `instance_type` - The EC2 instance type used
- `job_runtime_m` - Average job runtime in minutes
- `container_runtime_m` - Average container runtime in minutes
- `stage_in_size_gb` - Average stage-in data size in GB
- `stage_out_size_gb` - Average stage-out data size in GB
- `stage_in_rate_mbps` - Average stage-in transfer rate in MB/s
- `stage_out_rate_mbps` - Average stage-out transfer rate in MB/s
- `count` - Total number of jobs in this category
- `daily_count_avg` - Average daily count
- `duration_days` - Duration of the analysis period

## Example Output

For SCIFLO_RSLC jobs, you might see breakdowns like:

```
job_type,breakdown_field,breakdown_value,instance_type,job_runtime_m,container_runtime_m,stage_in_size_gb,stage_out_size_gb,stage_in_rate_mbps,stage_out_rate_mbps,count,daily_count_avg,duration_days
job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0,nisar_mode,L_20_DH_05_DH,p4d.24xlarge,45.2,42.1,2.8,15.2,35.1,320.5,892,15.9,56.0
job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0,nisar_mode,S_37_QP_00_NA,p4d.24xlarge,48.7,45.3,3.1,16.8,38.2,335.2,500,8.9,56.0
job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0,band,L,p4d.24xlarge,52.1,48.9,3.2,17.1,40.3,342.8,156,2.8,56.0
job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0,band,S,p4d.24xlarge,49.8,46.2,2.9,16.5,37.8,328.9,142,2.5,56.0
job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0,polarization,DH,p4d.24xlarge,47.3,44.1,2.7,15.8,36.2,315.4,234,4.2,56.0
job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0,polarization,QP,p4d.24xlarge,51.2,47.8,3.0,16.9,39.1,338.7,198,3.5,56.0
```

## Customizing Breakdown Patterns

The enhanced extractor automatically loads NISAR mode patterns from your config file. The patterns are:

```python
patterns = {
    'nisar_mode': f'({mode_pattern})',  # NISAR mode pattern (e.g., L_20_DH_05_DH)
    'band': r'([LS])',                   # L-band or S-band
    'resolution': r'([LS])_(\d{2})',     # Band and resolution (e.g., L_20, S_37)
    'polarization': r'([LS])_\d{2}_(\w{2})',  # Band and polarization (e.g., DH, QP)
}
```

To customize patterns:
1. Modify the NISAR config file to include/exclude specific modes
2. Run the pattern analysis script to see which modes are found in your job_ids
3. The enhanced extractor will automatically use the modes from your config file

## Requirements

- Python 3.8+
- Access to ElasticSearch endpoint with HySDS metrics data
- Same requirements as the original metrics extractor

## Notes

- The enhanced extractor focuses on successful jobs (exit code 0) only
- Job_id parsing uses NISAR mode patterns from your config file
- Large datasets may take time to process due to the detailed ES queries
- The breakdown analysis is specifically designed for SCIFLO_RSLC jobs with NISAR mode patterns
- NISAR mode patterns must match the regex `[LS]_\d{2}_\w{2}_\d{2}_\w{2}` to be recognized
