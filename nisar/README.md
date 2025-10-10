# NISAR Project Metrics Extractor

This directory contains NISAR-specific adaptations of the HySDS Metrics Extractor for analyzing SCIFLO_RSLC job performance with hierarchical breakdown by NISAR beam modes.

## 📁 Directory Structure

```
nisar/
├── README.md                                    # This file
├── README_ENHANCED.md                          # Detailed technical documentation
├── NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json  # NISAR beam mode configurations
├── hysds_metrics_es_extractor_enhanced.py      # Enhanced metrics extractor with hierarchical breakdown
└── job_execution_time_extractor.py             # Specialized execution time analyzer
```

## 🎯 Purpose

These scripts extend the base `hysds_metrics_es_extractor.py` to provide NISAR-specific analysis capabilities:

1. **Hierarchical Job Breakdown**: Breaks down SCIFLO_RSLC jobs by:
   - **Primary**: NISAR beam modes (e.g., `L_40_DH_05_DH`, `L_20_QP_05_QP`)
   - **Secondary**: Coverage type (`full`, `partial`)
   - **Tertiary**: Acquisition mode (`individual`, `mixed`)

2. **Execution Time Analysis**: Specialized analysis of job execution times using wall_time metrics

## 🚀 Quick Start

### Prerequisites
- Python 3.7+
- Access to ElasticSearch instance with HySDS metrics
- NISAR project credentials

### Basic Usage

#### 1. Enhanced Metrics Extractor (Hierarchical Breakdown)
```bash
cd /path/to/metrics_extractor/nisar
python hysds_metrics_es_extractor_enhanced.py \
  -u https://your-es-instance/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0" \
  --nisar_config NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json
```

#### 2. Execution Time Extractor
```bash
cd /path/to/metrics_extractor/nisar
python job_execution_time_extractor.py \
  -u https://your-es-instance/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0"
```

## 📊 Output Files

### Enhanced Metrics Extractor Output
- **Filename**: `job_three_level_breakdown_job_SCIFLO_RSLC_pcm_r4.0.7_pge_r4.1.0_[hostname]_[timestamp]_spanning_[days]_days.csv`
- **Columns**: `job_type`, `instance_type`, `beam_name`, `coverage`, `acquisition_mode`, `job_runtime_m`, `container_runtime_m`, `stage_in_size_gb`, `stage_out_size_gb`, `stage_in_rate_mbps`, `stage_out_rate_mbps`, `count`, `daily_count_avg`, `duration_days`

### Execution Time Extractor Output
- **Filename**: `job_execution_times_job_SCIFLO_RSLC_pcm_r4.0.7_pge_r4.1.0_[hostname]_[timestamp]_spanning_[days]_days.csv`
- **Columns**: `job_type`, `instance_type`, `beam_name`, `coverage`, `acquisition_mode`, `avg_execution_time_minutes`, `min_execution_time_minutes`, `max_execution_time_minutes`, `avg_pcm_container_runtime_m`, `min_pcm_container_runtime_m`, `max_pcm_container_runtime_m`, `count`, `wall_times_processed`, `total_jobs`, `daily_count_avg`, `duration_days`

## 🔧 Key Features

### Regex Pattern Matching
Uses the pattern: `_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_`

### Wall Time Processing
- Extracts wall_time values from `job.job_info.metrics.usage_stats`
- Uses **lesser value** for execution time analysis
- Uses **larger value** for PCM container runtime analysis
- Converts nanoseconds to minutes for readability

### NISAR Beam Mode Support
- Loads beam modes from `NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json`
- Supports patterns matching `[LS]_\d{2}_\w{2}_\d{2}_\w{2}`

## 📈 Analysis Capabilities

### Hierarchical Breakdown Analysis
- **Beam Mode Performance**: Compare performance across different NISAR beam modes
- **Coverage Analysis**: Analyze full vs partial coverage performance
- **Acquisition Mode Comparison**: Compare individual vs mixed acquisition modes
- **Instance Type Scaling**: Analyze performance across different EC2 instance types

### Execution Time Analysis
- **Actual Job Execution**: Uses minimum wall_time (actual processing time)
- **Container Overhead**: Uses maximum wall_time (total container runtime)
- **Performance Metrics**: Average, minimum, maximum execution times
- **Statistical Analysis**: Job counts, processing rates, daily averages

## 🔄 Adapting for Other Projects

To adapt these scripts for other projects:

1. **Update Regex Patterns**: Modify the regex in `parse_job_id_patterns()` to match your job ID structure
2. **Create Configuration File**: Create a project-specific configuration file similar to `NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json`
3. **Update Job Type**: Change the job type filter from `SCIFLO_RSLC` to your project's job type
4. **Modify Breakdown Logic**: Update the hierarchical breakdown to match your project's needs

### Example Adaptation Structure
```
your_project/
├── README.md
├── your_project_config.json
├── hysds_metrics_es_extractor_enhanced.py  # Adapted version
└── job_execution_time_extractor.py         # Adapted version
```

## 📚 Documentation

- **README_ENHANCED.md**: Detailed technical documentation
- **Base Script**: `../metrics_extractor/hysds_metrics_es_extractor.py`

## 🤝 Contributing

When adapting these scripts for new projects:
1. Create a new project directory
2. Copy and modify the enhanced scripts
3. Update configuration files and regex patterns
4. Document project-specific requirements
5. Test with your project's data

## 📞 Support

For questions about NISAR-specific adaptations, refer to:
- This README for basic usage
- README_ENHANCED.md for technical details
- Base script documentation for core functionality
