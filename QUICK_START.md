# Quick Start Guide

## 🎯 What's Available

### Core Scripts
- **`metrics_extractor/hysds_metrics_es_extractor.py`** - Base metrics extractor (general purpose)

### Project-Specific Adaptations
- **`nisar_project/`** - NISAR-specific adaptations with hierarchical breakdown

## 🚀 Quick Usage

### For General Metrics Analysis
```bash
cd /path/to/metrics_extractor/metrics_extractor
python hysds_metrics_es_extractor.py \
  -u https://your-es-instance/mozart_es/logstash-*/_search \
  -b 56
```

### For NISAR Project Analysis
```bash
cd /path/to/metrics_extractor/nisar
python hysds_metrics_es_extractor_enhanced.py \
  -u https://your-es-instance/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0" \
  --nisar_config NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json
```

### For NISAR Execution Time Analysis
```bash
cd /path/to/metrics_extractor/nisar
python job_execution_time_extractor.py \
  -u https://your-es-instance/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0"
```

## 📁 Directory Structure

```
metrics_extractor/
├── metrics_extractor/                    # Core scripts
│   └── hysds_metrics_es_extractor.py    # Base extractor
├── nisar/                               # NISAR-specific adaptations
│   ├── hysds_metrics_es_extractor_enhanced.py
│   ├── job_execution_time_extractor.py
│   ├── NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json
│   ├── generated_csv_files/
│   └── README.md
├── PROJECT_TEMPLATE.md                   # Template for new projects
└── QUICK_START.md                        # This file
```

## 🔧 Creating New Project Adaptations

1. **Follow the Template**: Use `PROJECT_TEMPLATE.md` as a guide
2. **Reference NISAR**: Look at `nisar/` for a working example
3. **Adapt Scripts**: Modify the enhanced scripts for your project's needs
4. **Test Thoroughly**: Validate with your project's data

## 📊 Output Files

### Base Extractor
- `job_aggregrates_by_version_instance_type_[hostname]_[timestamp]_spanning_[days]_days.csv`
- `job_counts_by_name_[hostname]_[timestamp]_spanning_[days]_days.csv`

### NISAR Enhanced Extractor
- `job_three_level_breakdown_[job_type]_[hostname]_[timestamp]_spanning_[days]_days.csv`

### NISAR Execution Time Extractor
- `job_execution_times_[job_type]_[hostname]_[timestamp]_spanning_[days]_days.csv`

## 🆘 Need Help?

- **General Usage**: Check the main `README.md`
- **NISAR Project**: Check `nisar/README.md`
- **Creating New Projects**: Check `PROJECT_TEMPLATE.md`
- **Technical Details**: Check `nisar/README_ENHANCED.md`
