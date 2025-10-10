# Project Adaptation Template

This template provides guidance for adapting the HySDS Metrics Extractor for new projects, following the pattern established for the NISAR project.

## 📁 Project Structure Template

```
your_project/
├── README.md                                    # Project-specific documentation
├── your_project_config.json                    # Project-specific configuration
├── hysds_metrics_es_extractor_enhanced.py      # Adapted enhanced extractor
├── job_execution_time_extractor.py             # Adapted execution time extractor
└── generated_csv_files/                        # Output CSV files
    ├── job_three_level_breakdown_*.csv
    └── job_execution_times_*.csv
```

## 🔧 Adaptation Steps

### 1. Create Project Directory
```bash
mkdir -p /Users/gmanipon/dev/metrics_extractor/your_project
mkdir -p /Users/gmanipon/dev/metrics_extractor/your_project/generated_csv_files
```

### 2. Copy Base Scripts
```bash
cp /Users/gmanipon/dev/metrics_extractor/nisar/hysds_metrics_es_extractor_enhanced.py /Users/gmanipon/dev/metrics_extractor/your_project/
cp /Users/gmanipon/dev/metrics_extractor/nisar/job_execution_time_extractor.py /Users/gmanipon/dev/metrics_extractor/your_project/
```

### 3. Create Project Configuration File

Create `your_project_config.json` with your project's specific patterns:

```json
{
  "project_name": "Your Project Name",
  "job_type_pattern": "job-YOUR_JOB_TYPE:version",
  "regex_patterns": {
    "primary_breakdown": "your_primary_pattern",
    "secondary_breakdown": "your_secondary_pattern", 
    "tertiary_breakdown": "your_tertiary_pattern"
  },
  "breakdown_fields": {
    "primary": "field_name_1",
    "secondary": "field_name_2",
    "tertiary": "field_name_3"
  }
}
```

### 4. Update Scripts for Your Project

#### A. Update Job Type Filter
In both scripts, change:
```python
if 'SCIFLO_RSLC' in job_type:
```
to:
```python
if 'YOUR_JOB_TYPE' in job_type:
```

#### B. Update Regex Patterns
In `parse_job_id_patterns()`, replace the NISAR regex:
```python
patterns = {
    'beam_name': r'_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_',
    'coverage': r'_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_',
    'acquisition_mode': r'_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_',
}
```

With your project's patterns:
```python
patterns = {
    'primary_field': r'your_regex_pattern_with_named_groups',
    'secondary_field': r'your_regex_pattern_with_named_groups',
    'tertiary_field': r'your_regex_pattern_with_named_groups',
}
```

#### C. Update CSV Headers
In `export_job_breakdown_to_csv()`, update the header:
```python
csv_writer.writerow([
    'job_type', 'instance_type', 'primary_field', 'secondary_field', 'tertiary_field',
    'job_runtime_m', 'container_runtime_m', 'stage_in_size_gb', 'stage_out_size_gb',
    'stage_in_rate_mbps', 'stage_out_rate_mbps', 'count', 'daily_count_avg', 'duration_days'
])
```

#### D. Update Function Names and Documentation
- Update function docstrings to reflect your project
- Change variable names from NISAR-specific to your project-specific
- Update logging messages

### 5. Create Project Documentation

Create `README.md` for your project:
```markdown
# Your Project Metrics Extractor

## Overview
Brief description of your project and what the metrics extractor does.

## Usage
```bash
python hysds_metrics_es_extractor_enhanced.py \
  -u https://your-es-instance/mozart_es/logstash-*/_search \
  -b 56 \
  --breakdown_job "job-YOUR_JOB_TYPE:version" \
  --your_config your_project_config.json
```

## Breakdown Structure
- **Primary**: Your primary breakdown field
- **Secondary**: Your secondary breakdown field  
- **Tertiary**: Your tertiary breakdown field

## Output
Describe your CSV output structure and what insights it provides.
```

## 🎯 NISAR Project Reference

The NISAR project serves as a complete reference implementation:

- **Location**: `/Users/gmanipon/dev/metrics_extractor/nisar/`
- **Job Type**: `job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0`
- **Breakdown**: `beam_name` → `coverage` → `acquisition_mode`
- **Regex**: `_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_`

## 🔍 Testing Your Adaptation

1. **Test Job ID Extraction**: Verify your regex patterns extract the correct values
2. **Test Hierarchical Grouping**: Ensure jobs are grouped correctly
3. **Test CSV Output**: Verify the output structure matches your expectations
4. **Test with Sample Data**: Run with a small time range first

## 📊 Common Adaptation Patterns

### Single-Level Breakdown
If you only need one level of breakdown:
```python
patterns = {
    'single_field': r'your_single_regex_pattern',
}
```

### Two-Level Breakdown
For two levels:
```python
patterns = {
    'primary_field': r'your_primary_regex_pattern',
    'secondary_field': r'your_secondary_regex_pattern',
}
```

### Custom Field Names
Update the CSV headers and variable names to match your domain:
- NISAR uses: `beam_name`, `coverage`, `acquisition_mode`
- Your project might use: `mission`, `instrument`, `mode` or `region`, `season`, `type`

## 🚀 Best Practices

1. **Start Simple**: Begin with basic breakdown and add complexity
2. **Document Changes**: Keep track of what you modified from the base
3. **Test Incrementally**: Test each modification before moving to the next
4. **Use Descriptive Names**: Make variable and function names self-documenting
5. **Validate Data**: Ensure your regex patterns match your actual job IDs
6. **Handle Edge Cases**: Consider jobs that might not match your patterns

## 📞 Getting Help

- Reference the NISAR implementation for working examples
- Check the base script documentation for core functionality
- Test with verbose logging (`-v` flag) to debug issues
- Use the debug output to understand your data structure
