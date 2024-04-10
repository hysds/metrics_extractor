# Changelog

All notable changes to this project will be documented in this file. 

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


## [0.0.7] - 2024-04-10:
### Added in an optional Basic Product Estimation Sheet 
- Added in the "add_hardware_stats...", "add_network_stats..." "get_product_estimates" and "format_product_estimates" functions for product estimation
- Added in new input parameter for optionally generating the product estimates sheet

## [0.0.6] - 2024-04-02:
### Refactoed the Job Metrics Dataframe, Replaced CSV generation with a single Excel Workbook creation
- Transitioned Job Metrics storage from dictionary to pandas DataFrame for enhanced data manipulation and analysis.
- Revised the creation process for Job Counts by Metrics to leverage pandas capabilities, improving efficiency and readability.
- Replaced CSV export functionality with pandas and openpyxl integration, enabling more flexible and powerful data export options.
- Added a Dynamic Workbook naming depending on whether "days_back" or "time_start/end" were provided

## [0.0.5] - 2024-04-01:
### Refactored the following functions [get_job_metrics_aggregration, export_job_metrics_to_csv, get_counts_by_job_name, export_job_counts_to_csv]
- get_job_metrics_aggregration : Added a parameter for desired rounding. Changed the "job_metrics" data structure holding the Metrics for EC2 Instances Metrics per Job Type from a tuple, to a dictionary
- export_job_metrics_to_csv : Adjusted for the change that was made to the "job_metrics" data strucutre
- get_counts_by_job_name : Simplified the function to be faster & simpler. Also added a parameter for desired rounding. Changed the count_by_job_name to use a dictionary instead of a tuple for aggregate information
- export_job_counts_to_csv : Adjusted for the change that was made in "get_counts_by_job_name"


## [0.0.4] - 2024-03-22: 

### refactored and added export of job metric aggregates as well as totals by job name that merged versions and instance types.
- refactored to decouple job metrics csv export from extraction calls to ES.
- added export of second csv of job names by count.

## [0.0.3] - 2024-03-18: 

### cleaned up code.
- added documentation.
- added args options for --verbose and --debug to vary logger levels.

## [0.0.2] - 2024-03-15
### usability update
- added extraction of hostname from ES url and duration unit days to improve output naming scheme.

## [0.0.1] - 2024-03-12

### Created.
- Queries Metrics ES using aggregrations query API for each job type, then for each of its instance types, then metrics.
- Exports out to csv. 

