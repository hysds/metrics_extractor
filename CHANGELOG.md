# Changelog

All notable changes to this project will be documented in this file. 

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).


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

