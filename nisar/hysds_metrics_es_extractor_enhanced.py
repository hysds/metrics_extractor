#!/usr/bin/env python

# ----------------------------------------------------------------
# Enhanced version of HySDS Metrics ES Extractor that can break down jobs by parsing job_id substrings
#
# This extends the original hysds_metrics_es_extractor.py to add job_id parsing capabilities
# for more granular analysis of specific job types like SCIFLO_RSLC.
#
# usage
#  $ hysds_metrics_es_extractor_enhanced.py --verbose --es_url="https://my_pcm_venue/mozart_es/logstash-*/_search" --days_back=56 --breakdown_job="job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0"
#
# changelog
#   2025-01-09: Created enhanced version with job_id parsing capabilities
# ----------------------------------------------------------------

import json
import logging
import csv
import re
from datetime import datetime, timedelta
from urllib.parse import urlsplit
import requests
import sys
import getpass
from argparse import ArgumentParser

# Import functions from the original script
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'metrics_extractor'))

from hysds_metrics_es_extractor import (
    _get_es_aggregations_buckets_keys,
    _get_es_aggregations_value,
    _get_es_aggs_avg_field,
    get_job_types,
    get_instance_types_by_job_type,
    get_job_runtime,
    get_container_runtime,
    get_stage_in_size,
    get_stage_in_rate,
    get_stage_out_size,
    get_stage_out_rate,
    get_job_metrics_aggregration,
    export_job_metrics_to_csv,
    get_counts_by_job_name,
    export_job_counts_to_csv,
)


def get_sample_job_ids(session, api_url, time_start, time_end, job_type, limit=100):
    """
    Queries ES to get sample job_id values for a specific job type to understand patterns.
    @param session: the reusable request session
    @param api_url: the api endpoint to elasticsearch
    @param time_start: the start time constraint for ES query
    @param time_end: the end time constraint for ES query
    @param job_type: the job type constraint for ES query
    @param limit: maximum number of job_ids to return
    @return: list of job_id strings
    """

    query = {
        "size": limit,
        "_source": ["job.job_id"],
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}},
                    {
                        "query_string": {
                            "query": "type.keyword:job_info",
                            "analyze_wildcard": True,
                            "time_zone": "America/Los_Angeles",
                        }
                    },
                ],
                "filter": [
                    {"match_phrase": {"job_type.keyword": job_type}},
                    {"match_phrase": {"job.job_info.status": 0}},
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_start,
                                "lte": time_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ],
                "should": [],
                "must_not": [],
            }
        },
    }

    headers = {"Content-Type": "application/json"}
    payload = json.dumps(query)

    response = session.post(api_url, data=payload, headers=headers, verify=False)

    if response.status_code != 200:
        raise Exception(
            f"got response code {response.status_code} due to {response.reason}"
        )

    result = response.json()
    job_ids = []

    for hit in result.get("hits", {}).get("hits", []):
        job_id = hit.get("_source", {}).get("job", {}).get("job_id")
        if job_id:
            job_ids.append(job_id)

    logging.info(f"Found {len(job_ids)} sample job_ids for {job_type}")
    return job_ids


def load_nisar_config(config_file_path):
    """
    Loads the NISAR mixed modes configuration file.
    @param config_file_path: path to the NISAR config JSON file
    @return: dict with mode mappings
    """
    import json

    try:
        with open(config_file_path, "r") as f:
            config = json.load(f)

        # Extract all unique mode patterns that match [LS]_\d{2}_\w{2}_\d{2}_\w{2}
        mode_patterns = set()
        for key, modes in config.items():
            if isinstance(modes, list):
                for mode in modes:
                    if re.match(r"^[LS]_\d{2}_\w{2}_\d{2}_\w{2}$", mode):
                        mode_patterns.add(mode)

        logging.info(f"Loaded {len(mode_patterns)} NISAR mode patterns from config")
        return sorted(list(mode_patterns))

    except Exception as e:
        logging.error(f"Failed to load NISAR config: {e}")
        return []


def parse_job_id_patterns(job_ids, job_type, nisar_modes=None):
    """
    Analyzes job_id patterns to identify meaningful substrings for hierarchical breakdown.
    Primary breakdown: beam_name (NISAR modes)
    Secondary breakdown: coverage
    Tertiary breakdown: acquisition_mode
    @param job_ids: list of job_id strings
    @param job_type: the job type being analyzed
    @param nisar_modes: list of NISAR mode patterns to look for
    @return: dict with parsing patterns and sample extractions
    """

    patterns = {}

    if "SCIFLO_RSLC" in job_type:
        # Use the specific regex pattern provided by user
        # Pattern: "_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_"
        patterns = {
            "beam_name": r"_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_",  # Primary: beam_name
            "coverage": r"_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_",  # Secondary: coverage
            "acquisition_mode": r"_(?P<coverage>full|partial)_(?P<acquisition_mode>individual|mixed)_(?P<beam_name>L_\d{2}_\w{2}_\d{2}_\w{2})_",  # Tertiary: acquisition_mode
        }

    # Analyze the actual job_ids to refine patterns
    sample_extractions = {}
    for pattern_name, pattern_regex in patterns.items():
        extractions = set()
        for job_id in job_ids[:20]:  # Sample first 20 for pattern analysis
            match = re.search(pattern_regex, job_id)
            if match:
                if pattern_name == "beam_name":
                    extractions.add(match.group("beam_name"))
                elif pattern_name == "coverage":
                    extractions.add(match.group("coverage"))
                elif pattern_name == "acquisition_mode":
                    extractions.add(match.group("acquisition_mode"))
        sample_extractions[pattern_name] = list(extractions)

    logging.info(f"Pattern analysis for {job_type}:")
    for pattern_name, extractions in sample_extractions.items():
        logging.info(f"  {pattern_name}: {extractions}")

    return patterns, sample_extractions


def get_job_id_breakdown_aggregation(
    session,
    api_url,
    time_start,
    time_end,
    job_type,
    instance_type,
    breakdown_field,
    breakdown_pattern,
):
    """
    Gets metrics aggregated by a specific job_id breakdown field.
    @param session: the reusable request session
    @param api_url: the api endpoint to elasticsearch
    @param time_start: the start time constraint for ES query
    @param time_end: the end time constraint for ES query
    @param job_type: the job type constraint for ES query
    @param instance_type: the instance type constraint for ES query
    @param breakdown_field: the field name for the breakdown (e.g., 'mission', 'track')
    @param breakdown_pattern: regex pattern to extract the breakdown value
    @return: dict of breakdown_value -> metrics
    """

    # First get all job_ids for this job_type/instance_type combination
    query = {
        "size": 10000,  # Get all job_ids
        "_source": ["job.job_id"],
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}},
                    {
                        "query_string": {
                            "query": "type.keyword:job_info",
                            "analyze_wildcard": True,
                            "time_zone": "America/Los_Angeles",
                        }
                    },
                ],
                "filter": [
                    {"match_phrase": {"job_type.keyword": job_type}},
                    {"match_phrase": {"job.job_info.status": 0}},
                    {
                        "match_phrase": {
                            "job.job_info.facts.ec2_instance_type.keyword": instance_type
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_start,
                                "lte": time_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ],
                "should": [],
                "must_not": [],
            }
        },
    }

    headers = {"Content-Type": "application/json"}
    payload = json.dumps(query)

    response = session.post(api_url, data=payload, headers=headers, verify=False)

    if response.status_code != 200:
        raise Exception(
            f"got response code {response.status_code} due to {response.reason}"
        )

    result = response.json()

    # Group job_ids by breakdown value
    breakdown_groups = {}

    for hit in result.get("hits", {}).get("hits", []):
        job_id = hit.get("_source", {}).get("job", {}).get("job_id")
        if job_id:
            match = re.search(breakdown_pattern, job_id)
            if match:
                breakdown_value = match.group(1)
                if breakdown_value not in breakdown_groups:
                    breakdown_groups[breakdown_value] = []
                breakdown_groups[breakdown_value].append(job_id)

    logging.info(
        f"Found breakdown groups for {breakdown_field}: {list(breakdown_groups.keys())}"
    )

    # Now get metrics for each breakdown group
    breakdown_metrics = {}

    for breakdown_value, job_ids in breakdown_groups.items():
        logging.info(
            f"Getting metrics for {breakdown_field}={breakdown_value} ({len(job_ids)} jobs)"
        )

        # Get metrics using a query that filters by job_id
        job_id_query = {
            "aggs": {
                "job_runtime": {"avg": {"field": "job.job_info.duration"}},
                "container_runtime": {
                    "avg": {"field": "job.job_info.metrics.usage_stats.wall_time"}
                },
                "stage_in_size": {
                    "avg": {"field": "job.job_info.metrics.inputs_localized.disk_usage"}
                },
                "stage_in_rate": {
                    "avg": {
                        "field": "job.job_info.metrics.inputs_localized.transfer_rate"
                    }
                },
                "stage_out_size": {
                    "avg": {"field": "job.job_info.metrics.products_staged.disk_usage"}
                },
                "stage_out_rate": {
                    "avg": {
                        "field": "job.job_info.metrics.products_staged.transfer_rate"
                    }
                },
            },
            "size": 0,
            "track_total_hits": True,
            "query": {
                "bool": {
                    "must": [
                        {"match_all": {}},
                        {
                            "query_string": {
                                "query": "type.keyword:job_info",
                                "analyze_wildcard": True,
                                "time_zone": "America/Los_Angeles",
                            }
                        },
                    ],
                    "filter": [
                        {"match_phrase": {"job_type.keyword": job_type}},
                        {"match_phrase": {"job.job_info.status": 0}},
                        {
                            "match_phrase": {
                                "job.job_info.facts.ec2_instance_type.keyword": instance_type
                            }
                        },
                        {
                            "range": {
                                "@timestamp": {
                                    "gte": time_start,
                                    "lte": time_end,
                                    "format": "strict_date_optional_time",
                                }
                            }
                        },
                        {"terms": {"job.job_info.job_id.keyword": job_ids}},
                    ],
                    "should": [],
                    "must_not": [],
                }
            },
        }

        headers = {"Content-Type": "application/json"}
        payload = json.dumps(job_id_query)

        response = session.post(api_url, data=payload, headers=headers, verify=False)

        if response.status_code != 200:
            logging.warning(
                f"Failed to get metrics for {breakdown_value}: {response.status_code}"
            )
            continue

        result = response.json()
        aggs = result.get("aggregations", {})
        hits_total = result.get("hits", {}).get("total", {}).get("value", 0)

        if hits_total > 0:
            # Convert metrics to appropriate units
            job_runtime_m = aggs.get("job_runtime", {}).get("value")
            if job_runtime_m:
                job_runtime_m = job_runtime_m / 60  # Convert to minutes

            container_runtime_m = aggs.get("container_runtime", {}).get("value")
            if container_runtime_m:
                container_runtime_m = (
                    container_runtime_m / 1000000000 / 60
                )  # Convert nanoseconds to minutes

            stage_in_size_gb = aggs.get("stage_in_size", {}).get("value")
            if stage_in_size_gb:
                stage_in_size_gb = stage_in_size_gb / 1073741824  # Convert bytes to GB

            stage_in_rate_mbps = aggs.get("stage_in_rate", {}).get("value")
            if stage_in_rate_mbps:
                stage_in_rate_mbps = stage_in_rate_mbps / 1048576  # Convert bytes to MB

            stage_out_size_gb = aggs.get("stage_out_size", {}).get("value")
            if stage_out_size_gb:
                stage_out_size_gb = (
                    stage_out_size_gb / 1073741824
                )  # Convert bytes to GB

            stage_out_rate_mbps = aggs.get("stage_out_rate", {}).get("value")
            if stage_out_rate_mbps:
                stage_out_rate_mbps = (
                    stage_out_rate_mbps / 1048576
                )  # Convert bytes to MB

            breakdown_metrics[breakdown_value] = {
                "job_runtime_m": job_runtime_m,
                "container_runtime_m": container_runtime_m,
                "stage_in_size_gb": stage_in_size_gb,
                "stage_in_rate_mbps": stage_in_rate_mbps,
                "stage_out_size_gb": stage_out_size_gb,
                "stage_out_rate_mbps": stage_out_rate_mbps,
                "count": hits_total,
            }

    return breakdown_metrics


def get_hierarchical_job_breakdown_aggregation(
    session,
    api_url,
    time_start,
    time_end,
    job_type,
    instance_type,
    nisar_mode_pattern,
    processing_type_pattern,
):
    """
    Gets metrics aggregated by hierarchical breakdown: NISAR mode (primary) -> processing type (secondary).
    @param session: the reusable request session
    @param api_url: the api endpoint to elasticsearch
    @param time_start: the start time constraint for ES query
    @param time_end: the end time constraint for ES query
    @param job_type: the job type constraint for ES query
    @param instance_type: the instance type constraint for ES query
    @param nisar_mode_pattern: regex pattern to extract NISAR mode
    @param processing_type_pattern: regex pattern to extract processing type
    @return: dict of hierarchical breakdown metrics
    """

    # First get all job_ids for this job_type/instance_type combination
    query = {
        "size": 10000,  # Get all job_ids
        "_source": ["job.job_id"],
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}},
                    {
                        "query_string": {
                            "query": "type.keyword:job_info",
                            "analyze_wildcard": True,
                            "time_zone": "America/Los_Angeles",
                        }
                    },
                ],
                "filter": [
                    {"match_phrase": {"job_type.keyword": job_type}},
                    {"match_phrase": {"job.job_info.status": 0}},
                    {
                        "match_phrase": {
                            "job.job_info.facts.ec2_instance_type.keyword": instance_type
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_start,
                                "lte": time_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ],
                "should": [],
                "must_not": [],
            }
        },
    }

    headers = {"Content-Type": "application/json"}
    payload = json.dumps(query)

    response = session.post(api_url, data=payload, headers=headers, verify=False)

    if response.status_code != 200:
        raise Exception(
            f"got response code {response.status_code} due to {response.reason}"
        )

    result = response.json()

    # Group job_ids by hierarchical breakdown: NISAR mode -> processing type
    hierarchical_groups = {}

    for hit in result.get("hits", {}).get("hits", []):
        job_id = hit.get("_source", {}).get("job", {}).get("job_id")
        if job_id:
            # Extract NISAR mode (primary)
            nisar_match = re.search(nisar_mode_pattern, job_id)
            if nisar_match:
                nisar_mode = nisar_match.group(1)

                # Extract processing type (secondary)
                processing_match = re.search(processing_type_pattern, job_id)
                processing_type = (
                    processing_match.group(1) if processing_match else "unknown"
                )

                # Create hierarchical structure
                if nisar_mode not in hierarchical_groups:
                    hierarchical_groups[nisar_mode] = {}
                if processing_type not in hierarchical_groups[nisar_mode]:
                    hierarchical_groups[nisar_mode][processing_type] = []

                hierarchical_groups[nisar_mode][processing_type].append(job_id)

    logging.info(f"Found hierarchical groups: {list(hierarchical_groups.keys())}")
    for nisar_mode, processing_types in hierarchical_groups.items():
        logging.info(f"  {nisar_mode}: {list(processing_types.keys())}")

    # Now get metrics for each hierarchical group
    hierarchical_metrics = {}

    for nisar_mode, processing_types in hierarchical_groups.items():
        hierarchical_metrics[nisar_mode] = {}

        for processing_type, job_ids in processing_types.items():
            logging.info(
                f"Getting metrics for {nisar_mode} -> {processing_type} ({len(job_ids)} jobs)"
            )

            # Get metrics using a query that filters by job_id
            job_id_query = {
                "aggs": {
                    "job_runtime": {"avg": {"field": "job.job_info.duration"}},
                    "container_runtime": {
                        "avg": {"field": "job.job_info.metrics.usage_stats.wall_time"}
                    },
                    "stage_in_size": {
                        "avg": {
                            "field": "job.job_info.metrics.inputs_localized.disk_usage"
                        }
                    },
                    "stage_in_rate": {
                        "avg": {
                            "field": "job.job_info.metrics.inputs_localized.transfer_rate"
                        }
                    },
                    "stage_out_size": {
                        "avg": {
                            "field": "job.job_info.metrics.products_staged.disk_usage"
                        }
                    },
                    "stage_out_rate": {
                        "avg": {
                            "field": "job.job_info.metrics.products_staged.transfer_rate"
                        }
                    },
                },
                "size": 0,
                "track_total_hits": True,
                "query": {
                    "bool": {
                        "must": [
                            {"match_all": {}},
                            {
                                "query_string": {
                                    "query": "type.keyword:job_info",
                                    "analyze_wildcard": True,
                                    "time_zone": "America/Los_Angeles",
                                }
                            },
                        ],
                        "filter": [
                            {"match_phrase": {"job_type.keyword": job_type}},
                            {"match_phrase": {"job.job_info.status": 0}},
                            {
                                "match_phrase": {
                                    "job.job_info.facts.ec2_instance_type.keyword": instance_type
                                }
                            },
                            {
                                "range": {
                                    "@timestamp": {
                                        "gte": time_start,
                                        "lte": time_end,
                                        "format": "strict_date_optional_time",
                                    }
                                }
                            },
                            {"terms": {"job.job_id.keyword": job_ids}},
                        ],
                        "should": [],
                        "must_not": [],
                    }
                },
            }

            headers = {"Content-Type": "application/json"}
            payload = json.dumps(job_id_query)

            response = session.post(
                api_url, data=payload, headers=headers, verify=False
            )

            if response.status_code != 200:
                logging.warning(
                    f"Failed to get metrics for {nisar_mode} -> {processing_type}: {response.status_code}"
                )
                continue

            result = response.json()
            aggs = result.get("aggregations", {})
            hits_total = result.get("hits", {}).get("total", {}).get("value", 0)

            if hits_total > 0:
                # Convert metrics to appropriate units
                job_runtime_m = aggs.get("job_runtime", {}).get("value")
                if job_runtime_m:
                    job_runtime_m = job_runtime_m / 60  # Convert to minutes

                container_runtime_m = aggs.get("container_runtime", {}).get("value")
                if container_runtime_m:
                    container_runtime_m = (
                        container_runtime_m / 1000000000 / 60
                    )  # Convert nanoseconds to minutes

                stage_in_size_gb = aggs.get("stage_in_size", {}).get("value")
                if stage_in_size_gb:
                    stage_in_size_gb = (
                        stage_in_size_gb / 1073741824
                    )  # Convert bytes to GB

                stage_in_rate_mbps = aggs.get("stage_in_rate", {}).get("value")
                if stage_in_rate_mbps:
                    stage_in_rate_mbps = (
                        stage_in_rate_mbps / 1048576
                    )  # Convert bytes to MB

                stage_out_size_gb = aggs.get("stage_out_size", {}).get("value")
                if stage_out_size_gb:
                    stage_out_size_gb = (
                        stage_out_size_gb / 1073741824
                    )  # Convert bytes to GB

                stage_out_rate_mbps = aggs.get("stage_out_rate", {}).get("value")
                if stage_out_rate_mbps:
                    stage_out_rate_mbps = (
                        stage_out_rate_mbps / 1048576
                    )  # Convert bytes to MB

                hierarchical_metrics[nisar_mode][processing_type] = {
                    "job_runtime_m": job_runtime_m,
                    "container_runtime_m": container_runtime_m,
                    "stage_in_size_gb": stage_in_size_gb,
                    "stage_in_rate_mbps": stage_in_rate_mbps,
                    "stage_out_size_gb": stage_out_size_gb,
                    "stage_out_rate_mbps": stage_out_rate_mbps,
                    "count": hits_total,
                }

    return hierarchical_metrics


def get_three_level_hierarchical_breakdown(
    session, api_url, time_start, time_end, job_type, instance_type, pattern_regex
):
    """
    Gets metrics aggregated by three-level hierarchical breakdown: beam_name -> coverage -> acquisition_mode.
    @param session: the reusable request session
    @param api_url: the api endpoint to elasticsearch
    @param time_start: the start time constraint for ES query
    @param time_end: the end time constraint for ES query
    @param job_type: the job type constraint for ES query
    @param instance_type: the instance type constraint for ES query
    @param pattern_regex: regex pattern to extract beam_name, coverage, and acquisition_mode
    @return: dict of three-level hierarchical breakdown metrics
    """

    # First get all job_ids for this job_type/instance_type combination
    query = {
        "size": 10000,  # Get all job_ids
        "_source": ["job.job_id"],
        "query": {
            "bool": {
                "must": [
                    {"match_all": {}},
                    {
                        "query_string": {
                            "query": "type.keyword:job_info",
                            "analyze_wildcard": True,
                            "time_zone": "America/Los_Angeles",
                        }
                    },
                ],
                "filter": [
                    {"match_phrase": {"job_type.keyword": job_type}},
                    {"match_phrase": {"job.job_info.status": 0}},
                    {
                        "match_phrase": {
                            "job.job_info.facts.ec2_instance_type.keyword": instance_type
                        }
                    },
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_start,
                                "lte": time_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ],
                "should": [],
                "must_not": [],
            }
        },
    }

    headers = {"Content-Type": "application/json"}
    payload = json.dumps(query)

    response = session.post(api_url, data=payload, headers=headers, verify=False)

    if response.status_code != 200:
        raise Exception(
            f"got response code {response.status_code} due to {response.reason}"
        )

    result = response.json()

    # Group job_ids by three-level hierarchical breakdown: beam_name -> coverage -> acquisition_mode
    hierarchical_groups = {}

    for hit in result.get("hits", {}).get("hits", []):
        job_id = hit.get("_source", {}).get("job", {}).get("job_id")
        if job_id:
            # Extract beam_name, coverage, and acquisition_mode using the regex
            match = re.search(pattern_regex, job_id)
            if match:
                beam_name = match.group("beam_name")
                coverage = match.group("coverage")
                acquisition_mode = match.group("acquisition_mode")

                # Create three-level hierarchical structure
                if beam_name not in hierarchical_groups:
                    hierarchical_groups[beam_name] = {}
                if coverage not in hierarchical_groups[beam_name]:
                    hierarchical_groups[beam_name][coverage] = {}
                if acquisition_mode not in hierarchical_groups[beam_name][coverage]:
                    hierarchical_groups[beam_name][coverage][acquisition_mode] = []

                hierarchical_groups[beam_name][coverage][acquisition_mode].append(
                    job_id
                )

    logging.info(
        f"Found three-level hierarchical groups: {list(hierarchical_groups.keys())}"
    )
    for beam_name, coverages in hierarchical_groups.items():
        logging.info(f"  {beam_name}: {list(coverages.keys())}")
        for coverage, acquisition_modes in coverages.items():
            logging.info(f"    {coverage}: {list(acquisition_modes.keys())}")

    # Now get metrics for each three-level hierarchical group
    hierarchical_metrics = {}

    for beam_name, coverages in hierarchical_groups.items():
        hierarchical_metrics[beam_name] = {}

        for coverage, acquisition_modes in coverages.items():
            hierarchical_metrics[beam_name][coverage] = {}

            for acquisition_mode, job_ids in acquisition_modes.items():
                logging.info(
                    f"Getting metrics for {beam_name} -> {coverage} -> {acquisition_mode} ({len(job_ids)} jobs)"
                )

                # Get metrics using a query that filters by job_id
                job_id_query = {
                    "aggs": {
                        "job_runtime": {"avg": {"field": "job.job_info.duration"}},
                        "container_runtime": {
                            "avg": {
                                "field": "job.job_info.metrics.usage_stats.wall_time"
                            }
                        },
                        "stage_in_size": {
                            "avg": {
                                "field": "job.job_info.metrics.inputs_localized.disk_usage"
                            }
                        },
                        "stage_in_rate": {
                            "avg": {
                                "field": "job.job_info.metrics.inputs_localized.transfer_rate"
                            }
                        },
                        "stage_out_size": {
                            "avg": {
                                "field": "job.job_info.metrics.products_staged.disk_usage"
                            }
                        },
                        "stage_out_rate": {
                            "avg": {
                                "field": "job.job_info.metrics.products_staged.transfer_rate"
                            }
                        },
                    },
                    "size": 0,
                    "track_total_hits": True,
                    "query": {
                        "bool": {
                            "must": [
                                {"match_all": {}},
                                {
                                    "query_string": {
                                        "query": "type.keyword:job_info",
                                        "analyze_wildcard": True,
                                        "time_zone": "America/Los_Angeles",
                                    }
                                },
                            ],
                            "filter": [
                                {"match_phrase": {"job_type.keyword": job_type}},
                                {"match_phrase": {"job.job_info.status": 0}},
                                {
                                    "match_phrase": {
                                        "job.job_info.facts.ec2_instance_type.keyword": instance_type
                                    }
                                },
                                {
                                    "range": {
                                        "@timestamp": {
                                            "gte": time_start,
                                            "lte": time_end,
                                            "format": "strict_date_optional_time",
                                        }
                                    }
                                },
                                {"terms": {"job.job_id.keyword": job_ids}},
                            ],
                            "should": [],
                            "must_not": [],
                        }
                    },
                }

                headers = {"Content-Type": "application/json"}
                payload = json.dumps(job_id_query)

                response = session.post(
                    api_url, data=payload, headers=headers, verify=False
                )

                if response.status_code != 200:
                    logging.warning(
                        f"Failed to get metrics for {beam_name} -> {coverage} -> {acquisition_mode}: {response.status_code}"
                    )
                    continue

                result = response.json()
                aggs = result.get("aggregations", {})
                hits_total = result.get("hits", {}).get("total", {}).get("value", 0)

                if hits_total > 0:
                    # Convert metrics to appropriate units
                    job_runtime_m = aggs.get("job_runtime", {}).get("value")
                    if job_runtime_m:
                        job_runtime_m = job_runtime_m / 60  # Convert to minutes

                    container_runtime_m = aggs.get("container_runtime", {}).get("value")
                    if container_runtime_m:
                        container_runtime_m = (
                            container_runtime_m / 1000000000 / 60
                        )  # Convert nanoseconds to minutes

                    stage_in_size_gb = aggs.get("stage_in_size", {}).get("value")
                    if stage_in_size_gb:
                        stage_in_size_gb = (
                            stage_in_size_gb / 1073741824
                        )  # Convert bytes to GB

                    stage_in_rate_mbps = aggs.get("stage_in_rate", {}).get("value")
                    if stage_in_rate_mbps:
                        stage_in_rate_mbps = (
                            stage_in_rate_mbps / 1048576
                        )  # Convert bytes to MB

                    stage_out_size_gb = aggs.get("stage_out_size", {}).get("value")
                    if stage_out_size_gb:
                        stage_out_size_gb = (
                            stage_out_size_gb / 1073741824
                        )  # Convert bytes to GB

                    stage_out_rate_mbps = aggs.get("stage_out_rate", {}).get("value")
                    if stage_out_rate_mbps:
                        stage_out_rate_mbps = (
                            stage_out_rate_mbps / 1048576
                        )  # Convert bytes to MB

                    hierarchical_metrics[beam_name][coverage][acquisition_mode] = {
                        "job_runtime_m": job_runtime_m,
                        "container_runtime_m": container_runtime_m,
                        "stage_in_size_gb": stage_in_size_gb,
                        "stage_in_rate_mbps": stage_in_rate_mbps,
                        "stage_out_size_gb": stage_out_size_gb,
                        "stage_out_rate_mbps": stage_out_rate_mbps,
                        "count": hits_total,
                    }

    return hierarchical_metrics


def get_job_breakdown_metrics(
    session, es_url, dt_start, dt_end, breakdown_job_type, nisar_config_path=None
):
    """
    Gets three-level hierarchical breakdown metrics: beam_name -> coverage -> acquisition_mode.
    @param session: the reusable request session
    @param es_url: the api endpoint to elasticsearch
    @param dt_start: the start datetime
    @param dt_end: the end datetime
    @param breakdown_job_type: the job type to break down (e.g., "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0")
    @param nisar_config_path: path to the NISAR config file (not used in this version)
    @return: dict of three-level hierarchical breakdown metrics
    """

    time_start = dt_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    time_end = dt_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    # First get sample job_ids to understand patterns
    sample_job_ids = get_sample_job_ids(
        session, es_url, time_start, time_end, breakdown_job_type
    )

    if not sample_job_ids:
        logging.warning(f"No job_ids found for {breakdown_job_type}")
        return {}

    # Analyze patterns
    patterns, sample_extractions = parse_job_id_patterns(
        sample_job_ids, breakdown_job_type
    )

    if not patterns:
        logging.warning(f"No patterns found for {breakdown_job_type}")
        return {}

    # Get instance types for this job
    instance_types = get_instance_types_by_job_type(
        session, es_url, time_start, time_end, breakdown_job_type
    )

    hierarchical_metrics = {}

    # Use three-level hierarchical breakdown: beam_name -> coverage -> acquisition_mode
    pattern_regex = patterns.get("beam_name")  # All patterns use the same regex

    if pattern_regex:
        logging.info(
            f"Using three-level hierarchical breakdown: beam_name -> coverage -> acquisition_mode"
        )

        # For each instance type
        for instance_type, instance_count in instance_types:
            logging.info(f"  Processing instance type: {instance_type}")

            # Get three-level hierarchical breakdown metrics for this instance type
            instance_hierarchical = get_three_level_hierarchical_breakdown(
                session,
                es_url,
                time_start,
                time_end,
                breakdown_job_type,
                instance_type,
                pattern_regex,
            )

            if instance_hierarchical:
                hierarchical_metrics[instance_type] = instance_hierarchical

    return hierarchical_metrics


def export_job_breakdown_to_csv(
    breakdown_metrics, breakdown_job_type, csv_filepath, duration_days
):
    """
    Exports three-level hierarchical job breakdown metrics to CSV.
    @param breakdown_metrics: the three-level hierarchical breakdown metrics dict
    @param breakdown_job_type: the job type being broken down
    @param csv_filepath: the output CSV file path
    @param duration_days: the duration in days for calculating daily averages
    """

    with open(csv_filepath, mode="w") as csv_out:
        logging.info(
            f"Writing three-level hierarchical breakdown metrics to {csv_filepath}..."
        )
        csv_writer = csv.writer(
            csv_out, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )

        # Write header for three-level hierarchical breakdown
        csv_writer.writerow(
            [
                "job_type",
                "instance_type",
                "beam_name",
                "coverage",
                "acquisition_mode",
                "job_runtime_m",
                "container_runtime_m",
                "stage_in_size_gb",
                "stage_out_size_gb",
                "stage_in_rate_mbps",
                "stage_out_rate_mbps",
                "count",
                "daily_count_avg",
                "duration_days",
            ]
        )

        # Write data for three-level hierarchical structure: instance_type -> beam_name -> coverage -> acquisition_mode
        for instance_type, instance_breakdown in breakdown_metrics.items():
            for beam_name, coverages in instance_breakdown.items():
                for coverage, acquisition_modes in coverages.items():
                    for acquisition_mode, metrics in acquisition_modes.items():
                        daily_count_avg = (
                            metrics["count"] / duration_days if duration_days > 0 else 0
                        )

                        csv_writer.writerow(
                            [
                                breakdown_job_type,
                                instance_type,
                                beam_name,
                                coverage,
                                acquisition_mode,
                                metrics["job_runtime_m"],
                                metrics["container_runtime_m"],
                                metrics["stage_in_size_gb"],
                                metrics["stage_out_size_gb"],
                                metrics["stage_in_rate_mbps"],
                                metrics["stage_out_rate_mbps"],
                                metrics["count"],
                                daily_count_avg,
                                duration_days,
                            ]
                        )

    logging.info(
        f"Exported three-level hierarchical breakdown metrics to {csv_filepath}"
    )


if __name__ == "__main__":

    # Parse input arguments
    parser = ArgumentParser()

    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-v", "--verbose", action="store_true")

    parser.add_argument("-u", "--es_url", dest="es_url", help="es_url", metavar="URL")
    parser.add_argument(
        "-b", "--days_back", dest="days_back", help="days_back", metavar="BACK"
    )
    parser.add_argument(
        "-s", "--time_start", dest="time_start", help="time_start", metavar="START"
    )
    parser.add_argument(
        "-e", "--time_end", dest="time_end", help="time_end", metavar="END"
    )
    parser.add_argument(
        "--breakdown_job",
        dest="breakdown_job",
        help="job_type to break down by job_id parsing",
        metavar="JOB_TYPE",
    )
    parser.add_argument(
        "--nisar_config",
        dest="nisar_config",
        help="path to NISAR mixed modes config file",
        metavar="CONFIG_PATH",
    )

    argsNamespace = parser.parse_args()
    args = vars(argsNamespace)

    # Set logging level
    if args["debug"]:
        logging.basicConfig(
            level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s"
        )
    elif args["verbose"]:
        logging.basicConfig(
            level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
        )
    else:
        logging.basicConfig(
            level=logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s"
        )

    # Check required arguments
    es_url = args["es_url"]
    if not es_url:
        logging.error(f'missing argument es_url "{es_url}"')
        raise Exception(f'missing argument es_url "{es_url}"')

    breakdown_job = args["breakdown_job"]
    if not breakdown_job:
        logging.error(f"missing argument breakdown_job")
        raise Exception(f"missing argument breakdown_job")

    nisar_config = args["nisar_config"]

    try:
        days_back = int(args["days_back"])
    except (TypeError, ValueError):
        days_back = None

    try:
        time_start = args["time_start"]
    except ValueError:
        time_start = None

    try:
        time_end = args["time_end"]
    except ValueError:
        time_end = None

    # Determine time range
    timestamp_format_z = "%Y-%m-%dT%H:%M:%S.%fZ"
    timestamp_format_t = "%Y%m%dT%H%M%SZ"

    if days_back:
        dt_end = datetime.utcnow()
        dt = timedelta(days=days_back)
        dt_start = dt_end - dt
    else:
        if not (time_start and time_end):
            logging.error(f'missing argument "days_back", or "time_start"/"time_end"')
            raise Exception(f'missing argument "days_back", or "time_start"/"time_end"')

        try:
            dt_start = datetime.strptime(time_start, timestamp_format_t)
        except ValueError as e:
            logging.error(
                f"unable to convert {time_start} to datetime via {timestamp_format_t}"
            )
            raise Exception(
                f"unable to convert {time_start} to datetime via {timestamp_format_t}"
            )

        try:
            dt_end = datetime.strptime(time_end, timestamp_format_t)
        except ValueError as e:
            logging.error(
                f"unable to convert {time_end} to datetime via {timestamp_format_t}"
            )
            raise Exception(
                f"unable to convert {time_end} to datetime via {timestamp_format_t}"
            )

    # Calculate duration
    dt_duration = dt_end - dt_start
    duration_days = dt_duration.days + (dt_duration.seconds / (60 * 60 * 24))
    duration_days_str = "{:.1f}".format(duration_days)

    logging.info(f"dt_start: {dt_start}")
    logging.info(f"dt_end: {dt_end}")
    logging.info(f"duration_days: {duration_days}")
    logging.info(f"HySDS Metrics ES url: {es_url}")
    logging.info(f"Breakdown job: {breakdown_job}")
    logging.info(f"NISAR config: {nisar_config}")

    # Get hostname from URL
    hostname = urlsplit(es_url).netloc

    # Get credentials
    username = input("Username: ")
    password = getpass.getpass("Password: ")

    # Create session
    session = requests.Session()
    session.auth = (username, password)

    # Get breakdown metrics using NISAR config
    breakdown_metrics = get_job_breakdown_metrics(
        session, es_url, dt_start, dt_end, breakdown_job, nisar_config
    )

    if breakdown_metrics:
        # Export to CSV
        csv_filepath = f'job_three_level_breakdown_{breakdown_job.replace(":", "_").replace("-", "_")} {hostname} {dt_start.strftime(timestamp_format_t)}-{dt_end.strftime(timestamp_format_t)} spanning {duration_days_str} days.csv'
        export_job_breakdown_to_csv(
            breakdown_metrics, breakdown_job, csv_filepath, duration_days
        )

        logging.info(f"Breakdown analysis complete. Results saved to: {csv_filepath}")
    else:
        logging.warning(f"No breakdown metrics found for {breakdown_job}")
