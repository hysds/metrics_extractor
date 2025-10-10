#!/usr/bin/env python

# ----------------------------------------------------------------
# Job Execution Time Extractor - Enhanced version focused on wall_time analysis
#
# This script extracts job execution times using wall_time values from
# job.job_info.metrics.usage_stats and uses the lesser of the 2 values.
# Breaks down jobs by beam_name -> coverage -> acquisition_mode using regex parsing.
#
# usage
#  $ job_execution_time_extractor.py --verbose --es_url="https://my_pcm_venue/mozart_es/logstash-*/_search" --days_back=56 --breakdown_job="job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0"
#
# changelog
#   2025-01-09: Created job execution time focused extractor
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

from hysds_metrics_es_extractor import get_instance_types_by_job_type


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


def parse_job_id_patterns(job_ids, job_type):
    """
    Analyzes job_id patterns to identify meaningful substrings for hierarchical breakdown.
    Primary breakdown: beam_name (NISAR modes)
    Secondary breakdown: coverage
    Tertiary breakdown: acquisition_mode
    @param job_ids: list of job_id strings
    @param job_type: the job type being analyzed
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


def get_job_execution_times(
    session, api_url, time_start, time_end, job_type, instance_type, pattern_regex
):
    """
    Gets job execution times aggregated by three-level hierarchical breakdown: beam_name -> coverage -> acquisition_mode.
    Uses wall_time values from job.job_info.metrics.usage_stats and takes the lesser of the 2 values.
    @param session: the reusable request session
    @param api_url: the api endpoint to elasticsearch
    @param time_start: the start time constraint for ES query
    @param time_end: the end time constraint for ES query
    @param job_type: the job type constraint for ES query
    @param instance_type: the instance type constraint for ES query
    @param pattern_regex: regex pattern to extract beam_name, coverage, and acquisition_mode
    @return: dict of three-level hierarchical execution time metrics
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

    # Now get execution times for each three-level hierarchical group
    execution_time_metrics = {}

    for beam_name, coverages in hierarchical_groups.items():
        execution_time_metrics[beam_name] = {}

        for coverage, acquisition_modes in coverages.items():
            execution_time_metrics[beam_name][coverage] = {}

            for acquisition_mode, job_ids in acquisition_modes.items():
                logging.info(
                    f"Getting execution times for {beam_name} -> {coverage} -> {acquisition_mode} ({len(job_ids)} jobs)"
                )

                # Get wall_time values using a query that filters by job_id
                job_id_query = {
                    "size": len(job_ids),  # Get all jobs for this group
                    "_source": [
                        "job.job_id",
                        "job.job_info.metrics.usage_stats.wall_time",
                    ],
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
                        f"Failed to get execution times for {beam_name} -> {coverage} -> {acquisition_mode}: {response.status_code}"
                    )
                    continue

                result = response.json()
                hits = result.get("hits", {}).get("hits", [])

                # Debug: Print the structure of the first hit to understand the data format
                if hits:
                    logging.info(
                        f"First hit structure: {json.dumps(hits[0], indent=2, default=str)}"
                    )

                if hits:
                    # Process wall_time values for each job
                    execution_times = []
                    pcm_container_runtimes = []
                    wall_times_processed = 0

                    for hit in hits:
                        # Debug: Check the structure of hit
                        logging.debug(f"Hit type: {type(hit)}, Hit content: {hit}")

                        # Handle case where hit might be a list or dict
                        if isinstance(hit, list):
                            logging.warning(f"Unexpected list structure in hit: {hit}")
                            continue
                        elif not isinstance(hit, dict):
                            logging.warning(f"Unexpected hit type {type(hit)}: {hit}")
                            continue

                        # Extract usage_stats which is a list of objects with wall_time
                        usage_stats = (
                            hit.get("_source", {})
                            .get("job", {})
                            .get("job_info", {})
                            .get("metrics", {})
                            .get("usage_stats", [])
                        )

                        # Debug: Print usage_stats structure
                        logging.debug(
                            f"Usage stats: {usage_stats} (type: {type(usage_stats)})"
                        )

                        if (
                            usage_stats
                            and isinstance(usage_stats, list)
                            and len(usage_stats) >= 2
                        ):
                            wall_times_processed += 1

                            # Extract wall_time values from the list of objects
                            wall_time_values = []
                            for stat in usage_stats:
                                if isinstance(stat, dict) and "wall_time" in stat:
                                    wall_time_values.append(stat["wall_time"])

                            if len(wall_time_values) >= 2:
                                # Take the lesser of the 2 values for execution time (both are in nanoseconds)
                                execution_time_ns = min(
                                    wall_time_values[0], wall_time_values[1]
                                )

                                # Take the larger of the 2 values for PCM container runtime (both are in nanoseconds)
                                pcm_container_runtime_ns = max(
                                    wall_time_values[0], wall_time_values[1]
                                )

                                # Convert nanoseconds to minutes
                                execution_time_minutes = execution_time_ns / (
                                    1000000000 * 60
                                )
                                pcm_container_runtime_minutes = (
                                    pcm_container_runtime_ns / (1000000000 * 60)
                                )

                                execution_times.append(execution_time_minutes)
                                pcm_container_runtimes.append(
                                    pcm_container_runtime_minutes
                                )

                                logging.debug(
                                    f"Wall times: {wall_time_values[0]}ns, {wall_time_values[1]}ns -> Execution: {execution_time_minutes:.2f}min, PCM: {pcm_container_runtime_minutes:.2f}min"
                                )
                            elif len(wall_time_values) == 1:
                                # Single value case - use same value for both
                                wall_time_ns = wall_time_values[0]
                                wall_time_minutes = wall_time_ns / (1000000000 * 60)
                                execution_times.append(wall_time_minutes)
                                pcm_container_runtimes.append(wall_time_minutes)

                                logging.debug(
                                    f"Single wall time: {wall_time_ns}ns -> {wall_time_minutes:.2f}min"
                                )
                            else:
                                logging.warning(
                                    f"No valid wall_time values found in usage_stats: {usage_stats}"
                                )
                        else:
                            logging.debug(f"No valid usage_stats found: {usage_stats}")

                    if execution_times:
                        # Calculate statistics for execution times
                        avg_execution_time = sum(execution_times) / len(execution_times)
                        min_execution_time = min(execution_times)
                        max_execution_time = max(execution_times)

                        # Calculate statistics for PCM container runtimes
                        avg_pcm_container_runtime = sum(pcm_container_runtimes) / len(
                            pcm_container_runtimes
                        )
                        min_pcm_container_runtime = min(pcm_container_runtimes)
                        max_pcm_container_runtime = max(pcm_container_runtimes)

                        execution_time_metrics[beam_name][coverage][
                            acquisition_mode
                        ] = {
                            "avg_execution_time_minutes": avg_execution_time,
                            "min_execution_time_minutes": min_execution_time,
                            "max_execution_time_minutes": max_execution_time,
                            "avg_pcm_container_runtime_m": avg_pcm_container_runtime,
                            "min_pcm_container_runtime_m": min_pcm_container_runtime,
                            "max_pcm_container_runtime_m": max_pcm_container_runtime,
                            "count": len(execution_times),
                            "wall_times_processed": wall_times_processed,
                            "total_jobs": len(hits),
                        }

                        logging.info(
                            f"  Processed {wall_times_processed}/{len(hits)} jobs with wall_time data"
                        )
                        logging.info(
                            f"  Avg execution time: {avg_execution_time:.2f} minutes"
                        )
                        logging.info(
                            f"  Avg PCM container runtime: {avg_pcm_container_runtime:.2f} minutes"
                        )
                    else:
                        logging.warning(
                            f"  No valid wall_time data found for {beam_name} -> {coverage} -> {acquisition_mode}"
                        )

    return execution_time_metrics


def get_job_execution_time_breakdown(
    session, es_url, dt_start, dt_end, breakdown_job_type
):
    """
    Gets three-level hierarchical execution time breakdown: beam_name -> coverage -> acquisition_mode.
    @param session: the reusable request session
    @param es_url: the api endpoint to elasticsearch
    @param dt_start: the start datetime
    @param dt_end: the end datetime
    @param breakdown_job_type: the job type to break down (e.g., "job-SCIFLO_RSLC:pcm_r4.0.7_pge_r4.1.0")
    @return: dict of three-level hierarchical execution time metrics
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

    execution_time_metrics = {}

    # Use three-level hierarchical breakdown: beam_name -> coverage -> acquisition_mode
    pattern_regex = patterns.get("beam_name")  # All patterns use the same regex

    if pattern_regex:
        logging.info(
            f"Using three-level hierarchical execution time breakdown: beam_name -> coverage -> acquisition_mode"
        )

        # For each instance type
        for instance_type, instance_count in instance_types:
            logging.info(f"  Processing instance type: {instance_type}")

            # Get three-level hierarchical execution time metrics for this instance type
            instance_execution_times = get_job_execution_times(
                session,
                es_url,
                time_start,
                time_end,
                breakdown_job_type,
                instance_type,
                pattern_regex,
            )

            if instance_execution_times:
                execution_time_metrics[instance_type] = instance_execution_times

    return execution_time_metrics


def export_execution_times_to_csv(
    execution_time_metrics, breakdown_job_type, csv_filepath, duration_days
):
    """
    Exports three-level hierarchical job execution time metrics to CSV.
    @param execution_time_metrics: the three-level hierarchical execution time metrics dict
    @param breakdown_job_type: the job type being broken down
    @param csv_filepath: the output CSV file path
    @param duration_days: the duration in days for calculating daily averages
    """

    with open(csv_filepath, mode="w") as csv_out:
        logging.info(
            f"Writing three-level hierarchical execution time metrics to {csv_filepath}..."
        )
        csv_writer = csv.writer(
            csv_out, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )

        # Write header for three-level hierarchical execution time breakdown
        csv_writer.writerow(
            [
                "job_type",
                "instance_type",
                "beam_name",
                "coverage",
                "acquisition_mode",
                "avg_execution_time_minutes",
                "min_execution_time_minutes",
                "max_execution_time_minutes",
                "avg_pcm_container_runtime_m",
                "min_pcm_container_runtime_m",
                "max_pcm_container_runtime_m",
                "count",
                "wall_times_processed",
                "total_jobs",
                "daily_count_avg",
                "duration_days",
            ]
        )

        # Write data for three-level hierarchical structure: instance_type -> beam_name -> coverage -> acquisition_mode
        for instance_type, instance_breakdown in execution_time_metrics.items():
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
                                metrics["avg_execution_time_minutes"],
                                metrics["min_execution_time_minutes"],
                                metrics["max_execution_time_minutes"],
                                metrics["avg_pcm_container_runtime_m"],
                                metrics["min_pcm_container_runtime_m"],
                                metrics["max_pcm_container_runtime_m"],
                                metrics["count"],
                                metrics["wall_times_processed"],
                                metrics["total_jobs"],
                                daily_count_avg,
                                duration_days,
                            ]
                        )

    logging.info(
        f"Exported three-level hierarchical execution time metrics to {csv_filepath}"
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

    # Get hostname from URL
    hostname = urlsplit(es_url).netloc

    # Get credentials
    username = input("Username: ")
    password = getpass.getpass("Password: ")

    # Create session
    session = requests.Session()
    session.auth = (username, password)

    # Get execution time breakdown metrics
    execution_time_metrics = get_job_execution_time_breakdown(
        session, es_url, dt_start, dt_end, breakdown_job
    )

    if execution_time_metrics:
        # Export to CSV
        csv_filepath = f'job_execution_times_{breakdown_job.replace(":", "_").replace("-", "_")} {hostname} {dt_start.strftime(timestamp_format_t)}-{dt_end.strftime(timestamp_format_t)} spanning {duration_days_str} days.csv'
        export_execution_times_to_csv(
            execution_time_metrics, breakdown_job, csv_filepath, duration_days
        )

        logging.info(
            f"Execution time analysis complete. Results saved to: {csv_filepath}"
        )
    else:
        logging.warning(f"No execution time metrics found for {breakdown_job}")
