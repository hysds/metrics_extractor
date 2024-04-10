#!/usr/bin/env python

# ----------------------------------------------------------------
# Extracts HySDS Metrics from ES to export out reports. These reports can be used as input to other models for anlaysis.
#
# usage
#  $ hysds_metrics_es_extractor.py --verbose --es_url="https://my_pcm_venue/mozart_es/logstash-*/_search" --days_back=21
#  $ hysds_metrics_es_extractor.py --debug --es_url="https://my_pcm_venue/metrics_es/logstash-*/_search"  --time_start=20240101T000000Z --time_end=20240313T000000Z
#
# references
#   https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations.html
#
# changelog
#   2024-03-12: Created. Queries Metrics ES using aggregrations query API for each job type, then for each of its instance types, then metrics. Exports out to csv.
#   2024-03-15: added extraction of hostname from ES url and duration unit days to improve output naming scheme.
#   2024-03-18: clean up code. added documentation. added args options for --verbose and --debug to vary logger levels.
#   2024-03-29: Refactored most of code base. The code I originally was working with was written quickly and was not Pythonic.
#               A quick overview of the major changes: Moved all imports to the top, created a main function, created functions for major tasks. Re-indented file using the command "black --line-length 90 <filename>"
#   2024-04-01: Added in a variable to determine how many digits after the decimal are desired to be reported. Fixes the "Data Conversion" error that would happen with all previously generated spreadsheets
#               Refactored the following functions [get_job_metrics_aggregration, export_job_metrics_to_csv, get_counts_by_job_name, export_job_counts_to_csv]
#                   -get_job_metrics_aggregration : Added a parameter for desired rounding. Changed the "job_metrics" data structure holding the Metrics for EC2 Instances Metrics per Job Type from a tuple, to a dictionary
#                   -export_job_metrics_to_csv : Adjusted for the change that was made to the "job_metrics" data strucutre
#                   -get_counts_by_job_name : Simplified the function to be faster & simpler. Also added a parameter for desired rounding. Changed the count_by_job_name to use a dictionary instead of a tuple for aggregate information
#                   -export_job_counts_to_csv : Adjusted for the change that was made in "get_counts_by_job_name"
#
#   2024-04-02:
#               - Transitioned Job Metrics storage from dictionary to pandas DataFrame for enhanced data manipulation and analysis.
#               - Revised the creation process for Job Counts by Metrics to leverage pandas capabilities, improving efficiency and readability.
#               - Replaced CSV export functionality with pandas and openpyxl integration, enabling more flexible and powerful data export options.
#               - Added a Dynamic Workbook naming depending on whether "days_back" or "time_start/end" were provided
# 2024-04-10:
#               - Added in the "add_hardware_stats...", "add_network_stats..." "get_product_estimates" and "format_product_estimates" functions for product estimation
#               - Added in new input parameter for optionally generating the product estimates sheet
#
#
# ----------------------------------------------------------------
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
import getpass
import json
import logging
import openpyxl
import pandas as pd
import requests
import sys
from urllib.parse import urlsplit


#### Create ERROR Codes
ERR_INVALID_INPUTS = -3


def _get_es_aggregations_buckets_keys(session, api_url, query):
    """
    Queries ES metrics given a query for response of aggregations buckets keys/counts.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param query: the ES aggregation query to buckets and keys.

    @return: list of (key,count).
    """
    headers = {"Content-Type": "application/json"}

    payload = json.dumps(query)

    query_json = json.dumps(query, indent=2)
    # logging.info( query_json )

    # https://docs.python-requests.org/en/latest/api/#sessionapi
    response = session.post(api_url, data=payload, headers=headers, verify=False)

    logging.debug(f"code: {response.status_code}")
    logging.debug(f"reason: {response.reason}")

    if response.status_code != 200:
        raise Exception(
            f"got response code {response.status_code} due to {response.reason}"
        )
    # end if

    result = response.json()

    #  example response
    # {
    #   "took": 2,
    #   "timed_out": false,
    #   "_shards": {
    #     "total": 22,
    #     "successful": 22,
    #     "skipped": 0,
    #     "failed": 0
    #   },
    #   "hits": {
    #     "total": {
    #       "value": 0,
    #       "relation": "eq"
    #     },
    #     "max_score": 0.0,
    #     "hits": []
    #   }
    #   "aggregations": {
    #     "2": {
    #       "doc_count_error_upper_bound": 0,
    #       "sum_other_doc_count": 0,
    #       "buckets": [
    #         {
    #           "key": "c5.large",
    #           "doc_count": 1286
    #         }
    #       ]
    #     }
    #   }
    # }

    pretty_json = json.dumps(result, indent=2)
    logging.debug(pretty_json)

    hits = result["hits"]
    total = hits["total"]
    try:
        hits_total_value = int(total["value"])
    except ValueError as e:
        hits_total_value = None
    # end try-except
    hits_total_relation = total["relation"]

    # use list instesad of dict to retain ordered response.
    keys_counts = list()

    if hits_total_value:
        aggregations = result["aggregations"]
        for ac in aggregations.keys():
            aggregation = aggregations[ac]

            buckets = aggregation["buckets"]

            for b in buckets:
                key = b["key"]
                count = b["doc_count"]
                keys_counts.append((key, count))
            # end for

        # end for
    # end if

    logging.debug(f"keys_counts: {keys_counts}")
    return keys_counts


def _get_es_aggregations_value(session, api_url, query):
    """
    Queries ES metrics given a query for response of aggregations first value.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param query: the ES aggregation query to values.

    @return: tuple of (hits_total, value).
    """
    headers = {"Content-Type": "application/json"}

    payload = json.dumps(query)

    # https://docs.python-requests.org/en/latest/api/#sessionapi
    response = session.post(api_url, data=payload, headers=headers, verify=False)

    logging.debug(f"code: {response.status_code}")
    logging.debug(f"reason: {response.reason}")

    result = response.json()

    #  example response
    # {
    #   "took": 2,
    #   "timed_out": false,
    #   "_shards": {
    #     "total": 22,
    #     "successful": 22,
    #     "skipped": 0,
    #     "failed": 0
    #   },
    #   "hits": {
    #     "total": {
    #       "value": 3920,
    #       "relation": "eq"
    #     },
    #     "max_score": null,
    #     "hits": []
    #   },
    #   "aggregations": {
    #     "2": {
    #       "value": 2793043426
    #     }
    #   }
    # }

    pretty_json = json.dumps(result, indent=2)
    logging.debug(pretty_json)

    hits = result["hits"]
    total = hits["total"]
    try:
        hits_total_value = int(total["value"])
    except ValueError as e:
        hits_total_value = None
    # end try-except
    hits_total_relation = total["relation"]

    value = None

    if hits_total_value:
        aggregations = result["aggregations"]
        # should only be one aggregration for this query
        for ac in aggregations.keys():
            aggregation = aggregations[ac]
            value = aggregation["value"]
        # end for
    # end if

    logging.debug(
        f"hits_total_relation: {hits_total_relation}, hits_total_value: {hits_total_value}, value: {value}"
    )
    return (hits_total_relation, hits_total_value, value)


def _get_es_aggs_avg_field(
    session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
):
    """
    Queries ES metrics for aggregations of a mean value.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param exit_code: the exit code of job constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @param field: the name of the ES field to get its averaged value.

    @return: tuple of (hits_total, value).
    """

    # to allow beyond 10,000 aggregration limit default, use "track_total_hits": True
    query = {
        "aggs": {
            "2": {"avg": {"field": field}},
        },
        "size": 0,
        "track_total_hits": True,
        "stored_fields": ["*"],
        "script_fields": {},
        "docvalue_fields": [
            {"field": "@timestamp", "format": "date_time"},
            {"field": "job.job_info.cmd_end", "format": "date_time"},
            {"field": "job.job_info.cmd_start", "format": "date_time"},
            {
                "field": "job.job_info.metrics.inputs_localized.time_end",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.inputs_localized.time_start",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.product_provenance.availability_time",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.product_provenance.processing_start_time",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.products_staged.time_end",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.products_staged.time_start",
                "format": "date_time",
            },
            {"field": "job.job_info.time_end", "format": "date_time"},
            {"field": "job.job_info.time_queued", "format": "date_time"},
            {"field": "job.job_info.time_start", "format": "date_time"},
        ],
        "_source": {"excludes": []},
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
                    {"match_phrase": {"job.job_info.status": exit_code}},
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

    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggregations_value(
        session, api_url, query
    )

    logging.debug(
        f"hits_total_relation: {hits_total_relation}, hits_total_value: {hits_total_value}, avg_value: {avg_value}"
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_job_types(session, api_url, time_start, time_end):
    """
    Queries ES metrics for list of job types and their counts.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.

    @return: list of (job_type,count)
    """

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
    query = {
        "aggs": {
            "2": {
                "terms": {
                    "field": "job_type.keyword",
                    "order": {"_count": "desc"},
                    "size": 100,
                }
            }
        },
        "size": 0,
        "stored_fields": ["*"],
        "script_fields": {},
        "docvalue_fields": [
            {"field": "@timestamp", "format": "date_time"},
            {"field": "job.job_info.cmd_end", "format": "date_time"},
            {"field": "job.job_info.cmd_start", "format": "date_time"},
            {
                "field": "job.job_info.metrics.inputs_localized.time_end",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.inputs_localized.time_start",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.product_provenance.availability_time",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.product_provenance.processing_start_time",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.products_staged.time_end",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.products_staged.time_start",
                "format": "date_time",
            },
            {"field": "job.job_info.time_end", "format": "date_time"},
            {"field": "job.job_info.time_queued", "format": "date_time"},
            {"field": "job.job_info.time_start", "format": "date_time"},
        ],
        "_source": {"excludes": []},
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
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_start,
                                "lte": time_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    }
                ],
                "should": [],
                "must_not": [],
            }
        },
    }

    # get list of (job_type,count)
    keys_counts = _get_es_aggregations_buckets_keys(session, api_url, query)

    logging.debug(f"keys_counts: {keys_counts}")
    return keys_counts


def get_instance_types_by_job_type(session, api_url, time_start, time_end, job_type):
    """
    Queries ES metrics for list of instance types for a given job type.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.

    @return: list of (instance_type,count)
    """

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
    query = {
        "aggs": {
            "2": {
                "terms": {
                    "field": "job.job_info.facts.ec2_instance_type.keyword",
                    "order": {"_count": "desc"},
                    "size": 100,
                }
            }
        },
        "size": 0,
        "stored_fields": ["*"],
        "script_fields": {},
        "docvalue_fields": [
            {"field": "@timestamp", "format": "date_time"},
            {"field": "job.job_info.cmd_end", "format": "date_time"},
            {"field": "job.job_info.cmd_start", "format": "date_time"},
            {
                "field": "job.job_info.metrics.inputs_localized.time_end",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.inputs_localized.time_start",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.product_provenance.availability_time",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.product_provenance.processing_start_time",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.products_staged.time_end",
                "format": "date_time",
            },
            {
                "field": "job.job_info.metrics.products_staged.time_start",
                "format": "date_time",
            },
            {"field": "job.job_info.time_end", "format": "date_time"},
            {"field": "job.job_info.time_queued", "format": "date_time"},
            {"field": "job.job_info.time_start", "format": "date_time"},
        ],
        "_source": {"excludes": []},
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

    # get list of (instance_type,count)
    keys_counts = _get_es_aggregations_buckets_keys(session, api_url, query)

    logging.debug(f"keys_counts: {keys_counts}")
    return keys_counts


def get_job_runtime(session, api_url, time_start, time_end, job_type, instance_type):
    """
    Gets the mean runtime of job step, which includes data stage-in, container, and stage-out.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.

    @return: mean runtime duration in seconds.
    """
    field = "job.job_info.duration"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(
        session,
        api_url,
        time_start,
        time_end,
        job_type,
        exit_code,
        instance_type,
        field,
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_container_runtime(
    session, api_url, time_start, time_end, job_type, instance_type
):
    """
    Gets the mean runtime of container step only.
    Note that this is the mean runtime of containers per job.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.

    @return: mean runtime duration in nanoseconds.
    """
    field = "job.job_info.metrics.usage_stats.wall_time"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(
        session,
        api_url,
        time_start,
        time_end,
        job_type,
        exit_code,
        instance_type,
        field,
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_stage_in_size(session, api_url, time_start, time_end, job_type, instance_type):
    """
    Gets the mean stage-in data size of the given job-instance type constraint.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.

    @return: mean stage-in data size in bytes.
    """
    field = "job.job_info.metrics.inputs_localized.disk_usage"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(
        session,
        api_url,
        time_start,
        time_end,
        job_type,
        exit_code,
        instance_type,
        field,
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_stage_in_rate(session, api_url, time_start, time_end, job_type, instance_type):
    """
    Gets the mean stage-in transfer rate of the given job-instance type constraint.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.

    @return: mean stage-in transfer rate in bytes per second.
    """
    field = "job.job_info.metrics.inputs_localized.transfer_rate"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(
        session,
        api_url,
        time_start,
        time_end,
        job_type,
        exit_code,
        instance_type,
        field,
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_stage_out_size(session, api_url, time_start, time_end, job_type, instance_type):
    """
    Gets the mean stage-out data size of the given job-instance type constraint.

    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.

    @return: mean stage-out data size in bytes.
    """
    field = "job.job_info.metrics.products_staged.disk_usage"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(
        session,
        api_url,
        time_start,
        time_end,
        job_type,
        exit_code,
        instance_type,
        field,
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_stage_out_rate(session, api_url, time_start, time_end, job_type, instance_type):
    """
    Gets the mean stage-out transfer rate of the given job-instance type constraint.

    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.

    @return: mean stage-out transfer rate in bytes per second.
    """
    field = "job.job_info.metrics.products_staged.transfer_rate"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(
        session,
        api_url,
        time_start,
        time_end,
        job_type,
        exit_code,
        instance_type,
        field,
    )
    return (hits_total_relation, hits_total_value, avg_value)


def get_job_metrics_aggregration(
    session, es_url, dt_start, dt_end, dt_days, es_timestamp_fmt, desired_rounding
):
    """
    Gets the aggregrations of job metrics by job_type and instance_type.
    For each job type, for each instance type, get metrics.
    @param session: the reusbale request session
    @param es_url: the api end point to elasticsearch.
    @param dt_start: the start time constraint for ES query.
    @param dt_end: the end time constraint for ES query.
    @param desired_rounding: The desired precision for rounding collected metrics

    @return: dict of job_metrics[job_type][instance_type] = (job_runtime_m, container_runtime_m, stage_in_size_gb, stage_out_size_gb, stage_in_rate_MBps, stage_out_rate_MBps, daily_count_mean, count, duration_days)
    """

    # ES expects datetime in format "2024-03-12T22:32:26.383Z"
    time_start = dt_start.strftime(es_timestamp_fmt)
    time_end = dt_end.strftime(es_timestamp_fmt)

    # job_metrics[job_type][instance_type] = (job_runtime_m, container_runtime_m, stage_in_size_gb, stage_out_size_gb, stage_in_rate_MBps, stage_out_rate_MBps, daily_count_mean, count, duration_days)
    job_metrics = dict()

    # for each job type
    job_type_counts = get_job_types(session, es_url, time_start, time_end)
    for job_type, job_type_count in sorted(job_type_counts):
        logging.info(f"job_type: {job_type} --------------------")
        try:
            metrics_job_type = job_metrics[job_type]
        except KeyError as e:
            metrics_job_type = dict()
            job_metrics[job_type] = metrics_job_type
        # end try-except

        # for each instance type
        instance_type_counts = get_instance_types_by_job_type(
            session, es_url, time_start, time_end, job_type
        )
        for instance_type, instance_type_count in sorted(instance_type_counts):
            logging.info(f"    instance_type: {instance_type}")
            (count_relation, count, job_runtime) = get_job_runtime(
                session, es_url, time_start, time_end, job_type, instance_type
            )
            try:
                # convert to minutes
                job_runtime_m = float(job_runtime / 60)
                logging.info(f"    job_runtime: {job_runtime_m} minutes")
            except TypeError as e:
                job_runtime_m = ""
            # end try-except
            try:
                daily_count_mean = float(count / dt_days)
                logging.info(
                    f"    count: {count_relation} {count} , {daily_count_mean} avg per day"
                )
            except TypeError as e:
                daily_count_mean = ""
            # end try-except
            (count_relation, count, container_runtime) = get_container_runtime(
                session, es_url, time_start, time_end, job_type, instance_type
            )
            try:
                # convert nanoseconds to minutes
                container_runtime_m = float(container_runtime / 1000000000 / 60)
                logging.info(f"    container_runtime: {container_runtime_m} minutes")
            except TypeError as e:
                container_runtime_m = ""
            # end try-except
            try:
                daily_count_mean = float(count / dt_days)
                logging.info(
                    f"    count: {count_relation} {count} , {daily_count_mean} avg per day"
                )
            except TypeError as e:
                daily_count_mean = ""
            # end try-except
            (count_relation, count, stage_in_size_bytes) = get_stage_in_size(
                session, es_url, time_start, time_end, job_type, instance_type
            )
            try:
                # convert bytes to GB
                stage_in_size_gb = float(stage_in_size_bytes) / 1073741824
                logging.info(f"    stage_in_size: {stage_in_size_gb} GB")
            except TypeError as e:
                stage_in_size_gb = ""
            # end try-except
            (count_relation, count, stage_in_rate_bps) = get_stage_in_rate(
                session, es_url, time_start, time_end, job_type, instance_type
            )
            try:
                # convert bytes to MB
                stage_in_rate_MBps = float(stage_in_rate_bps) / 1048576
                logging.info(f"    stage_in_rate: {stage_in_rate_MBps} MB/s")
            except TypeError as e:
                stage_in_rate_MBps = ""
            # end try-except
            (count_relation, count, stage_out_size_bytes) = get_stage_out_size(
                session, es_url, time_start, time_end, job_type, instance_type
            )
            try:
                # convert bytes to GB
                stage_out_size_gb = float(stage_out_size_bytes) / 1073741824
                logging.info(f"    stage_out_size: {stage_out_size_gb} GB")
            except TypeError as e:
                stage_out_size_gb = ""
            # end try-except
            (count_relation, count, stage_out_rate_bps) = get_stage_out_rate(
                session, es_url, time_start, time_end, job_type, instance_type
            )
            try:
                # convert bytes to MB
                stage_out_rate_MBps = float(stage_out_rate_bps) / 1048576
                logging.info(f"    stage_out_rate: {stage_out_rate_MBps} MB/s")
            except TypeError as e:
                stage_out_rate_MBps = ""
            # end try-except

            # Collect all the metrics and store them in a dictionary
            metrics_job_type[instance_type] = {
                "job_runtime_m": job_runtime_m,
                "container_runtime_m": container_runtime_m,
                "stage_in_size_gb": stage_in_size_gb,
                "stage_out_size_gb": stage_out_size_gb,
                "stage_in_rate_MBps": stage_in_rate_MBps,
                "stage_out_rate_MBps": stage_out_rate_MBps,
                "daily_count_mean": daily_count_mean,
                "count": count,
                "duration_days": dt_days,
            }

            # Round all of the Job Metric information to make things consistent and work nicer with Excel
            for metrics in metrics_job_type.values():
                for key, value in metrics.items():
                    if value is None or value == "":
                        pass  # Don't add a value...may eventually change to "N/A"
                    elif isinstance(value, float):
                        metrics[key] = round(value, desired_rounding)
        # end for
    # end for
    return job_metrics


def flatten_job_metrics_to_dataframe(job_metrics):
    """
    Flattens the nested job metrics dictionary into a pandas DataFrame. Each row in the resulting DataFrame represents a unique combination of a job type and an EC2 instance,
    including all associated metrics.

    @param job_metrics: dict, Nested dictionary of job metrics.
    @return: DataFrame, A pandas DataFrame where each row corresponds to a unique job-type-EC2 instance
             combination, with columns for each metric.
    """
    data = []

    # Iterate over the nested dictionary to flatten it into a list of dictionaries
    for job_type, instances in job_metrics.items():
        for instance_type, metrics in instances.items():
            row = {"JobType": job_type, "InstanceType": instance_type}

            row.update(metrics)
            data.append(row)

    # Convert the list of dictionaries into a DataFrame
    job_metrics_df = pd.DataFrame(data)

    return job_metrics_df


def aggregate_job_metrics_by_count(job_metrics_df):
    """
    Aggregates and recalculates the Metrics for JobTypes across different instances. Modifies Job Types to exclude version/release information, sums total counts,
    and averages daily counts without weighting, assuming consistent duration across all counts.

    @param job_metrics (pd.DataFrame): DataFrame with columns including JobType, daily_count_mean, count, duration_days, and potentially others.
    @return (pd.DataFrame): DataFrame with aggregated metrics per JobType, with columns for JobType, recalculated daily count mean, total count, and duration days.
    """

    temp_df = job_metrics_df.copy()
    temp_df["JobType"] = temp_df["JobType"].str.split(":").str[0]

    # Aggregate metrics
    aggregated_df = temp_df.groupby("JobType", as_index=False).agg(
        recalculated_daily_count_mean=pd.NamedAgg(
            column="daily_count_mean", aggfunc="mean"
        ),
        total_count=pd.NamedAgg(column="count", aggfunc="sum"),
        total_duration_days=pd.NamedAgg(
            column="duration_days", aggfunc="mean"
        ),  # Assuming consistent duration
    )

    spreadshet_column_order_for_counts = [
        "JobType",
        "recalculated_daily_count_mean",
        "total_count",
        "total_duration_days",
    ]
    aggregated_df = aggregated_df[spreadshet_column_order_for_counts]

    return aggregated_df


def get_ref_ec2_info(cost_production_estimates_filename):
    """
    Find the Cost Production Workbook within the directory where the script is run, and load it into a dateframe

    @param cost_product_estimates_filename (str) : The filename of the Cost Product Estimates Excel Workbook
    @returns ref_ec2_df (pd.Dataframe) : A dataframe containing all of the information for AWS EC2 Instances
    """

    # Hook's sheet has an empty row at the beginning. I don't know if that's supposed to be there or not, so I will just work around it
    starting_row = 1
    ref_ec2_df = pd.read_excel(
        cost_production_estimates_filename,
        sheet_name="ref_aws_ec2",
        header=starting_row,
    )

    # Hook has a column labeled "Sticky Favorite" that helps him sort the entire workbook. It's not necessary for what I'd like to do
    ref_ec2_df.drop(columns=["Sticky Favorite"], inplace=True)

    return ref_ec2_df


def add_hardware_stats_to_product_estimates(
    product_estimates_df, ref_ec2_df, minimum_ebs_size_gb, ebs_gp3_cost_in_us_west_2
):
    """
    Add in the Hardware Stats for the EC2 Instances that will be used to create Product Estimates

    @param product_estimates_df (pd.DataFrame): DataFrame of Product Estimates containing base Product Estimates
    @param ref_ec2_df (pd.Dataframe) : Dataframe containing all of the information for an EC2 Instance
    @param minimum_ebs_size_gb (int) : The minimum size in GB for EBS disk space used by a Job for Cost Estimation
    @param ebs_gp3_cost_in_us_west_2 (float) : How much it costs to host 1GB of EBS in the us-west-2 region
    @returns product_estimates (pd.Dataframe) : Dataframe containing the calculated product metrics for a HySDS cluster with newly added Hardware Stats
    """

    """
    # Looking at Hook's Cost model, it looks like the "Effective Threads" of his cost model seems to be a copy of the vCPUs"
    effective_threads_col = ref_ec2_df["vCPUs"].copy()
    ref_ec2_df["Effective Threads"] = effective_threads_col

    desired_ec2_stats = ["API Name", "Physical Processor", "vCPUs", "Instance Memory (GiB)", "Effective Threads", "GiB of Memory per vCPU", "Instance Storage" ]
    instance_stats_df = ref_ec2_df[desired_ec2_stats].copy()
    """

    desired_hardware_stats = [
        "API Name",
        "Physical Processor",
        "vCPUs",
        "Instance Memory (GiB)",
        "GiB of Memory per vCPU",
        "Instance Storage",
    ]
    instance_hardware_stats_df = ref_ec2_df[desired_hardware_stats].copy()

    ### Merge the Instance Hardware Stats with the Base Information Provided by the Metrics Extraction
    product_estimates_df = pd.merge(
        product_estimates_df, instance_hardware_stats_df, on="API Name", how="left"
    )

    # These are naturally objects in the Dataframe, must change them to actual numbers
    columns_to_make_numeric = [
        "compute instance cost ($/hr)",
        "stage_in_size_gb",
        "stage_out_size_gb",
        "job_runtime_m",
    ]
    for col in columns_to_make_numeric:
        product_estimates_df[col] = pd.to_numeric(
            product_estimates_df[col], errors="coerce"
        )

    # Estimate the EBS Size used by the PGE. It will default to 50GB.
    product_estimates_df["EBS scratch Disk General Purpose SSD gp3 Volumes (GB)"] = (
        product_estimates_df["stage_in_size_gb"]
        + product_estimates_df["stage_out_size_gb"] * 2.5
    ).clip(lower=minimum_ebs_size_gb)

    product_estimates_df[
        "EBS gp3 cost/GB/month in us-west-2"
    ] = ebs_gp3_cost_in_us_west_2

    return product_estimates_df


def add_network_stats_to_product_estimates(product_estimates_df, ref_ec2_df):
    """
    Add in the Network Stats for the EC2 Instances that will be used to create Product Estimates

    @param product_estimates (pd.Dataframe) : Dataframe containing the calculated product metrics for a HySDS cluster
    @param ref_ec2_df (pd.Dataframe) : Dataframe containing all of the information for an EC2 Instance
    @returns product_estimates (pd.Dataframe) : Dataframe containing the calculated product metrics for a HySDS cluster with newly added network stats
    """

    network_performance_df = ref_ec2_df[["API Name", "Network Performance"]].copy()
    product_estimates_df = pd.merge(
        product_estimates_df, network_performance_df, on="API Name", how="left"
    )

    columns_to_reorder = ["stage_in_rate_MBps", "stage_out_rate_MBps"]
    new_column_order = [
        col for col in product_estimates_df.columns if col not in columns_to_reorder
    ] + columns_to_reorder
    product_estimates_df = product_estimates_df[new_column_order]

    # Get the Data Staging Time in Minutes
    product_estimates_df["Data Stage-In time (minutes)"] = (
        (product_estimates_df["stage_in_size_gb"] * 1024)
        / product_estimates_df["stage_in_rate_MBps"]
    ) / 60
    product_estimates_df["Data Stage-Out time (minutes)"] = (
        (product_estimates_df["stage_out_size_gb"] * 1024)
        / product_estimates_df["stage_out_rate_MBps"]
    ) / 60

    # Get the Job RunTime With Data movement
    product_estimates_df["Job Runtime with data movement (hours)"] = (
        (
            product_estimates_df["job_runtime_m"]
            + product_estimates_df["Data Stage-In time (minutes)"]
            + product_estimates_df["Data Stage-Out time (minutes)"]
        )
        / 60
    ).astype(float)

    product_estimates_df[
        "Total Job Runtime to use for estimating per job cost (hours)"
    ] = (product_estimates_df["job_runtime_m"] / 60)

    return product_estimates_df


def get_product_estimates(job_metrics_df, ref_ec2_df, wanted_compute_type):
    """
    Takes in the collected Job Metrics and the Reference AWS EC2 Information to create product estimates for the cost of
    a single job, for each PGE, on each EC2 Instance

    @param job_metrics_df (pd.DataFrame): DataFrame with columns including JobType, daily_count_mean, count, duration_days, and potentially others.
    @param ref_ec2_df (pd.Dataframe) : Dataframe containing all of the information for an EC2 Instance
    @param wanted_compute_type (str) : The Instance Type that will be used to determine pricing for the Estimates, e.g. (Spot, On-Demand, RI-3)
    @returns product_estimates (pd.Dataframe) : Dataframe containing the calculated product metrics for a HySDS cluster
    """

    # Start to build the Product Estimates Sheet that Hook has
    starting_production_estimate_cols = [
        "JobType",
        "job_runtime_m",
        "stage_in_size_gb",
        "stage_out_size_gb",
        "stage_in_rate_MBps",
        "stage_out_rate_MBps",
        "InstanceType",
    ]
    product_estimates_df = job_metrics_df[starting_production_estimate_cols].copy()

    # Figure out the Compute_Type I'm going to be looking at and add that to my dataframe
    product_estimates_df["Compute Billing Type"] = wanted_compute_type

    # Grab the pricing info for the aforementioned compute type from the ref_ec2_df to append only those columnds to the product_estimates_df
    pricing_info_df = ref_ec2_df[["API Name", wanted_compute_type]].copy()

    # Merge the pricing information with the product_estimates_df, drop the API_NAME as it's redudant
    product_estimates_df = product_estimates_df.merge(
        pricing_info_df, left_on="InstanceType", right_on="API Name", how="left"
    )

    # Rename the Spot (Avg) to something more useful
    product_estimates_df.rename(
        columns={wanted_compute_type: "compute instance cost ($/hr)"}, inplace=True
    )

    minimum_ebs_size_gb = 50
    ebs_gp3_cost_in_us_west_2 = 0.08  # EBS gp3 cost/GB/month in us-west-2
    product_estimates_df = add_hardware_stats_to_product_estimates(
        product_estimates_df, ref_ec2_df, minimum_ebs_size_gb, ebs_gp3_cost_in_us_west_2
    )
    product_estimates_df = add_network_stats_to_product_estimates(
        product_estimates_df, ref_ec2_df
    )

    ### Figure out the Cost of a single Job run
    # Figure out how much it "theoritically" cost to run our instance for the time that it was alive
    product_estimates_df["EC2 Instance cost (for duration of job)"] = (
        product_estimates_df["compute instance cost ($/hr)"]
        * product_estimates_df[
            "Total Job Runtime to use for estimating per job cost (hours)"
        ]
    )

    # Figure out the cost for the EBS Scratch Disk used by the Job
    product_estimates_df["EBS scratch disk cost (for duration of job)"] = (
        product_estimates_df["EBS scratch Disk General Purpose SSD gp3 Volumes (GB)"]
        * product_estimates_df["EBS gp3 cost/GB/month in us-west-2"]
        * (product_estimates_df["Job Runtime with data movement (hours)"] / 24 * 60)
    )

    # Add the two previous costs for the cost of a single run
    product_estimates_df["Cost of a single job run"] = (
        product_estimates_df["Job Runtime with data movement (hours)"]
        + product_estimates_df["EBS scratch disk cost (for duration of job)"]
    )

    # Drop the API Column that was useful for Merging Dataframes...it's a copy of the "Instance Type" column
    product_estimates_df.drop("API Name", axis=1, inplace=True)

    return product_estimates_df


# ------ Begin Excel/Formatting Functions ------
def auto_size_columns(sheet, width_buffer):
    """
    Automatically adjusts the width of all columns in the specified sheet based on the widest value in each column. Adds a small buffer for aesthetics.

    @param sheet (openpyxl.worksheet.worksheet.Worksheet): The sheet to autosize columns for.
    @param width_buffer (float): The multiplication buffer wanted to increase the width of the autosized columns
    """

    for column_cells in sheet.columns:
        max_length = 0
        for cell in column_cells:
            try:
                cell_length = len(str(cell.value))
                max_length = max(max_length, cell_length)
            except TypeError:
                pass  # Handles NoneType or similar issues

        # Apply a buffer and set column width
        adjusted_width = (
            max_length + 2
        ) * width_buffer  # Adjust the multiplier as needed
        column_letter = openpyxl.utils.get_column_letter(column_cells[0].column)
        sheet.column_dimensions[column_letter].width = adjusted_width


def format_job_aggregate_metrics(sheet):
    """
    Perform the desired formatting changes for the Spreadsheet containing the Job Aggregate Metrics

    @param sheet (openpyxl.worksheet.worksheet.Worksheet): The Job Aggregate Metrics Sheet
    """
    # Autosizing columns based on the widest value in each column
    width_buffer = 1.1
    auto_size_columns(sheet, width_buffer)


def format_job_metrics_by_count(sheet):
    """
    Perform the desired formatting changes for the Spreadsheet containing the Job Count Metrics

    @param sheet (openpyxl.worksheet.worksheet.Worksheet): The Metrics by Job Count Sheet
    """
    width_buffer = 1.1
    auto_size_columns(sheet, width_buffer)


def format_product_estimates(sheet):
    """
    Perform the desired formatting changes for the Spreadsheet containing the Product Estimates
    @param sheet (openpyxl.worksheet.worksheet.Worksheet): The Metrics by Job Count Sheet
    """
    width_buffer = 0.8
    auto_size_columns(sheet, width_buffer)


def write_dfs_to_excel(dfs, workbook_name):
    """
    Writes multiple DataFrames to an Excel workbook, each DataFrame to a separate sheet.
    Applies custom formatting to each sheet.

    @param dfs (dict): A dictionary where keys are sheet names and values are pandas DataFrames.
    @param workbook_name (str): The file path of the Excel workbook to write.
    """

    with pd.ExcelWriter(workbook_name, engine="openpyxl") as writer:
        for sheet_name, df in dfs.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)
            workbook = writer.book
            sheet = workbook[sheet_name]

            # Freeze the First Column and Row of each Sheet
            sheet.freeze_panes = sheet["B2"]

            if sheet_name == "job_aggregate_metrics":
                format_job_aggregate_metrics(sheet)

            elif sheet_name == "job_metrics_by_count":
                format_job_metrics_by_count(sheet)

            elif sheet_name == "product_estimates":
                format_product_estimates(sheet)

        # Note: The workbook is automatically saved when exiting the 'with' block.
        # If you'd like to do more stuff to the workbook once all sheets are added, you can do so outside this function


# ------ End Excel/Formatting Functions ------


# ------ Begin Input Functions ------
def parse_inputs(parser) -> dict:
    """
    Parses out all of the input parameters and returns them as a dictionary

    @param parser: the parser being used to grab the inputs

    @return: a dictionary holding al the inputs
    """

    parser.add_argument(
        "-l",
        "--log_level",
        dest="log_level",
        choices=["debug", "verbose", "warning"],
        help="Set the logging level",
    )

    # Other arguments remain unchanged
    parser.add_argument(
        "-u", "--es_url", dest="es_url", help="Elasticsearch URL", metavar="URL"
    )
    parser.add_argument(
        "-b",
        "--days_back",
        dest="days_back",
        help="Number of days to look back",
        metavar="BACK",
    )
    parser.add_argument(
        "-s",
        "--time_start",
        dest="time_start",
        help="Start time for querying",
        metavar="START",
    )
    parser.add_argument(
        "-e", "--time_end", dest="time_end", help="End time for querying", metavar="END"
    )
    # The Optional File for the Production Estimate Information
    parser.add_argument(
        "--cost_estimate_file",
        dest="cost_estimate_file",
        help="Path to the cost estimate file",
        metavar="FILE",
        default=None,  # Makes this argument optional. `None` will be the value if not specified.
    )

    args_dict = parser.parse_args()

    return vars(args_dict)


def validate_inputs(input_args: dict) -> None:
    """
    Validates the presence and correctness of necessary input parameters, focusing on 'es_url', 'days_back', and the
    combination of 'time_start' and 'time_end'. Ensures that 'es_url' is provided and that either a valid 'days_back'
    integer is given or both 'time_start' and 'time_end' are provided. This validation is crucial for the operation's
    subsequent steps, ensuring that necessary inputs are present and correctly formatted.

    @param input_args: A dictionary holding all the input parameters. Expected keys are 'es_url' (string), 'days_back'
    (string representing an integer), 'time_start', and 'time_end' (both strings in "YYYYMMDDTHHMMSSZ" format).

    @return: None. The function raises exceptions if validations fail, instead of returning a value.

    @raises ValueError: Raised if 'es_url' is missing, if 'days_back' is not a positive integer, or if both
    'time_start' and 'time_end' are not provided when 'days_back' is absent or invalid. Detailed error messages are
    logged for each type of validation failure.
    """

    # Validate es_url
    es_url = input_args.get("es_url")
    if not es_url:
        logging.error('Missing argument "es_url"')
        raise ValueError('Missing argument "es_url"')
    else:
        logging.debug(f"es_url: {es_url}")

    # Attempt to parse days_back
    try:
        days_back = int(input_args.get("days_back", 0))
        if days_back > 0:
            logging.debug(f"days_back: {days_back}")
        else:
            days_back = None
    except ValueError:
        days_back = None
        logging.warning('Invalid format for "days_back", expected an integer.')

    # Attempt to get time_start and time_end without exception handling
    time_start = input_args.get("time_start")
    time_end = input_args.get("time_end")

    if time_start:
        logging.debug(f"time_start: {time_start}")
    if time_end:
        logging.debug(f"time_end: {time_end}")

    # Sanity check for required time arguments
    if not days_back and not (time_start and time_end):
        logging.error(
            'Missing argument "days_back", or both "time_start" and "time_end"'
        )
        raise ValueError(
            'Missing argument "days_back", or both "time_start" and "time_end"'
        )


# ------ End Input Functions ------


def calculate_time_range(
    days_back: str,
    time_start: str,
    time_end: str,
    es_timestamp_fmt: str,
    basic_timestamp_fmt: str,
) -> tuple:
    """
    Calculates the start and end datetime based on input parameters and external timestamp formats. Supports specifying
    a range via 'days_back' or exact start and end times using custom timestamp formats for more flexibility.

    @param days_back: Number of days before the current date to start the range, or None if using time_start and time_end.
    @param time_start: Start of the date range, to be parsed according to timestamp_format_start, or None if using days_back.
    @param time_end: End of the date range, to be parsed according to timestamp_format_end, or None if using days_back.
    @param es_timestamp_format: The detailed format (with microseconds) to parse time_start and time_end.
    @param basic_timestamp_format: The compact format (without microseconds) as an alternative for parsing.


    @return: Tuple with start datetime, end datetime, and duration in days as float.
    @raises ValueError: If parameters are missing or parsing issues arise with the dates.
    """

    # Helper Function to make parsing the date a lil' easier
    def parse_date(date_str, fmt):
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError as e:
            logging.error(
                f"Unable to parse date: {date_str} with format {fmt}. Error: {e}"
            )
            raise ValueError(
                f"Unable to parse date: {date_str} with format {fmt}. Error: {e}"
            )

    # Check to see if the user only wanted to go back a certain amount of days
    if days_back:
        try:
            days_back = int(days_back)
            dt_end = datetime.now(timezone.utc)
            dt_start = dt_end - timedelta(days=days_back)
        except ValueError:
            raise ValueError('Invalid value for "days_back", expected an integer.')
    # Only look at the specific time range asked by the user
    elif time_start and time_end:
        try:
            dt_start = parse_date(time_start, es_timestamp_fmt)
        except ValueError:
            dt_start = parse_date(time_start, basic_timestamp_fmt)

        try:
            dt_end = parse_date(time_end, es_timestamp_fmt)
        except ValueError:
            dt_end = parse_date(time_end, basic_timestamp_fmt)
    # Raise exception because the user didn't provide any time-range...may want to eventually add a deftault?
    else:
        raise ValueError(
            'Either "days_back" or both "time_start" and "time_end" must be provided along with their respective formats.'
        )

    logging.info(f"dt_start: {dt_start.isoformat()}")
    logging.info(f"dt_end: {dt_end.isoformat()}")

    # Calculate duration
    dt_duration = dt_end - dt_start
    duration_days = dt_duration.days + (dt_duration.seconds / (60 * 60 * 24))
    logging.info(f"duration_days: {duration_days}")

    return dt_start, dt_end, duration_days


def main():
    parser = ArgumentParser()
    input_args = parse_inputs(parser)

    # ----------------------------------------------------------------
    # Map the user input to logging levels
    log_level_map = {
        "debug": logging.DEBUG,
        "verbose": logging.INFO,  # Verbose typically maps to INFO
        "warning": logging.WARNING,
    }

    # Determine the logging level based on the user's input
    logging_level = log_level_map.get(
        input_args["log_level"], logging.WARNING
    )  # Default to WARNING if no input

    # Configure the logging
    logging.basicConfig(
        level=logging_level, format="%(asctime)s - %(levelname)s - %(message)s"
    )

    # ----------------------------------------------------------------
    try:
        validate_inputs(input_args)
    # Any Exception raised was logged in the 'validate_inputs' function
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(ERR_INVALID_INPUTS)

    es_url = input_args["es_url"]
    days_back = input_args["days_back"]
    time_start = input_args["time_start"]
    time_end = input_args["time_end"]

    # Will eventually want to define this better. Either with a better var-name or by being user-input
    desired_rounding = 4  # Determine how many significant figures after the decimal

    # ----------------------------------------------------------------
    # determine start and end times.
    # if days_back is given, use based from now, else look for start and end times given.

    # ES expects datetime in format "2024-03-12T22:32:26.383Z"
    es_timestamp_fmt = "%Y-%m-%dT%H:%M:%S.%fZ"

    # format "20240312T223226Z"
    iso8601_basic_fmt = "%Y%m%dT%H%M%SZ"

    dt_start, dt_end, duration_days = calculate_time_range(
        days_back, time_start, time_end, es_timestamp_fmt, iso8601_basic_fmt
    )

    # ----------------------------------------------------------------

    logging.info(f"HySDS Metrics ES url: {es_url}")
    hostname = urlsplit(es_url).netloc

    # ----------------------------------------------------------------

    if sys.stdin.isatty():
        username = input("Username: ")
        password = getpass.getpass("Password: ")
    else:
        username = sys.stdin.readline().rstrip()
        password = sys.stdin.readline().rstrip()
    # end if

    # ----------------------------------------------------------------
    # reuse session to ES

    session = requests.Session()
    session.auth = (username, password)

    # ----------------------------------------------------------------
    # get the job metrics from ES

    # for each job type, for each instance type, get metrics.
    # job_metrics[job_type][instance_type] = (job_runtime_m, container_runtime_m, stage_in_size_gb, stage_out_size_gb, stage_in_rate_MBps, stage_out_rate_MBps, daily_count_mean, count, duration_days)
    job_metrics = get_job_metrics_aggregration(
        session,
        es_url,
        dt_start,
        dt_end,
        duration_days,
        es_timestamp_fmt,
        desired_rounding,
    )

    # Create a Dataframe holding all of the Job Metrics information, making sure to only have rows with valid information in it
    job_metrics_df = flatten_job_metrics_to_dataframe(job_metrics)
    job_metrics_df = job_metrics_df[job_metrics_df["count"] != 0]

    # Reorder the Dataframe Columns to appear in a specific order
    new_order_of_metrics = [
        "JobType",
        "job_runtime_m",
        "container_runtime_m",
        "stage_in_size_gb",
        "stage_out_size_gb",
        "InstanceType",
    ]
    new_order_of_metrics.extend(
        [col for col in job_metrics_df.columns if col not in new_order_of_metrics]
    )

    job_metrics_df = job_metrics_df[new_order_of_metrics]

    # Aggregate all Metrics by Job Count, stripping away the versioning from the Job Types
    metrics_by_job_count_df = aggregate_job_metrics_by_count(job_metrics_df)

    # Determine whether or not the Product Estimation Sheet will be generated
    if input_args["cost_estimate_file"] is None:
        dataframes = {
            "job_aggregate_metrics": job_metrics_df,
            "job_metrics_by_count": metrics_by_job_count_df,
        }
    else:
        # Load Up the EC2 Information into a dataframe
        cost_production_estimates_filename = (
            "../../../cost_production_estimate_template_2024-03-26.xlsx"
        )

        # Decide what kind of EC2 Compute Types we'd like to Estimate for
        wanted_compute_type = "Spot (avg)"

        ref_ec2_df = get_ref_ec2_info(cost_production_estimates_filename)
        product_estimates_df = get_product_estimates(job_metrics_df, ref_ec2_df, wanted_compute_type)

        # Organize the Dataframes in a dictionary to keep track of them
        dataframes = {
            "job_aggregate_metrics": job_metrics_df,
            "job_metrics_by_count": metrics_by_job_count_df,
            "product_estimates": product_estimates_df,
        }

    # Save the Dataframes to an Excel Workbook
    if days_back:
        workbook_name = f"hysds_metrics_for_{hostname}_spanning_{str(int(duration_days))}_days_back_from_{dt_end.strftime(iso8601_basic_fmt)}.xlsx"
    elif time_start and time_end:
        workbook_name = f"hysds_metrics_for_{hostname}_from_{dt_start.strftime(iso8601_basic_fmt)}_to_{dt_end.strftime(iso8601_basic_fmt)}.xlsx"

    write_dfs_to_excel(dataframes, workbook_name)
    logging.info(
        f"Finished Collecting Metrics...wrote out results to {workbook_name} to the current working directory"
    )
    print(
        f"Finished Collectin Metrics...the results can be found here: {workbook_name} "
    )


if __name__ == "__main__":
    main()
