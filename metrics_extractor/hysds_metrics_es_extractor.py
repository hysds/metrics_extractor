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
#               
# ----------------------------------------------------------------
from argparse import ArgumentParser
from collections import defaultdict
import csv
from datetime import datetime, timedelta, timezone
import getpass
import json
import logging
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
# end def


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
# end def


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
# end def


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
# end def


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
# end def


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
        session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
    )
    return (hits_total_relation, hits_total_value, avg_value)
# end def


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
        session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
    )
    return (hits_total_relation, hits_total_value, avg_value)
# end def


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
        session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
    )
    return (hits_total_relation, hits_total_value, avg_value)
# end def


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
        session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
    )
    return (hits_total_relation, hits_total_value, avg_value)
# end def


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
        session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
    )
    return (hits_total_relation, hits_total_value, avg_value)
# end def


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
        session, api_url, time_start, time_end, job_type, exit_code, instance_type, field
    )
    return (hits_total_relation, hits_total_value, avg_value)
# end def


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
# end def


def export_job_metrics_to_csv(job_metrics, csv_filepath):
    """
    Exports to csv file the given aggregrations of job metrics by job_type and instance_type.
    @param job_metrics: the dict of job metrics returned by get_job_metrics_aggregration()
    @param csv_filepath: the file path to the output csv.
    """
    import csv

    with open(csv_filepath, mode="w") as csv_out:
        logging.debug(f"writing to {csv_filepath}...")
        csv_writer = csv.writer(
            csv_out, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )
        csv_writer.writerow(
            [
                "job type",
                "job runtime (minutes avg)",
                "container runtime (minutes avg)",
                "stage-in size (GB avg)",
                "stage-out size (GB avg)",
                "instance type",
                "stage-in rate (MB/s avg)",
                "stage-out rate (MB/s avg)",
                "daily count avg",
                "count over duration",
                "duration days",
            ]
        )

        for job_type in sorted(job_metrics.keys()):
            metrics_job_type = job_metrics[job_type]
            for instance_type in sorted(metrics_job_type.keys()):
                # Grab all of the metrics we've stored
                job_runtime_m = metrics_job_type[instance_type]["job_runtime_m"]
                container_runtime_m = metrics_job_type[instance_type][
                    "container_runtime_m"
                ]
                stage_in_size_gb = metrics_job_type[instance_type]["stage_in_size_gb"]
                stage_out_size_gb = metrics_job_type[instance_type]["stage_out_size_gb"]
                stage_in_rate_MBps = metrics_job_type[instance_type]["stage_in_rate_MBps"]
                stage_out_rate_MBps = metrics_job_type[instance_type][
                    "stage_out_rate_MBps"
                ]
                daily_count_mean = metrics_job_type[instance_type]["daily_count_mean"]
                count = metrics_job_type[instance_type]["count"]
                duration_days = metrics_job_type[instance_type]["duration_days"]

                logging.debug(
                    f"  {job_type} - {instance_type} : {job_runtime_m}, {container_runtime_m}, {stage_in_size_gb}, {stage_out_size_gb}, {instance_type}, {stage_in_rate_MBps}, {stage_out_rate_MBps}, {daily_count_mean}, {count}, {duration_days}"
                )
                csv_writer.writerow(
                    [
                        job_type,
                        job_runtime_m,
                        container_runtime_m,
                        stage_in_size_gb,
                        stage_out_size_gb,
                        instance_type,
                        stage_in_rate_MBps,
                        stage_out_rate_MBps,
                        daily_count_mean,
                        count,
                        duration_days,
                    ]
                )
            # end for

        # end for
    # end with open csv
    logging.info(f"...exported metrics to {csv_filepath}")
# end def


def get_counts_by_job_name(job_metrics, duration_days, desired_rounding):
    """
    Gets job names (no version) and counts that merges each job_type's versions and instance_types together.
    @param job_metrics: the dict of job metrics returned by get_job_metrics_aggregration()
    @param duration_days: Number of days over which counts are aggregated.
    @param desired_rounding: The desired amount of significant digits after the decimal point for collected metrics

    @return: Dictionary with job type names as keys and another dictionary with total counts and average counts as values.
    """

    counts_by_job_name = defaultdict(list)  # Holds counts for each job type name.

    for job_type, metrics in job_metrics.items():
        job_type_name, job_type_version = job_type.split(":")  # Split for logging.
        logging.debug(f"job_type_tokens:  {job_type_name}, {job_type_version}")

        for instance_type, instance_metrics in metrics.items():
            counts_by_job_name[job_type_name].append(instance_metrics["count"])
            logging.debug(
                f"  {job_type_name} - {job_type_version} - {instance_type} : {instance_metrics['count']} jobs over {duration_days} days"
            )

    final_counts = {}  # Final output dictionary.
    for job_type_name, counts in counts_by_job_name.items():
        count_total = sum(counts)  # Total counts for this job type.
        count_avg = round(count_total / duration_days, desired_rounding)  # Average count.
        # Store total and average count separately in a nested dictionary.
        final_counts[job_type_name] = {
            "count_total": count_total,
            "count_avg": count_avg,
            "duration_days": duration_days,
        }
        logging.debug(
            f"  {job_type_name}: Total {count_total}, Average {count_avg} over {duration_days} days."
        )

    return final_counts

def export_job_counts_to_csv(counts_by_job_name, csv_filepath):
    """
    Exports to csv file the given job name counts.
    @param counts_by_job_name: the dict of counts_by_job_name[job_type_name] = (count_avg, count_total, duration_days)
    @param csv_filepath: the file path to the output csv.
    """
    with open(csv_filepath, mode="w") as csv_out:
        logging.debug(f"writing to {csv_filepath}...")
        csv_writer = csv.writer(
            csv_out, delimiter=",", quotechar='"', quoting=csv.QUOTE_NONNUMERIC
        )
        csv_writer.writerow(
            ["job name", "daily count avg", "count over duration", "duration days"]
        )
        for job_type_name in sorted(counts_by_job_name.keys()):
            count_avg = counts_by_job_name[job_type_name]["count_avg"]
            count_total = counts_by_job_name[job_type_name]["count_total"]
            duration_days = counts_by_job_name[job_type_name]["duration_days"]

            logging.debug(
                f"  {job_type_name}: count_avg={count_avg} , count_total={count_total} , duration_days={duration_days}"
            )
            csv_writer.writerow([job_type_name, count_avg, count_total, duration_days])
        # end for
    # end with open csv
    logging.info(f"...exported metrics to {csv_filepath}")
# end def


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
        logging.error('Missing argument "days_back", or both "time_start" and "time_end"')
        raise ValueError(
            'Missing argument "days_back", or both "time_start" and "time_end"'
        )


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
    desired_rounding = 4  # Determine the amount of siginifcant digits our metrics will have

    # ----------------------------------------------------------------
    # determine start and end times.
    # if days_back is given, use based from now.
    # else look for start and end times given.

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

    # export job metrics to csv
    csv_filepath = f"job_aggregrates_by_version_instance_type {hostname} {dt_start.strftime(iso8601_basic_fmt)}-{dt_end.strftime(iso8601_basic_fmt)} spanning {str(duration_days)} days.csv"
    export_job_metrics_to_csv(job_metrics, csv_filepath)

    # ----------------------------------------------------------------
    # get the counts_by_job_name[job_type_name] = (count_avg, count_total, duration_days)

    counts_by_job_name = get_counts_by_job_name(
        job_metrics, duration_days, desired_rounding
    )

    # export job metrics to csv
    csv_filepath = f"job_counts_by_name {hostname} {dt_start.strftime(iso8601_basic_fmt)}-{dt_end.strftime(iso8601_basic_fmt)} spanning {str(duration_days)} days.csv"
    export_job_counts_to_csv(counts_by_job_name, csv_filepath)


if __name__ == "__main__":
    main()
