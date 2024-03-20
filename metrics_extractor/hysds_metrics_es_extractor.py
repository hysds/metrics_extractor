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
# ----------------------------------------------------------------


def _get_es_aggregations_buckets_keys(session, api_url, query):
    '''
    Queries ES metrics given a query for response of aggregations buckets keys/counts.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param query: the ES aggregation query to buckets and keys.
    @return: list of (key,count).
    '''
    headers =  {"Content-Type":"application/json"}

    payload = json.dumps(query)

    query_json = json.dumps(query, indent=2)
    #logging.info( query_json )

    # https://docs.python-requests.org/en/latest/api/#sessionapi
    response = session.post(api_url, data=payload, headers=headers, verify=False)

    logging.debug(f'code: {response.status_code}')
    logging.debug(f'reason: {response.reason}')

    if response.status_code != 200:
        raise Exception(f'got response code {response.status_code} due to {response.reason}')
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
    logging.debug( pretty_json )

    hits = result['hits']
    total = hits['total']
    try:
        hits_total_value = int(total['value'])
    except ValueError as e:
        hits_total_value = None
    # end try-except
    hits_total_relation = total['relation']

    # use list instesad of dict to retain ordered response.
    keys_counts = list()

    if hits_total_value:
        aggregations = result['aggregations']
        for ac in aggregations.keys():
            aggregation = aggregations[ac]

            buckets = aggregation['buckets']

            for b in buckets:
                key = b['key']
                count = b['doc_count']
                keys_counts.append( (key,count) )
            # end for

        # end for
    # end if

    logging.debug(f'keys_counts: {keys_counts}')
    return keys_counts
# end def


def _get_es_aggregations_value(session, api_url, query):
    '''
    Queries ES metrics given a query for response of aggregations first value.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param query: the ES aggregation query to values.
    @return: tuple of (hits_total, value).
    '''
    headers =  {"Content-Type":"application/json"}

    payload = json.dumps(query)

    # https://docs.python-requests.org/en/latest/api/#sessionapi
    response = session.post(api_url, data=payload, headers=headers, verify=False)

    logging.debug(f'code: {response.status_code}')
    logging.debug(f'reason: {response.reason}')

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
    logging.debug( pretty_json )

    hits = result['hits']
    total = hits['total']
    try:
        hits_total_value = int(total['value'])
    except ValueError as e:
        hits_total_value = None
    # end try-except
    hits_total_relation = total['relation']

    value = None

    if hits_total_value:
        aggregations = result['aggregations']
        # should only be one aggregration for this query
        for ac in aggregations.keys():
            aggregation = aggregations[ac]
            value = aggregation['value']
        # end for
    # end if

    logging.debug(f'hits_total_relation: {hits_total_relation}, hits_total_value: {hits_total_value}, value: {value}')
    return (hits_total_relation, hits_total_value, value)
# end def


def _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field):
    '''
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
    '''

    # to allow beyond 10,000 aggregration limit default, use "track_total_hits": True 
    query = {
      "aggs": {
        "2": {
          "avg": {
            "field": field
          }
        },
      },
      "size": 0,
      "track_total_hits": True,
      "stored_fields": [
        "*"
      ],
      "script_fields": {},
      "docvalue_fields": [
        {
          "field": "@timestamp",
          "format": "date_time"
        },
        {
          "field": "job.job_info.cmd_end",
          "format": "date_time"
        },
        {
          "field": "job.job_info.cmd_start",
          "format": "date_time"
        },
        {
          "field": "job.job_info.metrics.inputs_localized.time_end",
          "format": "date_time"
        },
        {
          "field": "job.job_info.metrics.inputs_localized.time_start",
          "format": "date_time"
        },
        {
          "field": "job.job_info.metrics.product_provenance.availability_time",
          "format": "date_time"
        },
        {
          "field": "job.job_info.metrics.product_provenance.processing_start_time",
          "format": "date_time"
        },
        {
          "field": "job.job_info.metrics.products_staged.time_end",
          "format": "date_time"
        },
        {
          "field": "job.job_info.metrics.products_staged.time_start",
          "format": "date_time"
        },
        {
          "field": "job.job_info.time_end",
          "format": "date_time"
        },
        {
          "field": "job.job_info.time_queued",
          "format": "date_time"
        },
        {
          "field": "job.job_info.time_start",
          "format": "date_time"
        }
      ],
      "_source": {
        "excludes": []
      },
      "query": {
        "bool": {
          "must": [
            {
              "match_all": {}
            },
            {
              "query_string": {
                "query": "type.keyword:job_info",
                "analyze_wildcard": True,
                "time_zone": "America/Los_Angeles"
              }
            }
          ],
          "filter": [
            {
              "match_phrase": {
                "job_type.keyword": job_type
              }
            },
            {
              "match_phrase": {
                "job.job_info.status": exit_code
              }
            },
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
                  "format": "strict_date_optional_time"
                }
              }
            }
          ],
          "should": [],
          "must_not": []
        }
      }
    }

    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggregations_value(session, api_url, query)

    logging.debug(f'hits_total_relation: {hits_total_relation}, hits_total_value: {hits_total_value}, avg_value: {avg_value}')
    return (hits_total_relation, hits_total_value, avg_value)
# end def


def get_job_types(session, api_url, time_start, time_end):
    '''
    Queries ES metrics for list of job types and their counts.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @return: list of (job_type,count)
    '''

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
    query = {
    "aggs": {
        "2": {
        "terms": {
            "field": "job_type.keyword",
            "order": {
            "_count": "desc"
            },
            "size": 100
        }
        }
    },
    "size": 0,
    "stored_fields": [
        "*"
    ],
    "script_fields": {},
    "docvalue_fields": [
        {
        "field": "@timestamp",
        "format": "date_time"
        },
        {
        "field": "job.job_info.cmd_end",
        "format": "date_time"
        },
        {
        "field": "job.job_info.cmd_start",
        "format": "date_time"
        },
        {
        "field": "job.job_info.metrics.inputs_localized.time_end",
        "format": "date_time"
        },
        {
        "field": "job.job_info.metrics.inputs_localized.time_start",
        "format": "date_time"
        },
        {
        "field": "job.job_info.metrics.product_provenance.availability_time",
        "format": "date_time"
        },
        {
        "field": "job.job_info.metrics.product_provenance.processing_start_time",
        "format": "date_time"
        },
        {
        "field": "job.job_info.metrics.products_staged.time_end",
        "format": "date_time"
        },
        {
        "field": "job.job_info.metrics.products_staged.time_start",
        "format": "date_time"
        },
        {
        "field": "job.job_info.time_end",
        "format": "date_time"
        },
        {
        "field": "job.job_info.time_queued",
        "format": "date_time"
        },
        {
        "field": "job.job_info.time_start",
        "format": "date_time"
        }
    ],
    "_source": {
        "excludes": []
    },
    "query": {
        "bool": {
        "must": [
            {
            "match_all": {}
            },
            {
            "query_string": {
                "query": "type.keyword:job_info",
                "analyze_wildcard": True,
                "time_zone": "America/Los_Angeles"
            }
            }
        ],
        "filter": [
            {
            "range": {
                "@timestamp": {
                "gte": time_start,
                "lte": time_end,
                "format": "strict_date_optional_time"
                }
            }
            }
        ],
        "should": [],
        "must_not": []
        }
    }
    }

    # get list of (job_type,count)
    keys_counts = _get_es_aggregations_buckets_keys(session, api_url, query)

    logging.debug(f'keys_counts: {keys_counts}')
    return keys_counts
# end def



def get_instance_types_by_job_type(session, api_url, time_start, time_end, job_type):
    '''
    Queries ES metrics for list of instance types for a given job type.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @return: list of (instance_type,count)
    '''

    # https://www.elastic.co/guide/en/elasticsearch/reference/current/search-aggregations-bucket-daterange-aggregation.html
    query = {
  "aggs": {
    "2": {
      "terms": {
        "field": "job.job_info.facts.ec2_instance_type.keyword",
        "order": {
          "_count": "desc"
        },
        "size": 100
      }
    }
  },
  "size": 0,
  "stored_fields": [
    "*"
  ],
  "script_fields": {},
  "docvalue_fields": [
    {
      "field": "@timestamp",
      "format": "date_time"
    },
    {
      "field": "job.job_info.cmd_end",
      "format": "date_time"
    },
    {
      "field": "job.job_info.cmd_start",
      "format": "date_time"
    },
    {
      "field": "job.job_info.metrics.inputs_localized.time_end",
      "format": "date_time"
    },
    {
      "field": "job.job_info.metrics.inputs_localized.time_start",
      "format": "date_time"
    },
    {
      "field": "job.job_info.metrics.product_provenance.availability_time",
      "format": "date_time"
    },
    {
      "field": "job.job_info.metrics.product_provenance.processing_start_time",
      "format": "date_time"
    },
    {
      "field": "job.job_info.metrics.products_staged.time_end",
      "format": "date_time"
    },
    {
      "field": "job.job_info.metrics.products_staged.time_start",
      "format": "date_time"
    },
    {
      "field": "job.job_info.time_end",
      "format": "date_time"
    },
    {
      "field": "job.job_info.time_queued",
      "format": "date_time"
    },
    {
      "field": "job.job_info.time_start",
      "format": "date_time"
    }
  ],
  "_source": {
    "excludes": []
  },
  "query": {
    "bool": {
      "must": [
        {
          "match_all": {}
        },
        {
          "query_string": {
            "query": "type.keyword:job_info",
            "analyze_wildcard": True,
            "time_zone": "America/Los_Angeles"
          }
        }
      ],
      "filter": [
        {
          "match_phrase": {
            "job_type.keyword": job_type
          }
        },
        {
          "range": {
            "@timestamp": {
              "gte": time_start,
              "lte": time_end,
              "format": "strict_date_optional_time"
            }
          }
        }
      ],
      "should": [],
      "must_not": []
    }
  }
}

    # get list of (instance_type,count)
    keys_counts = _get_es_aggregations_buckets_keys(session, api_url, query)

    logging.debug(f'keys_counts: {keys_counts}')
    return keys_counts
# end def




def get_job_runtime(session, api_url, time_start, time_end, job_type, instance_type):
    '''
    Gets the mean runtime of job step, which includes data stage-in, container, and stage-out.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @return: mean runtime duration in seconds.
    '''
    field = "job.job_info.duration"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field)
    return (hits_total_relation, hits_total_value, avg_value)
# end def


def get_container_runtime(session, api_url, time_start, time_end, job_type, instance_type):
    '''
    Gets the mean runtime of container step only.
    Note that this is the mean runtime of containers per job.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @return: mean runtime duration in nanoseconds.
    '''
    field = "job.job_info.metrics.usage_stats.wall_time"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field)
    return (hits_total_relation, hits_total_value, avg_value)
# end def


def get_stage_in_size(session, api_url, time_start, time_end, job_type, instance_type):
    '''
    Gets the mean stage-in data size of the given job-instance type constraint.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @return: mean stage-in data size in bytes.
    '''
    field = "job.job_info.metrics.inputs_localized.disk_usage"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field)
    return (hits_total_relation, hits_total_value, avg_value)
# end def


def get_stage_in_rate(session, api_url, time_start, time_end, job_type, instance_type):
    '''
    Gets the mean stage-in transfer rate of the given job-instance type constraint.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @return: mean stage-in transfer rate in bytes per second.
    '''
    field = "job.job_info.metrics.inputs_localized.transfer_rate"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field)
    return (hits_total_relation, hits_total_value, avg_value)
# end def


def get_stage_out_size(session, api_url, time_start, time_end, job_type, instance_type):
    '''
    Gets the mean stage-out data size of the given job-instance type constraint.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @return: mean stage-out data size in bytes.
    '''
    field = "job.job_info.metrics.products_staged.disk_usage"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field)
    return (hits_total_relation, hits_total_value, avg_value)
# end def


def get_stage_out_rate(session, api_url, time_start, time_end, job_type, instance_type):
    '''
    Gets the mean stage-out transfer rate of the given job-instance type constraint.
    @param session: the reusbale request session
    @param api_url: the api end point to elasticsearch.
    @param time_start: the start time constraint for ES query.
    @param time_end: the end time constraint for ES query.
    @param job_type: the job type constraint for ES query.
    @param instance_type: the job's instance type constraint for ES query.
    @return: mean stage-out transfer rate in bytes per second.
    '''
    field = "job.job_info.metrics.products_staged.transfer_rate"
    exit_code = 0
    (hits_total_relation, hits_total_value, avg_value) = _get_es_aggs_avg_field(session, api_url, time_start, time_end, job_type, exit_code, instance_type, field)
    return (hits_total_relation, hits_total_value, avg_value)
# end def


if __name__ == "__main__":

    # ----------------------------------------------------------------
    # parse input arguments

    from argparse import ArgumentParser

    parser = ArgumentParser()

    parser.add_argument('-d', '--debug', action='store_true')
    parser.add_argument('-v', '--verbose', action='store_true')

    parser.add_argument('-u', '--es_url', dest='es_url', help='es_url', metavar='URL')
    parser.add_argument('-b', '--days_back', dest='days_back', help='days_back', metavar='BACK')
    parser.add_argument('-s', '--time_start', dest='time_start', help='time_start', metavar='START')
    parser.add_argument('-e', '--time_end', dest='time_end', help='time_end', metavar='END')

    argsNamespace = parser.parse_args()

    args = vars(argsNamespace)

    # ----------------------------------------------------------------
    # set logging level

    import logging
    if args['debug']:
        logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')
    elif args['verbose']:
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    else: # defualt
        logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(message)s')
    # end if

    # ----------------------------------------------------------------
    # check input arguments

    es_url = args['es_url']
    logging.debug( f'es_url: {es_url}' )
    if not es_url:
        logging.error( f'missing argument es_url "{es_url}"' )
        raise Exception( f'missing argument es_url "{es_url}"' )
    # end if

    try:
        days_back = int( args['days_back'] )
        logging.debug( f'days_back: {days_back}' )
    except TypeError as e:
        days_back = None
        logging.warning( f'missing argument "days_back"' )
    # end try

    try:
        time_start = args['time_start']
        logging.debug( f'time_start: {time_start}' )
    except ValueError as e:
        time_start = None
        logging.warning( f'missing argument "time_start"' )
    # end try

    try:
        time_end = args['time_end']
        logging.debug( f'time_end: {time_end}' )
    except ValueError as e:
        time_end = None
        logging.warning( f'missing argument "time_end"' )
    # end try

    # sanity check of required input arguments
    if (not days_back) and not (time_start and time_end):
        logging.error( f'missing argument "days_back", or "time_start"/"time_end" ' )
        raise Exception( f'missing argument "days_back", or "time_start"/"time_end" ' )
    # end if

    # ----------------------------------------------------------------
    # determine start and end times.
    # if days_back is given, use based from now.
    # else look for start and end times given.

    from datetime import datetime
    from datetime import timedelta

    # ES expects datetime in format "2024-03-12T22:32:26.383Z"
    timestamp_format_z = '%Y-%m-%dT%H:%M:%S.%fZ'

    # format "20240312T223226Z"
    timestamp_format_t = '%Y%m%dT%H%M%SZ'

    if days_back:
        dt_end = datetime.utcnow()

        dt = timedelta(days=days_back)
        dt_start = dt_end - dt

    else:
      try:
          dt_start = datetime.strptime(time_start, timestamp_format_t)
      except ValueError as e:
          logging.error(f'# unable to convert {time_start} to datetime via {timestamp_format_z}')
          raise Exception(f'# unable to convert {time_start} to datetime via {timestamp_format_z}')        
      # end try-except

      try:
          dt_end = datetime.strptime(time_end, timestamp_format_t)
      except ValueError as e:
          logging.error(f'# unable to convert {time_end} to datetime via {timestamp_format_z}')
          raise Exception(f'# unable to convert {time_end} to datetime via {timestamp_format_z}')        
      # end try-except
    # end if

    logging.info(f'dt_start: {dt_start}')
    logging.info(f'dt_end:   {dt_end}')

    # get generic duration in unit days between start and end time
    dt_duration = dt_end - dt_start
    # days as float
    duration_days = dt_duration.days + (dt_duration.seconds / (60*60*24) )
    logging.info(f'duration_days: {duration_days}')

    # ----------------------------------------------------------------

    logging.info(f'HySDS Metrics ES url: {es_url}')

    from urllib.parse import urlsplit
    hostname = urlsplit(es_url).netloc

    # ----------------------------------------------------------------

    import sys
    import getpass

    if sys.stdin.isatty():
        username = input("Username: ")
        password = getpass.getpass("Password: ")
    else:
        username = sys.stdin.readline().rstrip()
        password = sys.stdin.readline().rstrip()
    # end if

    # ----------------------------------------------------------------
    # reuse session to ES

    import requests
    import json

    session = requests.Session()
    session.auth = (username, password)

    # ----------------------------------------------------------------
    # for each job type, for each instance type, get metrics

    duration_days_str = '{:.1f}'.format(duration_days)
    csv_filename = f'job_metrics {hostname} {dt_start.strftime(timestamp_format_t)}-{dt_end.strftime(timestamp_format_t)} spanning {duration_days_str} days.csv'

    # ES expects datetime in format "2024-03-12T22:32:26.383Z"
    time_start = dt_start.strftime(timestamp_format_z)
    time_end = dt_end.strftime(timestamp_format_z)

    import csv
    with open(csv_filename, mode='w') as csv_out:
        csv_writer = csv.writer(csv_out, delimiter=',', quotechar='"', quoting=csv.QUOTE_NONNUMERIC)
        csv_writer.writerow(['job_type', 'job runtime (minutes avg)', 'container runtime (minutes avg)', 'stage-in size (GB avg)', 'stage-out size (GB avg)', 'instance type', 'stage-in rate (MB/s avg)', 'stage-out rate (MB/s avg)', 'daily count avg', 'count over duration', 'duration days'])

        # for each job type
        job_type_counts = get_job_types(session, es_url, time_start, time_end)
        for (job_type,job_type_count) in sorted(job_type_counts):
            logging.info(f'job_type: {job_type} --------------------')

            # for each instance type
            instance_type_counts = get_instance_types_by_job_type(session, es_url, time_start, time_end, job_type)
            for (instance_type,instance_type_count) in sorted(instance_type_counts):
                logging.info(f'    instance_type: {instance_type}')

                (count_relation, count, job_runtime) = get_job_runtime(session, es_url, time_start, time_end, job_type, instance_type)
                try:
                    # convert to minutes
                    job_runtime_m = float(job_runtime / 60)
                    logging.info(f'    job_runtime: {job_runtime_m} minutes')
                except TypeError as e:
                    job_runtime_m = 'n/a'
                # end try-except
                try:
                    daily_count_mean = float( count / duration_days )
                    logging.info(f'    count: {count_relation} {count} , {daily_count_mean} avg per day')
                except TypeError as e:
                    daily_count_mean = 'n/a'
                # end try-except

                (count_relation, count, container_runtime) = get_container_runtime(session, es_url, time_start, time_end, job_type, instance_type)
                try:
                    # convert nanoseconds to minutes
                    container_runtime_m = float(container_runtime / 1000000000 / 60)
                    logging.info(f'    container_runtime: {container_runtime_m} minutes')
                except TypeError as e:
                    container_runtime_m = 'n/a'
                # end try-except
                try:
                    daily_count_mean = float( count / duration_days )
                    logging.info(f'    count: {count_relation} {count} , {daily_count_mean} avg per day')
                except TypeError as e:
                    daily_count_mean = 'n/a'
                # end try-except

                (count_relation, count, stage_in_size_bytes) = get_stage_in_size(session, es_url, time_start, time_end, job_type, instance_type)
                try:
                    # convert bytes to GB
                    stage_in_size_gb = float(stage_in_size_bytes) / 1073741824
                    logging.info(f'    stage_in_size: {stage_in_size_gb} GB')
                except TypeError as e:
                    stage_in_size_gb = 'n/a'
                # end try-except

                (count_relation, count, stage_in_rate_bps) = get_stage_in_rate(session, es_url, time_start, time_end, job_type, instance_type)
                try:
                    # convert bytes to MB
                    stage_in_rate_MBps = float(stage_in_rate_bps) / 1048576
                    logging.info(f'    stage_in_rate: {stage_in_rate_MBps} MB/s')
                except TypeError as e:
                    stage_in_rate_MBps = 'n/a'
                # end try-except

                (count_relation, count, stage_out_size_bytes) = get_stage_out_size(session, es_url, time_start, time_end, job_type, instance_type)
                try:
                    # convert bytes to GB
                    stage_out_size_gb = float(stage_out_size_bytes) / 1073741824
                    logging.info(f'    stage_out_size: {stage_out_size_gb} GB')
                except TypeError as e:
                    stage_out_size_gb = 'n/a'
                # end try-except

                (count_relation, count, stage_out_rate_bps) = get_stage_out_rate(session, es_url, time_start, time_end, job_type, instance_type)
                try:
                    # convert bytes to MB
                    stage_out_rate_MBps = float(stage_out_rate_bps) / 1048576
                    logging.info(f'    stage_out_rate: {stage_out_rate_MBps} MB/s')
                except TypeError as e:
                    stage_out_rate_MBps = 'n/a'
                # end try-except

                csv_writer.writerow([job_type, job_runtime_m, container_runtime_m, stage_in_size_gb, stage_out_size_gb, instance_type, stage_in_rate_MBps, stage_out_rate_MBps, daily_count_mean, count, duration_days])

            # end for
        # end for
    # end with open csv

    logging.info(f'...exported metrics to {csv_filename}')


# end if main




