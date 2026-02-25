#!/usr/bin/env python
"""
PGE Execution Time Extractor

Extracts job execution times for NISAR PGE jobs filtered by data day.
Data day and match_id are extracted from the first product staged.

Supported PGE types: L0B, RSLC, GSLC, GCOV, INSAR, L3_SM

Usage:
    pge_execution_time_extractor.py --pge_type=RSLC --data_day=2025-11-10 --es_url="https://..."
    pge_execution_time_extractor.py --pge_type=RSLC --list-data-days --es_url="https://..."
"""

import csv
import getpass
import glob as glob_mod
import json
import logging
import os
import re
import sys
from argparse import ArgumentParser
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests

from es_pagination import search_after_scan

try:
    import keyring
    KEYRING_AVAILABLE = True
except ImportError:
    KEYRING_AVAILABLE = False


# PGE configurations
# - product_id_pattern: regex to extract timestamps from product ID
# - timestamp_index: which timestamp to use for data day (0-indexed)
# - timestamp_count: total timestamps to include in match_id
PGE_CONFIGS = {
    'L0B': {
        'job_type_pattern': 'job-SCIFLO_L0B*',
        'product_id_pattern': r'_(\d{8}T\d{6})_(\d{8}T\d{6})_',
        'timestamp_index': 0,
        'timestamp_count': 2,
        'rcid_pattern': r'_(\d+)S_\d{8}T\d{6}_',
    },
    'RSLC': {
        'job_type_pattern': 'job-SCIFLO_RSLC*',
        'product_id_pattern': r'_(\d{8}T\d{6})_(\d{8}T\d{6})_',
        'timestamp_index': 0,
        'timestamp_count': 2,
    },
    'GSLC': {
        'job_type_pattern': 'job-SCIFLO_GSLC*',
        'product_id_pattern': r'_(\d{8}T\d{6})_(\d{8}T\d{6})_',
        'timestamp_index': 0,
        'timestamp_count': 2,
    },
    'GCOV': {
        'job_type_pattern': 'job-SCIFLO_GCOV*',
        'product_id_pattern': r'_(\d{8}T\d{6})_(\d{8}T\d{6})_',
        'timestamp_index': 0,
        'timestamp_count': 2,
    },
    'INSAR': {
        'job_type_pattern': 'job-SCIFLO_INSAR*',
        'product_id_pattern': r'_(\d{8}T\d{6})_(\d{8}T\d{6})_(\d{8}T\d{6})_(\d{8}T\d{6})_',
        'timestamp_index': 2,  # secondary start timestamp
        'timestamp_count': 4,
    },
    'L3_SM': {
        'job_type_pattern': 'job-SCIFLO_L3_SM*',
        'product_id_pattern': r'_(\d{8}T\d{6})_(\d{8}T\d{6})_',
        'timestamp_index': 0,
        'timestamp_count': 2,
    },
}


def get_credentials(hostname):
    """Get credentials from environment, keychain, or prompt."""
    # Try environment variables first
    username = os.environ.get("ES_USERNAME")
    password = os.environ.get("ES_PASSWORD")
    if username and password:
        logging.info("Using credentials from environment variables")
        return username, password

    # Try keychain
    service_name = f"metrics_extractor_{hostname}"
    if KEYRING_AVAILABLE:
        try:
            username = keyring.get_password(service_name, "username")
            if username:
                password = keyring.get_password(service_name, username)
                if password:
                    logging.info(f"Using cached credentials for {hostname}")
                    return username, password
        except Exception as e:
            logging.debug(f"Keyring error: {e}")

    # Prompt user
    username = input("Username: ")
    password = getpass.getpass("Password: ")

    # Cache credentials
    if KEYRING_AVAILABLE:
        try:
            keyring.set_password(service_name, "username", username)
            keyring.set_password(service_name, username, password)
            logging.info(f"Credentials cached for {hostname}")
        except Exception as e:
            logging.debug(f"Could not cache credentials: {e}")

    return username, password


def get_product_id(hit):
    """Extract the first product ID from a job hit."""
    products = (
        hit.get("_source", {})
        .get("job", {})
        .get("job_info", {})
        .get("metrics", {})
        .get("products_staged", [])
    )
    if products:
        return products[0].get("id", "")
    return ""


def extract_timestamps(product_id, pattern, count):
    """Extract timestamps from product ID."""
    match = re.search(pattern, product_id)
    if match:
        return [match.group(i + 1) for i in range(count)]
    return None


def get_data_day(timestamps, index):
    """Convert timestamp at index to YYYY-MM-DD format."""
    if timestamps and index < len(timestamps):
        try:
            dt = datetime.strptime(timestamps[index], "%Y%m%dT%H%M%S")
            return dt.strftime("%Y-%m-%d")
        except ValueError:
            pass
    return None


def get_match_id(timestamps):
    """Create match_id from all timestamps."""
    if timestamps:
        return "_".join(timestamps)
    return None


def extract_rcid(product_id, pattern):
    """Extract RCID (radar config ID) from product ID."""
    match = re.search(pattern, product_id)
    if match:
        return int(match.group(1))
    return None


def load_modes_config(config_path):
    """Load NISAR mixed modes config JSON. Returns dict mapping RCID str → mode list."""
    with open(config_path) as f:
        return json.load(f)


def get_diagnostic_mode_flag(rcid, modes_config):
    """Derive diagnostic mode flag from RCID using modes config.

    Returns 1 (DM1), 2 (DM2), 'cal', or 0 (science).
    """
    if rcid is None or modes_config is None:
        return None
    modes = modes_config.get(str(rcid))
    if not modes:
        return None
    first = modes[0]
    if first == "DM1":
        return 1
    elif first == "DM2":
        return 2
    elif first == "cal":
        return "cal"
    return 0


def extract_version(job_type):
    """Extract version string from job_type field.

    e.g. 'job-SCIFLO_L0B:pcm_r05.00.1_pge_r05.00.5.1' → 'pcm_r05.00.1_pge_r05.00.5.1'
    """
    if job_type and ":" in job_type:
        return job_type.split(":", 1)[1]
    return "unknown"


def query_jobs(session, es_url, job_type, time_start, time_end):
    """Query Elasticsearch for jobs."""
    if "*" in job_type:
        job_filter = {"wildcard": {"job_type.keyword": job_type}}
    else:
        job_filter = {"match_phrase": {"job_type.keyword": job_type}}

    query = {
        "_source": [
            "job.job_id",
            "job.job_info.metrics.usage_stats.wall_time",
            "job.job_info.metrics.products_staged",
            "job.job_info.facts.ec2_instance_type",
            "job_type",
        ],
        "query": {
            "bool": {
                "must": [
                    {"query_string": {"query": "type.keyword:job_info"}},
                ],
                "filter": [
                    job_filter,
                    {"match_phrase": {"job.job_info.status": 0}},
                    {"range": {"@timestamp": {"gte": time_start, "lte": time_end}}},
                ],
            }
        },
    }

    return search_after_scan(session, es_url, query)


def process_jobs(hits, pge_config, target_data_day=None, pge_type=None, modes_config=None):
    """Process job hits and extract execution times."""
    pattern = pge_config['product_id_pattern']
    ts_index = pge_config['timestamp_index']
    ts_count = pge_config['timestamp_count']
    rcid_pattern = pge_config.get('rcid_pattern')
    is_l0b = pge_type == 'L0B'

    results = []
    data_day_counts = {}

    for hit in hits:
        source = hit.get("_source", {}).get("job", {})
        job_id = source.get("job_id", "")
        product_id = get_product_id(hit)
        hit_job_type = hit.get("_source", {}).get("job_type", "")

        timestamps = extract_timestamps(product_id, pattern, ts_count)
        if not timestamps:
            continue

        data_day = get_data_day(timestamps, ts_index)
        if not data_day:
            continue

        # Count data days
        data_day_counts[data_day] = data_day_counts.get(data_day, 0) + 1

        # Filter by target data day if specified
        if target_data_day and data_day != target_data_day:
            continue

        # Extract execution times
        usage_stats = source.get("job_info", {}).get("metrics", {}).get("usage_stats", [])
        wall_times = [s.get("wall_time") for s in usage_stats if isinstance(s, dict) and "wall_time" in s]

        if len(wall_times) < 2:
            continue

        pge_time = min(wall_times[0], wall_times[1]) / (1e9 * 60)  # nanoseconds to minutes
        pcm_time = max(wall_times[0], wall_times[1]) / (1e9 * 60)

        # L0B-specific fields
        rcid = None
        diagnostic_mode_flag = None
        beam_mode = None
        version = None
        if is_l0b:
            rcid = extract_rcid(product_id, rcid_pattern)
            diagnostic_mode_flag = get_diagnostic_mode_flag(rcid, modes_config)
            if rcid is not None and modes_config is not None:
                modes = modes_config.get(str(rcid))
                if modes:
                    beam_mode = modes[0]
            version = extract_version(hit_job_type)

        results.append({
            "job_id": job_id,
            "match_id": get_match_id(timestamps),
            "data_day": data_day,
            "instance_type": source.get("job_info", {}).get("facts", {}).get("ec2_instance_type", "unknown"),
            "pge_execution_time": pge_time,
            "pcm_container_time": pcm_time,
            "rcid": rcid,
            "diagnostic_mode_flag": diagnostic_mode_flag,
            "beam_mode": beam_mode,
            "version": version,
        })

    return results, data_day_counts


def calculate_stats(results, pge_type=None):
    """Calculate statistics grouped by instance type (and version/RCID for L0B)."""
    is_l0b = pge_type == 'L0B'
    by_group = {}
    for r in results:
        if is_l0b:
            key = (r.get("version"), r["instance_type"], r.get("rcid"), r.get("diagnostic_mode_flag"))
        else:
            key = (None, r["instance_type"], None, None)
        if key not in by_group:
            by_group[key] = []
        by_group[key].append(r)

    stats = {}
    for key, jobs in by_group.items():
        pge_times = [j["pge_execution_time"] for j in jobs]
        pcm_times = [j["pcm_container_time"] for j in jobs]
        version, inst, rcid, diag_flag = key
        entry = {
            "count": len(jobs),
            "avg_pge_time": sum(pge_times) / len(pge_times),
            "min_pge_time": min(pge_times),
            "max_pge_time": max(pge_times),
            "avg_pcm_time": sum(pcm_times) / len(pcm_times),
            "min_pcm_time": min(pcm_times),
            "max_pcm_time": max(pcm_times),
            "instance_type": inst,
            "version": version,
            "rcid": rcid,
            "diagnostic_mode_flag": diag_flag,
            "beam_mode": jobs[0].get("beam_mode"),
        }
        stats[key] = entry
    return stats


def _fmt(val):
    """Format a value for CSV: None → empty string."""
    return "" if val is None else val


def export_csv(results, stats, pge_type, job_type, data_day, filepath):
    """Export results to CSV."""
    with open(filepath, "w", newline="") as f:
        writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC)

        # Summary section
        writer.writerow(["# Summary Statistics"])
        writer.writerow([
            "pge_type", "job_type", "data_day", "instance_type",
            "version", "rcid", "diagnostic_mode_flag", "beam_mode",
            "count",
            "avg_pge_time_min", "min_pge_time_min", "max_pge_time_min",
            "avg_pcm_time_min", "min_pcm_time_min", "max_pcm_time_min"
        ])
        for key, s in stats.items():
            writer.writerow([
                pge_type, job_type, data_day, s["instance_type"],
                _fmt(s.get("version")), _fmt(s.get("rcid")),
                _fmt(s.get("diagnostic_mode_flag")), _fmt(s.get("beam_mode")),
                s["count"],
                s["avg_pge_time"], s["min_pge_time"], s["max_pge_time"],
                s["avg_pcm_time"], s["min_pcm_time"], s["max_pcm_time"]
            ])

        # Details section
        writer.writerow([])
        writer.writerow(["# Individual Job Details"])
        writer.writerow([
            "job_id", "match_id", "data_day", "instance_type",
            "version", "rcid", "diagnostic_mode_flag", "beam_mode",
            "pge_execution_time_min", "pcm_container_time_min"
        ])
        for r in results:
            writer.writerow([
                r["job_id"], r["match_id"], r["data_day"],
                r["instance_type"],
                _fmt(r.get("version")), _fmt(r.get("rcid")),
                _fmt(r.get("diagnostic_mode_flag")), _fmt(r.get("beam_mode")),
                r["pge_execution_time"], r["pcm_container_time"]
            ])

    logging.info(f"Exported to {filepath}")


def main():
    parser = ArgumentParser(description="Extract PGE execution times by data day")
    parser.add_argument("-v", "--verbose", action="store_true")
    parser.add_argument("-d", "--debug", action="store_true")
    parser.add_argument("-u", "--es_url", required=True, help="Elasticsearch URL")
    parser.add_argument("--pge_type", required=True, choices=list(PGE_CONFIGS.keys()))
    parser.add_argument("--data_day", help="Filter by data day (YYYY-MM-DD)")
    parser.add_argument("--job_type", help="Override job type pattern")
    parser.add_argument("--days_back", type=int, default=30, help="Days to search back")
    parser.add_argument("--list-data-days", action="store_true", help="List available data days")
    parser.add_argument("--modes_config", help="Path to NISAR_MIXED_MODES_CONFIG JSON (auto-detected if not specified)")
    args = parser.parse_args()

    # Setup logging
    level = logging.DEBUG if args.debug else logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")

    if not args.list_data_days and not args.data_day:
        print("Error: Either --data_day or --list-data-days is required")
        sys.exit(1)

    pge_config = PGE_CONFIGS[args.pge_type]
    job_type = args.job_type or pge_config['job_type_pattern']

    # Load modes config for L0B
    modes_config = None
    if args.pge_type == 'L0B':
        if args.modes_config:
            modes_config = load_modes_config(args.modes_config)
            logging.info(f"Loaded modes config from {args.modes_config}")
        else:
            # Auto-detect in same directory as this script
            script_dir = os.path.dirname(os.path.abspath(__file__))
            candidates = sorted(glob_mod.glob(os.path.join(script_dir, "NISAR_MIXED_MODES_CONFIG_*.json")))
            if candidates:
                modes_config = load_modes_config(candidates[-1])
                logging.info(f"Auto-detected modes config: {candidates[-1]}")
            else:
                logging.warning("No NISAR_MIXED_MODES_CONFIG file found; RCID/mode fields will be empty")

    # Time range
    dt_end = datetime.now(timezone.utc)
    dt_start = dt_end - timedelta(days=args.days_back)
    time_start = dt_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    time_end = dt_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    logging.info(f"PGE type: {args.pge_type}, Job type: {job_type}")
    logging.info(f"Time range: {time_start} to {time_end}")

    # Get credentials and query
    hostname = urlsplit(args.es_url).netloc
    username, password = get_credentials(hostname)

    session = requests.Session()
    session.auth = (username, password)

    hits = query_jobs(session, args.es_url, job_type, time_start, time_end)
    logging.info(f"Found {len(hits)} jobs")

    if not hits:
        print(f"No {args.pge_type} jobs found in the last {args.days_back} days")
        sys.exit(0)

    # Process jobs
    results, data_day_counts = process_jobs(
        hits, pge_config,
        target_data_day=args.data_day if not args.list_data_days else None,
        pge_type=args.pge_type,
        modes_config=modes_config,
    )

    # List data days mode
    if args.list_data_days:
        print(f"\n{args.pge_type} Jobs by Data Day (last {args.days_back} days)")
        print(f"{'Data Day':<15} {'Count':>8}")
        print("-" * 25)
        for day in sorted(data_day_counts.keys()):
            print(f"{day:<15} {data_day_counts[day]:>8}")
        print("-" * 25)
        print(f"{'Total':<15} {sum(data_day_counts.values()):>8}")
        sys.exit(0)

    if not results:
        print(f"No jobs found for data day {args.data_day}")
        sys.exit(0)

    # Calculate stats and export
    stats = calculate_stats(results, pge_type=args.pge_type)

    print(f"\n{args.pge_type} Execution Time Summary for {args.data_day}")
    print(f"Total jobs: {len(results)}")
    for key, s in stats.items():
        label = s["instance_type"]
        if args.pge_type == 'L0B':
            parts = [s.get("version", ""), f"RCID={s.get('rcid', '')}", f"DM={s.get('diagnostic_mode_flag', '')}"]
            label = f"{label} | {' | '.join(parts)}"
        print(f"\n  {label}: {s['count']} jobs")
        print(f"    PGE Time: avg={s['avg_pge_time']:.2f}, min={s['min_pge_time']:.2f}, max={s['max_pge_time']:.2f} min")

    csv_path = f"{args.pge_type}_execution_times_{args.data_day}_{hostname}.csv"
    export_csv(results, stats, args.pge_type, job_type, args.data_day, csv_path)
    print(f"\nResults saved to: {csv_path}")


if __name__ == "__main__":
    main()
