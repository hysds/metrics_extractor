#!/usr/bin/env python
"""
L0B three-level breakdown: rcid -> beam_mode -> diagnostic_mode.

L0B job_ids don't contain beam info (L0B runs pre-focus), so the generic
three-level breakdown in hysds_metrics_es_extractor_enhanced.py cannot be
used. This script derives the breakdown from the L0B product filename
pattern and maps rcid → beam_mode via the NISAR_MIXED_MODES_CONFIG.

Usage:
    ES_USERNAME=hysdsops ES_PASSWORD=... l0b_three_level_breakdown.py \\
        --es_url "https://localhost:9202/logstash-*/_search" \\
        --job_type "job-SCIFLO_L0B:release-r05.01.3" \\
        --days_back 200 \\
        --nisar_config NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json

Output:
    job_three_level_breakdown_<job_type>_<hostname>_L0B_rcid_beam_mode_diagmode_spanning_<N>.0_days.csv
"""

import argparse
import csv
import getpass
import json
import logging
import os
import re
import sys
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit

import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# L0B product ID suffix: [rcid][SDC]_[timestamp]_
# Example: NISAR_L0_PR_RRSD_016_048_A_156S_20260325T122214_20260325T123750_X05013_F_J_001
#          rcid=156, suffix=S (science)
RCID_RE = re.compile(r"_(\d+)([SDC])_\d{8}T\d{6}_")
DIAG_MAP = {"S": "science", "D": "diagnostic", "C": "cal"}


def extract_wall_time(usage_stats):
    if not usage_stats:
        return None
    try:
        stats = json.loads(usage_stats) if isinstance(usage_stats, str) else usage_stats
        if isinstance(stats, list):
            walls = [float(s["wall_time"]) for s in stats if isinstance(s, dict) and "wall_time" in s]
            return min(walls) / 1e9 if walls else None
    except Exception:
        return None
    return None


def get_beam_mode(rcid, config):
    val = config.get(str(rcid))
    if isinstance(val, list) and val:
        lbeams = [v for v in val if v.startswith("L_")]
        return lbeams[0] if lbeams else val[0]
    return None


def get_credentials():
    user = os.environ.get("ES_USERNAME")
    pw = os.environ.get("ES_PASSWORD")
    if user and pw:
        return user, pw
    user = input("Username: ")
    pw = getpass.getpass("Password: ")
    return user, pw


def scroll_query(session, es_url, job_type, time_start, time_end):
    base = es_url.rsplit("/logstash-", 1)[0]
    query = {
        "size": 10000,
        "query": {
            "bool": {
                "filter": [
                    {"term": {"job_type.keyword": job_type}},
                    {
                        "range": {
                            "@timestamp": {
                                "gte": time_start,
                                "lte": time_end,
                                "format": "strict_date_optional_time",
                            }
                        }
                    },
                ]
            }
        },
        "_source": [
            "job.job_info.duration",
            "job.job_info.status",
            "job.job_info.metrics.products_staged",
            "job.job_info.metrics.usage_stats",
            "job.job_info.facts.ec2_instance_type",
        ],
    }
    r = session.post(f"{es_url}?scroll=5m", json=query, verify=False)
    r.raise_for_status()
    data = r.json()
    scroll_id = data.get("_scroll_id")
    hits = data["hits"]["hits"]
    while data["hits"]["hits"]:
        r = session.post(
            f"{base}/_search/scroll",
            json={"scroll": "5m", "scroll_id": scroll_id},
            verify=False,
        )
        r.raise_for_status()
        data = r.json()
        hits.extend(data["hits"]["hits"])
    try:
        session.delete(f"{base}/_search/scroll", json={"scroll_id": scroll_id}, verify=False)
    except Exception:
        pass
    return hits


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("-u", "--es_url", required=True, help="Elasticsearch URL (e.g., https://localhost:9202/logstash-*/_search)")
    ap.add_argument("--job_type", required=True, help="Job type keyword (e.g., job-SCIFLO_L0B:release-r05.01.3)")
    ap.add_argument("-b", "--days_back", type=int, default=200, help="Lookback window in days (default: 200)")
    ap.add_argument("--nisar_config", required=True, help="Path to NISAR_MIXED_MODES_CONFIG JSON")
    ap.add_argument("-v", "--verbose", action="store_true")
    args = ap.parse_args()

    logging.basicConfig(level=logging.INFO if args.verbose else logging.WARNING, format="%(asctime)s - %(levelname)s - %(message)s")

    if "SCIFLO_L0B" not in args.job_type:
        logging.error(f"This script is L0B-specific. Got job_type: {args.job_type}")
        sys.exit(1)

    with open(args.nisar_config) as f:
        config = json.load(f)

    user, pw = get_credentials()
    session = requests.Session()
    session.auth = (user, pw)

    dt_end = datetime.now(timezone.utc)
    dt_start = dt_end - timedelta(days=args.days_back)
    time_start = dt_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    time_end = dt_end.strftime("%Y-%m-%dT%H:%M:%S.%fZ")

    logging.info(f"Querying {args.job_type} on {args.es_url} ({args.days_back} days)")
    hits = scroll_query(session, args.es_url, args.job_type, time_start, time_end)
    logging.info(f"Total hits: {len(hits)}")

    groups = defaultdict(lambda: {
        "count": 0, "job_runtime": [], "container_runtime": [], "instance_type": set(),
    })

    for h in hits:
        src = h["_source"]
        job_info = src.get("job", {}).get("job_info", {})
        if job_info.get("status") != 0:
            continue
        metrics = job_info.get("metrics", {})
        ps = metrics.get("products_staged") or []
        if not (ps and isinstance(ps, list) and isinstance(ps[0], dict)):
            continue
        pid = ps[0].get("id", "")
        m = RCID_RE.search(pid)
        if not m:
            continue
        rcid = int(m.group(1))
        diagmode = DIAG_MAP.get(m.group(2), "unknown")
        beam_mode = get_beam_mode(rcid, config) or "unknown"

        key = (rcid, beam_mode, diagmode)
        g = groups[key]
        g["count"] += 1
        pcm_min = (job_info.get("duration") or 0) / 60.0
        pge_sec = extract_wall_time(metrics.get("usage_stats"))
        pge_min = pge_sec / 60.0 if pge_sec else pcm_min  # fallback
        g["job_runtime"].append(pcm_min)
        g["container_runtime"].append(pge_min)
        g["instance_type"].add(job_info.get("facts", {}).get("ec2_instance_type", "unknown"))

    hostname = urlsplit(args.es_url).netloc
    duration_days_str = f"{float(args.days_back):.1f}"
    safe_job = args.job_type.replace(":", "_").replace("-", "_")
    out_path = f"job_three_level_breakdown_{safe_job} {hostname} L0B_rcid_beam_mode_diagmode spanning {duration_days_str} days.csv"

    with open(out_path, "w", newline="") as f:
        w = csv.writer(f)
        w.writerow([
            "job_type", "instance_type", "rcid", "beam_mode", "diagnostic_mode",
            "job_runtime_m", "container_runtime_m", "count", "daily_count_avg", "duration_days",
        ])
        for (rcid, beam, diag), g in sorted(groups.items()):
            if g["count"] == 0:
                continue
            avg_job = sum(g["job_runtime"]) / g["count"]
            avg_pge = sum(g["container_runtime"]) / g["count"]
            instance = ",".join(sorted(g["instance_type"]))
            w.writerow([
                args.job_type, instance, rcid, beam, diag,
                round(avg_job, 4), round(avg_pge, 4),
                g["count"], round(g["count"] / args.days_back, 4), args.days_back,
            ])

    logging.info(f"Exported L0B three-level breakdown to {out_path}")
    print(f"Saved: {out_path}")


if __name__ == "__main__":
    main()
