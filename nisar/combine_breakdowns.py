#!/usr/bin/env python
"""
Combine per-PGE three-level breakdown CSVs into a single aggregated CSV
with a unified schema.

Auto-discovers CSV files in --input_dir matching:
    job_three_level_breakdown_job_SCIFLO_<PGE>_<version>*.csv

The two breakdown conventions are reconciled:
- Standard (RSLC/GSLC/GCOV/INSAR/L3_SM): beam_name → coverage → acquisition_mode
- L0B: rcid → beam_mode → diagnostic_mode

Both are projected onto generic level_1/level_2/level_3 columns while also
preserving the native columns for each convention.

Usage:
    combine_breakdowns.py --input_dir ~/dev/nisar/tmp \\
        --crid X05013 --cluster POP1 --version r05.01.3 \\
        [--output OUT.csv]
"""

import argparse
import csv
import re
import sys
from pathlib import Path

COLUMNS = [
    "pge_type", "job_type", "instance_type",
    "level_1", "level_2", "level_3",
    "beam_name", "coverage", "acquisition_mode",
    "rcid", "diagnostic_mode",
    "job_runtime_m", "container_runtime_m",
    "stage_in_size_gb", "stage_out_size_gb",
    "stage_in_rate_mbps", "stage_out_rate_mbps",
    "count", "daily_count_avg", "duration_days",
]

# Regex to extract PGE type from filename
FNAME_RE = re.compile(r"job_three_level_breakdown_job_SCIFLO_([A-Z0-9_]+?)_(release|pcm).*\.csv$")


def discover_csvs(input_dir: Path, version_filter: str | None):
    """Return dict of pge_type → Path. If multiple match, takes the most recent."""
    matches = {}
    for p in input_dir.glob("job_three_level_breakdown_*.csv"):
        m = FNAME_RE.search(p.name)
        if not m:
            continue
        pge = m.group(1).rstrip("_")
        if version_filter and version_filter.replace(".", "") not in p.name.replace(".", ""):
            continue
        # Keep newest by mtime if multiple match
        if pge not in matches or p.stat().st_mtime > matches[pge].stat().st_mtime:
            matches[pge] = p
    return matches


def project_row(row: dict, pge_type: str) -> dict:
    """Project a per-PGE native row onto the unified schema."""
    if pge_type == "L0B":
        # L0B native: job_type, instance_type, rcid, beam_mode, diagnostic_mode, ...
        return {
            "pge_type": pge_type,
            "job_type": row.get("job_type", ""),
            "instance_type": row.get("instance_type", ""),
            "level_1": str(row.get("rcid", "")),
            "level_2": row.get("beam_mode", ""),
            "level_3": row.get("diagnostic_mode", ""),
            "beam_name": row.get("beam_mode", ""),
            "coverage": "",
            "acquisition_mode": "",
            "rcid": row.get("rcid", ""),
            "diagnostic_mode": row.get("diagnostic_mode", ""),
            "job_runtime_m": row.get("job_runtime_m", ""),
            "container_runtime_m": row.get("container_runtime_m", ""),
            "stage_in_size_gb": "",
            "stage_out_size_gb": "",
            "stage_in_rate_mbps": "",
            "stage_out_rate_mbps": "",
            "count": row.get("count", ""),
            "daily_count_avg": row.get("daily_count_avg", ""),
            "duration_days": row.get("duration_days", ""),
        }
    # Standard: beam_name, coverage, acquisition_mode
    return {
        "pge_type": pge_type,
        "job_type": row.get("job_type", ""),
        "instance_type": row.get("instance_type", ""),
        "level_1": row.get("beam_name", ""),
        "level_2": row.get("coverage", ""),
        "level_3": row.get("acquisition_mode", ""),
        "beam_name": row.get("beam_name", ""),
        "coverage": row.get("coverage", ""),
        "acquisition_mode": row.get("acquisition_mode", ""),
        "rcid": "",
        "diagnostic_mode": "",
        "job_runtime_m": row.get("job_runtime_m", ""),
        "container_runtime_m": row.get("container_runtime_m", ""),
        "stage_in_size_gb": row.get("stage_in_size_gb", ""),
        "stage_out_size_gb": row.get("stage_out_size_gb", ""),
        "stage_in_rate_mbps": row.get("stage_in_rate_mbps", ""),
        "stage_out_rate_mbps": row.get("stage_out_rate_mbps", ""),
        "count": row.get("count", ""),
        "daily_count_avg": row.get("daily_count_avg", ""),
        "duration_days": row.get("duration_days", ""),
    }


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--input_dir", required=True, help="Directory containing per-PGE breakdown CSVs")
    ap.add_argument("--crid", required=True, help="CRID (e.g., X05013) — used in output filename")
    ap.add_argument("--cluster", required=True, help="Cluster name (e.g., POP1) — used in output filename")
    ap.add_argument("--version", help="Version filter substring (e.g., r05.01.3) to disambiguate when multiple CSVs exist")
    ap.add_argument("--output", help="Output CSV path (default: <input_dir>/all_pge_three_level_breakdown_<crid>_<cluster>_<date>.csv)")
    ap.add_argument("--date", help="Date suffix for default output name (default: today)")
    args = ap.parse_args()

    input_dir = Path(args.input_dir).expanduser()
    if not input_dir.is_dir():
        print(f"ERROR: {input_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    csvs = discover_csvs(input_dir, args.version)
    if not csvs:
        print(f"ERROR: no matching breakdown CSVs found in {input_dir}", file=sys.stderr)
        sys.exit(1)

    if args.output:
        out_path = Path(args.output)
    else:
        from datetime import datetime
        date = args.date or datetime.now().strftime("%Y-%m-%d")
        out_path = input_dir / f"all_pge_three_level_breakdown_{args.crid}_{args.cluster}_{date}.csv"

    all_rows = []
    for pge, path in sorted(csvs.items()):
        with open(path) as f:
            rows = list(csv.DictReader(f))
        print(f"  {pge:<8} {len(rows):>4} rows  ({path.name})")
        for r in rows:
            all_rows.append(project_row(r, pge))

    with open(out_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=COLUMNS)
        w.writeheader()
        w.writerows(all_rows)

    print(f"\nAggregated {len(all_rows)} rows from {len(csvs)} PGE types → {out_path}")


if __name__ == "__main__":
    main()
