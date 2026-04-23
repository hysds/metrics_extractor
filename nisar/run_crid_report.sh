#!/bin/bash
# run_crid_report.sh — Generate a full CRID metrics report for a NISAR cluster.
#
# This orchestrator chains:
#   1. hysds_metrics_es_extractor_enhanced.py for RSLC/GSLC/GCOV/INSAR/L3_SM
#      (three-level breakdown: beam_name → coverage → acquisition_mode)
#   2. l0b_three_level_breakdown.py for L0B
#      (three-level breakdown: rcid → beam_mode → diagnostic_mode)
#   3. combine_breakdowns.py — aggregate into a single CSV with unified schema
#   4. build_crid_workbook.py — build a combined Excel workbook
#
# Prerequisites:
#   - SSH tunnel to the cluster's OpenSearch (see README.md § "SSH Tunnel Setup")
#   - netrc-os credential file for the cluster
#
# Usage:
#   ./run_crid_report.sh \
#     --crid X05013 \
#     --cluster POP1 \
#     --es_url "https://localhost:9202/logstash-*/_search" \
#     --netrc_os ~/dev/nisar/tmp/netrc-os-ops-pop1 \
#     --version r05.01.3 \
#     --days_back 200 \
#     --output_dir ~/dev/nisar/tmp
#
# Individual PGE job types are derived from --version (e.g., release-r05.01.3).
# Override with --job_suffix_* if needed (e.g., L3_SM often has a -1 suffix).

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PYTHON="${PYTHON:-$HOME/dev/nisar/.venv/bin/python}"

# Defaults
CRID=""
CLUSTER=""
ES_URL=""
NETRC_OS=""
VERSION=""
DAYS_BACK=200
OUTPUT_DIR="$PWD"
NISAR_CONFIG="$SCRIPT_DIR/NISAR_MIXED_MODES_CONFIG_20200101T000000_01.json"

# PGE-specific job suffix overrides (default: release-<version>)
SUFFIX_L0B=""
SUFFIX_RSLC=""
SUFFIX_GSLC=""
SUFFIX_GCOV=""
SUFFIX_INSAR=""
SUFFIX_L3_SM=""

PGE_TYPES=("L0B" "RSLC" "GSLC" "GCOV" "INSAR" "L3_SM")

usage() {
    # Print the leading comment block (lines starting with #, after the shebang)
    awk '/^#!/{next} /^#/{sub(/^# ?/, ""); print; next} {exit}' "$0"
    exit 1
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --crid) CRID="$2"; shift 2 ;;
        --cluster) CLUSTER="$2"; shift 2 ;;
        --es_url) ES_URL="$2"; shift 2 ;;
        --netrc_os) NETRC_OS="$2"; shift 2 ;;
        --version) VERSION="$2"; shift 2 ;;
        --days_back) DAYS_BACK="$2"; shift 2 ;;
        --output_dir) OUTPUT_DIR="$2"; shift 2 ;;
        --nisar_config) NISAR_CONFIG="$2"; shift 2 ;;
        --job_suffix_l0b) SUFFIX_L0B="$2"; shift 2 ;;
        --job_suffix_rslc) SUFFIX_RSLC="$2"; shift 2 ;;
        --job_suffix_gslc) SUFFIX_GSLC="$2"; shift 2 ;;
        --job_suffix_gcov) SUFFIX_GCOV="$2"; shift 2 ;;
        --job_suffix_insar) SUFFIX_INSAR="$2"; shift 2 ;;
        --job_suffix_l3_sm) SUFFIX_L3_SM="$2"; shift 2 ;;
        --help|-h) usage ;;
        *) echo "Unknown argument: $1" >&2; usage ;;
    esac
done

for req in CRID CLUSTER ES_URL NETRC_OS VERSION; do
    if [[ -z "${!req}" ]]; then
        flag=$(echo "$req" | tr '[:upper:]' '[:lower:]')
        echo "ERROR: --$flag is required" >&2
        usage
    fi
done

if [[ ! -r "$NETRC_OS" ]]; then
    echo "ERROR: netrc-os file not readable: $NETRC_OS" >&2
    exit 1
fi
if [[ ! -r "$NISAR_CONFIG" ]]; then
    echo "ERROR: NISAR config not readable: $NISAR_CONFIG" >&2
    exit 1
fi

mkdir -p "$OUTPUT_DIR"

# Extract OpenSearch credentials from netrc-os file (last field is the password)
ES_USERNAME=$(awk '/login/{print $(NF-2); exit}' "$NETRC_OS")
ES_PASSWORD=$(awk '{print $NF}' "$NETRC_OS")
export ES_USERNAME ES_PASSWORD

if [[ -z "$ES_USERNAME" || -z "$ES_PASSWORD" ]]; then
    echo "ERROR: failed to parse credentials from $NETRC_OS" >&2
    exit 1
fi

# Default job suffix — typically release-<version>
DEFAULT_SUFFIX="release-$VERSION"

get_suffix() {
    local pge="$1"
    local var="SUFFIX_${pge}"
    local val="${!var}"
    if [[ -n "$val" ]]; then
        echo "$val"
    else
        echo "$DEFAULT_SUFFIX"
    fi
}

echo "============================================================"
echo "NISAR CRID Metrics Report"
echo "  CRID:       $CRID"
echo "  Cluster:    $CLUSTER"
echo "  Version:    $VERSION"
echo "  ES URL:     $ES_URL"
echo "  Days back:  $DAYS_BACK"
echo "  Output dir: $OUTPUT_DIR"
echo "============================================================"

cd "$OUTPUT_DIR"

for pge in "${PGE_TYPES[@]}"; do
    suffix=$(get_suffix "$pge")
    job_type="job-SCIFLO_${pge}:${suffix}"
    echo ""
    echo "--- $pge ($job_type) ---"

    if [[ "$pge" == "L0B" ]]; then
        # L0B uses the L0B-specific breakdown script
        "$PYTHON" "$SCRIPT_DIR/l0b_three_level_breakdown.py" -v \
            -u "$ES_URL" \
            --job_type "$job_type" \
            -b "$DAYS_BACK" \
            --nisar_config "$NISAR_CONFIG"
    else
        # Standard enhanced extractor (requires interactive stdin → pipe creds)
        printf '%s\n%s\n' "$ES_USERNAME" "$ES_PASSWORD" | "$PYTHON" "$SCRIPT_DIR/hysds_metrics_es_extractor_enhanced.py" -v \
            -u "$ES_URL" \
            -b "$DAYS_BACK" \
            --breakdown_job "$job_type" \
            --nisar_config "$NISAR_CONFIG"
    fi
done

echo ""
echo "--- Aggregating into unified CSV ---"
"$PYTHON" "$SCRIPT_DIR/combine_breakdowns.py" \
    --input_dir "$OUTPUT_DIR" \
    --crid "$CRID" \
    --cluster "$CLUSTER" \
    --version "$VERSION"

echo ""
echo "--- Building combined Excel workbook ---"
"$PYTHON" "$SCRIPT_DIR/build_crid_workbook.py" \
    --input_dir "$OUTPUT_DIR" \
    --crid "$CRID" \
    --cluster "$CLUSTER" \
    --version "$VERSION" \
    --days_back "$DAYS_BACK"

echo ""
echo "============================================================"
echo "Done. Deliverables in $OUTPUT_DIR:"
ls -1 "$OUTPUT_DIR"/job_three_level_breakdown_*${VERSION//./_}*.csv 2>/dev/null || true
ls -1 "$OUTPUT_DIR"/all_pge_three_level_breakdown_${CRID}_${CLUSTER}_*.csv 2>/dev/null || true
ls -1 "$OUTPUT_DIR"/ALL_PGE_three_level_breakdown_${CRID}_${CLUSTER}_*.xlsx 2>/dev/null || true
echo "============================================================"
