#!/bin/bash
# check_quality.sh — run data quality checks on a loaded HDB partition.
#
# Usage:
#   ./scripts/check_quality.sh --date 2024-06-03 --table trades
#   ./scripts/check_quality.sh --date 2024-06-03 --table ohlcv_1m
#
# Arguments:
#   --date  YYYY-MM-DD  Partition date to check (required)
#   --table trades|ohlcv_1m  Table to check (default: trades)
#
# Exits 0 if all checks pass, 1 if any check fails.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$PACKAGE_DIR/setenv.sh"

TABLE="trades"
DATE=""

while [[ $# -gt 0 ]]; do
    case "$1" in
        --date)
            DATE="$2"; shift 2 ;;
        --table)
            TABLE="$2"; shift 2 ;;
        *)
            echo "Unknown argument: $1"
            echo "Usage: $0 --date YYYY-MM-DD [--table trades|ohlcv_1m]"
            exit 1 ;;
    esac
done

if [ -z "$DATE" ]; then
    echo "ERROR: --date is required" >&2
    echo "Usage: $0 --date YYYY-MM-DD [--table trades|ohlcv_1m]" >&2
    exit 1
fi

echo "Running quality checks: table=$TABLE date=$DATE"

export QUALITY_DATE="$DATE"
export QUALITY_TABLE="$TABLE"

# Load quality.q and run checkQualityScript[].
# cd to PACKAGEHOME first so the relative \l path resolves regardless of
# where the caller invoked this script from.
Q_SCRIPT="\\l code/backfill/quality.q
checkQualityScript[]
exit 0"

cd "$PACKAGE_DIR"
echo "$Q_SCRIPT" | q -q
