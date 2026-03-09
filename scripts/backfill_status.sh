#!/bin/bash
# backfill_status.sh — show progress of backfill jobs by reading the job store.
#
# Usage:
#   ./scripts/backfill_status.sh [--request-id req_20240115_001] [--metrics]
#
# Without --request-id, shows a summary of ALL requests.
# Add --metrics to display per-chunk timing instead of status counts.
# Output columns: request_id | schema | chunks | submitted | running |
#                 downloaded | loaded | verified | failed

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$PACKAGE_DIR/setenv.sh"

VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"
if [ -d "$VENV_DIR" ]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
fi

# Check for --metrics flag: if present, use --metrics mode; otherwise --status
if [[ "$*" == *"--metrics"* ]]; then
    python "$PACKAGE_DIR/code/backfill/orchestrator.py" "$@"
else
    python "$PACKAGE_DIR/code/backfill/orchestrator.py" \
        --status \
        "$@"
fi
