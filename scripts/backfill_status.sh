#!/bin/bash
# backfill_status.sh — show progress of backfill jobs by reading the job store.
#
# Usage:
#   ./scripts/backfill_status.sh [--request-id req_20240115_001]
#
# Without --request-id, shows a summary of ALL requests.
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

python "$PACKAGE_DIR/code/backfill/orchestrator.py" \
    --status \
    "$@"
