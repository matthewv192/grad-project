#!/bin/bash
# retry_failed.sh — re-queue all chunks with status=failed and retries < max.
#
# Usage:
#   ./scripts/retry_failed.sh [--request-id req_20240115_001]
#
# Without --request-id, retries ALL failed chunks across all requests.
# Uses exponential backoff: chunk waits 2^retries seconds before resubmit.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$PACKAGE_DIR/setenv.sh"

VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"
if [ -d "$VENV_DIR" ]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
fi

if [ -z "${DATABENTO_API_KEY:-}" ]; then
    echo "ERROR: DATABENTO_API_KEY is not set."
    exit 1
fi

echo "Retrying failed chunks..."

# Pass --retry-failed flag to orchestrator which will read the job store,
# find all failed chunks with retries < MAX_RETRIES, and resubmit them.
python "$PACKAGE_DIR/code/backfill/orchestrator.py" \
    --retry-failed \
    "$@"
