#!/bin/bash
# download_only.sh — download and stage Databento data without invoking the q loader.
#
# Usage:
#   ./scripts/download_only.sh --symbols "AAPL,MSFT" \
#                               --start 2024-06-03 --end 2024-06-07 \
#                               --schema trades
#
# After this script completes, run load_only.sh (or request_backfill.sh --load-only)
# to write the staged CSVs into the HDB.

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

echo "Downloading data (no q load)..."
python "$PACKAGE_DIR/code/backfill/orchestrator.py" \
    --download-only \
    "$@"
