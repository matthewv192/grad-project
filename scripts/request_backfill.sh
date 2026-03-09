#!/bin/bash
# request_backfill.sh — CLI entry point: create a backfill request and kick
#                       off the orchestrator.
#
# Usage:
#   ./scripts/request_backfill.sh --symbols "AAPL,MSFT" \
#                                  --start 2024-01-01 \
#                                  --end 2024-01-31 \
#                                  --schema trades
#
# All arguments are forwarded directly to orchestrator.py.
# Add --dry-run to estimate cost without submitting any jobs.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Load environment variables (TORQHOME, PACKAGEHOME, DATABENTO_API_KEY, etc.)
# shellcheck source=../setenv.sh
source "$PACKAGE_DIR/setenv.sh"

# Activate the Python virtual environment if one exists alongside the package
VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"
if [ -d "$VENV_DIR" ]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
fi

# Check that the API key is actually set (not just exported as empty string)
if [ -z "${DATABENTO_API_KEY:-}" ]; then
    echo "ERROR: DATABENTO_API_KEY is not set. Export it before running this script."
    exit 1
fi

echo "Submitting backfill request..."
python "$PACKAGE_DIR/code/backfill/orchestrator.py" "$@"
