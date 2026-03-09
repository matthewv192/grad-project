#!/bin/bash
# load_only.sh — run the q loader on staged manifests without hitting the Databento API.
#
# Usage:
#   ./scripts/load_only.sh
#
# Run this after download_only.sh to write staged CSVs into the HDB.
# The API key is NOT required for this script.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$PACKAGE_DIR/setenv.sh"

VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"
if [ -d "$VENV_DIR" ]; then
    # shellcheck source=/dev/null
    source "$VENV_DIR/bin/activate"
fi

echo "Loading staged manifests into HDB..."
python "$PACKAGE_DIR/code/backfill/orchestrator.py" \
    --load-only \
    "$@"
