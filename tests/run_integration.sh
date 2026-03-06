#!/bin/bash
# run_integration.sh — run an end-to-end backfill and verify HDB output.
#
# Usage:
#   DATABENTO_API_KEY=your_key ./tests/run_integration.sh
#
# Defaults: AAPL, 2024-01-15 (one trading day, small volume).
# Override:
#   INTEGRATION_TEST_SYM=MSFT INTEGRATION_TEST_DATE=2024-01-16 ./run_integration.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$PACKAGE_DIR/setenv.sh"

VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"
if [ -d "$VENV_DIR" ]; then
    source "$VENV_DIR/bin/activate"
fi

export INTEGRATION_TEST_SYM="${INTEGRATION_TEST_SYM:-AAPL}"
export INTEGRATION_TEST_DATE="${INTEGRATION_TEST_DATE:-2024-01-15}"

echo "=== Integration test ==="
echo "Symbol : $INTEGRATION_TEST_SYM"
echo "Date   : $INTEGRATION_TEST_DATE"
echo ""

# Step 1: run backfill (submit → poll → download → convert → write manifests)
# --skip-load so we run the q loader manually in step 2 with explicit env vars
echo "[1/3] Running orchestrator..."
python "$PACKAGE_DIR/code/backfill/orchestrator.py" \
    --symbols "$INTEGRATION_TEST_SYM" \
    --start "$INTEGRATION_TEST_DATE" \
    --end "$INTEGRATION_TEST_DATE" \
    --schema trades \
    --skip-load

# Step 2: run q loader
echo "[2/3] Running q loader..."
cd "$PACKAGE_DIR"
q code/backfill/loader.q -e "runLoader[];exit 0"

# Step 3: run integration assertions
echo "[3/3] Running integration assertions..."
q tests/test_integration.q

echo ""
echo "=== Integration test PASSED ==="
