#!/bin/bash
# run_tests.sh — run the full test suite and exit 1 if any test fails.
#
# Usage (from any directory):
#   ./scripts/run_tests.sh
#
# Each test file runs in isolation.  The script collects failures and
# prints a summary at the end so all results are visible in one pass.
# Exits 0 only when every test file passes.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

cd "$PACKAGE_DIR"

# Source environment (sets KDBHDB, STAGING_DIR, etc.)
source "$PACKAGE_DIR/setenv.sh"

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

GREEN='\033[0;32m'
RED='\033[0;31m'
NC='\033[0m'

PASS=0
FAIL=0
FAILURES=()

YELLOW='\033[0;33m'

run_q_test() {
    local label="$1"
    local file="$2"
    local output
    printf "  %-45s" "$label"
    output=$(q "$file" -q 2>&1)
    if echo "$output" | grep -qE "^SKIP:"; then
        echo -e "${YELLOW}SKIP${NC}"
    elif echo "$output" | grep -qE "^FAIL:"; then
        echo -e "${RED}FAIL${NC}"
        FAIL=$((FAIL + 1))
        FAILURES+=("$label ($file)")
    else
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
    fi
}

run_py_test() {
    local label="$1"
    local file="$2"
    printf "  %-45s" "$label"
    if python3 -m pytest "$file" -q --tb=no 2>&1 | grep -qE "passed"; then
        echo -e "${GREEN}PASS${NC}"
        PASS=$((PASS + 1))
    else
        echo -e "${RED}FAIL${NC}"
        FAIL=$((FAIL + 1))
        FAILURES+=("$label ($file)")
    fi
}

# ---------------------------------------------------------------------------
# Activate Python venv if present
# ---------------------------------------------------------------------------

VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"
if [ -f "$VENV_DIR/bin/activate" ]; then
    # shellcheck disable=SC1091
    source "$VENV_DIR/bin/activate"
fi

# ---------------------------------------------------------------------------
# q test suite
# ---------------------------------------------------------------------------

echo ""
echo "=== q tests ==="
run_q_test "schema"      tests/test_schema.q
run_q_test "manifest"    tests/test_manifest.q
run_q_test "loader"      tests/test_loader.q
run_q_test "quality"     tests/test_quality.q
run_q_test "symbology"   tests/test_symbology.q
run_q_test "ref_tables"  tests/test_ref_tables.q
run_q_test "adj"         tests/test_adj.q
# integration test runs whenever KDBHDB points to an existing directory.
# Date and sym are auto-discovered from the HDB; the test skips gracefully
# if no trades data is found.  Override with INTEGRATION_TEST_DATE /
# INTEGRATION_TEST_SYM to target a specific partition.
if [ -n "${KDBHDB:-}" ] && [ -d "${KDBHDB}" ]; then
    run_q_test "integration" tests/test_integration.q
else
    printf "  %-45s" "integration"
    echo -e "\033[0;33mSKIP\033[0m (set KDBHDB to a populated HDB to enable)"
fi

# ---------------------------------------------------------------------------
# Python test suite
# ---------------------------------------------------------------------------

echo ""
echo "=== Python tests ==="
run_py_test "orchestrator" tests/test_orchestrator.py
run_py_test "metrics"      tests/test_metrics.py
run_py_test "ref_ingest"   tests/test_ref_ingest.py

# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------

echo ""
echo "=== Results ==="
echo -e "  Passed: ${GREEN}${PASS}${NC}"
echo -e "  Failed: ${RED}${FAIL}${NC}"

if [ ${#FAILURES[@]} -gt 0 ]; then
    echo ""
    echo "Failed tests:"
    for f in "${FAILURES[@]}"; do
        echo "  - $f"
    done
    echo ""
    exit 1
fi

echo ""
echo -e "${GREEN}All tests passed.${NC}"
exit 0
