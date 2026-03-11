#!/bin/bash
# setup_python.sh — install Python dependencies for the grad-project package.
#
# Usage:  ./scripts/setup_python.sh
#
# This script:
#   1. Creates (or reuses) a Python virtual environment at ../venv
#      (sibling to grad-project, e.g. ~/venv)
#   2. Installs the package and its dependencies via pip
#
# kdb+ itself must be installed separately and available on PATH as 'q'.
# See: https://kx.com/products/kdb-personal-edition/ for a free licence.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
VENV_DIR="$(cd "$PACKAGE_DIR/.." && pwd)/venv"

echo "=== grad-project installer ==="
echo ""

# 1. Python venv
if [ ! -d "$VENV_DIR" ]; then
    echo "[1/2] Creating Python virtual environment at $VENV_DIR ..."
    python3 -m venv "$VENV_DIR"
else
    echo "[1/2] Python virtual environment already exists at $VENV_DIR"
fi

# shellcheck source=/dev/null
source "$VENV_DIR/bin/activate"

# 2. Install Python deps
echo "[2/2] Installing Python dependencies..."
pip install --upgrade pip --quiet
pip install -e "$PACKAGE_DIR/" --quiet

echo ""
echo "Installation complete."
echo ""
echo "Next steps:"
echo "  1. export DATABENTO_API_KEY=your-key-here"
echo "  2. source $PACKAGE_DIR/setenv.sh"
echo "  3. ./scripts/request_backfill.sh --symbols AAPL --start 2024-01-15 --end 2024-01-15 --dry-run"
