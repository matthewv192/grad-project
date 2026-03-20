#!/bin/bash
# setenv.sh — set environment variables for the grad-project backfill package.
# Source this before running any q or Python scripts:
#   source setenv.sh
#
# This package follows TorQ directory conventions (KDBAPPCONFIG, KDBAPPCODE,
# process.csv, etc.) so it can be plugged into a TorQ deployment, but it runs
# standalone — TorQ is NOT required.

# Use BASH_SOURCE[0] so this resolves correctly whether sourced interactively,
# from another script, or via an absolute path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# This package
export PACKAGEHOME="$SCRIPT_DIR"

# TorQ compatibility — set TORQHOME if TorQ is present alongside this package.
# The pipeline runs without TorQ; these vars are only used if you start
# processes via TorQ's process manager (config/process.csv).
if [[ -d "$SCRIPT_DIR/../TorQ" ]]; then
    export TORQHOME="$(cd "$SCRIPT_DIR/../TorQ" && pwd)"
    export TORQAPPHOME="$PACKAGEHOME"
    export KDBLIB="${TORQHOME}/lib"
    export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:$KDBLIB/l32"
fi

# Standard TorQ-compatible env vars pointing into this package
export KDBAPPCONFIG="$PACKAGEHOME/config"
export KDBAPPCODE="$PACKAGEHOME/code"
export KDBHDB="$PACKAGEHOME/hdb"
export TORQPROCESSES="$KDBAPPCONFIG/process.csv"

# Log and staging dirs
export KDBLOG="$PACKAGEHOME/logs"
export STAGING_DIR="$PACKAGEHOME/staging"

# Databento API key — EDIT ME: replace with your own key from https://app.databento.com/portal/keys
export DATABENTO_API_KEY="db-afYQDhMym3h5fEcEkjvCRaGgw9GwC"

# q command (override if kdb+ is not on PATH)
export QCMD="${QCMD:-q}"

echo "PACKAGEHOME = $PACKAGEHOME"
echo "KDBHDB      = $KDBHDB"
if [[ -n "${TORQHOME:-}" ]]; then
    echo "TORQHOME    = $TORQHOME"
else
    echo "TORQHOME    = (not set — TorQ not found, running standalone)"
fi
