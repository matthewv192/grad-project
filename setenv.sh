#!/bin/bash
# setenv.sh — set environment variables for the grad-project TorQ package.
# Source this before running any q or Python scripts:
#   source setenv.sh

# Use BASH_SOURCE[0] so this resolves correctly whether sourced interactively,
# from another script, or via an absolute path.
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# TorQ framework location (read-only — do NOT modify TorQ/)
export TORQHOME="$(cd "$SCRIPT_DIR/../TorQ" && pwd)"

# This package
export PACKAGEHOME="$SCRIPT_DIR"
export TORQAPPHOME="$PACKAGEHOME"

# Standard TorQ env vars pointing into this package
export KDBAPPCONFIG="$PACKAGEHOME/config"
export KDBAPPCODE="$PACKAGEHOME/code"
export KDBHDB="$PACKAGEHOME/hdb"
export TORQPROCESSES="$KDBAPPCONFIG/process.csv"

# Log and staging dirs
export KDBLOG="$PACKAGEHOME/logs"
export STAGING_DIR="$PACKAGEHOME/staging"

# Databento API key — EDIT ME: replace with your own key from https://app.databento.com/portal/keys
export DATABENTO_API_KEY="db-afYQDhMym3h5fEcEkjvCRaGgw9GwC"

# Inherit TorQ's library paths
export KDBLIB="${TORQHOME}/lib"
export LD_LIBRARY_PATH="${LD_LIBRARY_PATH:-}:$KDBLIB/l32"

# q command (override if kdb+ is not on PATH)
export QCMD="${QCMD:-q}"

echo "TORQHOME    = $TORQHOME"
echo "PACKAGEHOME = $PACKAGEHOME"
echo "KDBHDB      = $KDBHDB"
