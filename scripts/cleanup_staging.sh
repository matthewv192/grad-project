#!/bin/bash
# cleanup_staging.sh — remove old staging data and rotated log files.
#
# Intended to be run manually or via cron to prevent disk bloat from
# accumulated backfill artefacts.  Safe to run while a backfill is in
# progress — it only removes data older than the retention window.
#
# Usage:
#   ./scripts/cleanup_staging.sh                 # defaults: 7-day staging, 30-day logs
#   STAGING_DAYS=3 LOG_DAYS=14 ./scripts/cleanup_staging.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

STAGING_DIR="${STAGING_DIR:-$PACKAGE_DIR/staging}"
LOG_DIR="${LOG_DIR:-$PACKAGE_DIR/logs}"

# Retention periods (days).  Override via env vars.
STAGING_DAYS="${STAGING_DAYS:-7}"
LOG_DAYS="${LOG_DAYS:-30}"

echo "=== Staging / log cleanup ==="
echo "  Staging dir : $STAGING_DIR  (remove dirs older than ${STAGING_DAYS}d)"
echo "  Log dir     : $LOG_DIR  (remove files older than ${LOG_DAYS}d)"
echo ""

# --- Staging chunk directories ---
# Each chunk creates a directory like staging/<chunk_id>/ containing CSVs.
# Verified chunks are cleaned by the orchestrator automatically, but failed
# or abandoned chunks accumulate.  Remove any chunk dir older than the
# retention window.
cleaned=0
if [[ -d "$STAGING_DIR" ]]; then
    while IFS= read -r -d '' dir; do
        rm -rf "$dir"
        cleaned=$((cleaned + 1))
    done < <(find "$STAGING_DIR" -mindepth 1 -maxdepth 1 -type d \
             -not -name "metadata" -not -name "reference" \
             -mtime +"$STAGING_DAYS" -print0)
    echo "  Removed $cleaned staging chunk dir(s)"
else
    echo "  Staging dir does not exist — nothing to clean"
fi

# --- Log files ---
log_cleaned=0
if [[ -d "$LOG_DIR" ]]; then
    while IFS= read -r -d '' f; do
        rm -f "$f"
        log_cleaned=$((log_cleaned + 1))
    done < <(find "$LOG_DIR" -maxdepth 1 -type f -name "*.log*" \
             -mtime +"$LOG_DAYS" -print0)
    echo "  Removed $log_cleaned log file(s)"
else
    echo "  Log dir does not exist — nothing to clean"
fi

echo ""
echo "Done."
