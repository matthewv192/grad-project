// config/settings.q — package-level configuration for the backfill pipeline.
//
// Loaded by all q processes in this package after TorQ bootstrapping.
// Override STAGING_DIR and KDBHDB via environment variables (set in setenv.sh).
// Pipeline parameters (chunk size, cost limit, retries, dataset) are controlled
// by environment variables read directly by orchestrator.py.

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

// Staging directory — where downloaded CSV files and manifests are stored.
// Override via the STAGING_DIR env var (set in setenv.sh).
STAGING_DIR:`$$[count s:getenv`STAGING_DIR;s;"staging"];

// HDB root directory
HDB_DIR:`$$[count s:getenv`KDBHDB;s;"hdb"];

// Job metadata directory inside staging
JOB_METADATA_DIR:`$string[STAGING_DIR],"/metadata";
