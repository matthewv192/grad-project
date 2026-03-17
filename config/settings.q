// config/settings.q — package-level configuration for the backfill pipeline.
//
// Loaded by all q processes in this package after TorQ bootstrapping.
// Override STAGING_DIR and KDBHDB via environment variables (set in setenv.sh).
// Pipeline parameters (chunk size, cost limit, retries, dataset) are controlled
// by environment variables read directly by orchestrator.py.

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

// Resolve PACKAGEHOME for building absolute defaults.
// getenv is the only reliable way to read env vars in kdb+5.
.settings.pkgHome:$[count p:getenv`PACKAGEHOME;p;""];

// Staging directory — where downloaded CSV files and manifests are stored.
// Override via the STAGING_DIR env var (set in setenv.sh).
// Falls back to $PACKAGEHOME/staging, or relative "staging" if PACKAGEHOME unset.
STAGING_DIR:`$$[count s:getenv`STAGING_DIR; s;
               count .settings.pkgHome; .settings.pkgHome,"/staging";
               "staging"];

// HDB root directory.
// Falls back to $PACKAGEHOME/hdb, or relative "hdb" if PACKAGEHOME unset.
HDB_DIR:`$$[count s:getenv`KDBHDB; s;
            count .settings.pkgHome; .settings.pkgHome,"/hdb";
            "hdb"];

// Job metadata directory inside staging
JOB_METADATA_DIR:`$string[STAGING_DIR],"/metadata";
