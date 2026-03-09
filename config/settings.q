// config/settings.q — package-level configuration for the backfill pipeline.
//
// Loaded by all q processes in this package after TorQ bootstrapping.
// Override any value by setting the corresponding environment variable before
// starting the process, or by creating a local override file.

// ---------------------------------------------------------------------------
// Databento dataset / schema defaults
// ---------------------------------------------------------------------------

// Default dataset to request from Databento
BACKFILL_DATASET:`$"XNAS.ITCH";

// Supported schemas — used for validation
BACKFILL_SCHEMAS:`trades`ohlcv_1m;

// Default schema when not specified on the CLI
BACKFILL_DEFAULT_SCHEMA:`trades;

// ---------------------------------------------------------------------------
// Chunking
// ---------------------------------------------------------------------------

// Maximum number of symbols per Databento batch job chunk.
// Smaller batches = finer-grained retry, but more API jobs.
BACKFILL_CHUNK_SIZE:10;

// ---------------------------------------------------------------------------
// Cost safeguard
// ---------------------------------------------------------------------------

// Abort a batch job submission if the estimated cost exceeds this threshold.
// Set to 0 to disable the check (not recommended in production).
BACKFILL_MAX_COST_USD:50.0;

// ---------------------------------------------------------------------------
// Retry policy
// ---------------------------------------------------------------------------

// Maximum number of retry attempts before a chunk is abandoned.
BACKFILL_MAX_RETRIES:3i;

// ---------------------------------------------------------------------------
// Paths
// ---------------------------------------------------------------------------

// Staging directory — where downloaded DBN/CSV files are stored temporarily.
// Override via the STAGING_DIR env var (set in setenv.sh).
STAGING_DIR:`$$[count s:getenv`STAGING_DIR;s;"staging"];

// HDB root directory
HDB_DIR:`$$[count s:getenv`KDBHDB;s;"hdb"];

// Job metadata directory inside staging
JOB_METADATA_DIR:`$string[STAGING_DIR],"/metadata";

// ---------------------------------------------------------------------------
// Logging
// ---------------------------------------------------------------------------

// Log level passed to TorQ's .lg functions (DEBUG=0, INFO=1, WARN=2, ERROR=3)
BACKFILL_LOG_LEVEL:1i;
