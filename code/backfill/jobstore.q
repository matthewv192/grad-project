// jobstore.q — kdb-backed persistent job store for the backfill orchestrator.
//
// Reads a JSON command from the JOBSTORE_CMD environment variable, performs the
// requested operation on a binary kdb table file, then exits.  Called by
// KdbJobStore in orchestrator.py via: q -q with \l code/backfill/jobstore.q
// piped via stdin, and JOBSTORE_CMD env var carrying the JSON payload.
//
// Environment variables:
//   JOBSTORE_CMD — JSON command object (required)
//   JOBS_FILE    — absolute path to the kdb binary table file
//                  default: staging/metadata/backfill_jobs (relative to cwd)
//
// Command JSON — one of:
//   {"op":"upsert","record":{...}}
//   {"op":"batchUpsert","records":[{...},{...},...]}
//   {"op":"load","chunk_id":"..."}
//   {"op":"loadAll"}
//   {"op":"loadFailed","max_retries":3}
//
// Stdout:
//   upsert  → prints "ok"
//   others  → prints a JSON array of record objects

\l schema/schema.q

JOBS_FILE:hsym`$$[count s:getenv`JOBS_FILE;s;"staging/metadata/backfill_jobs"];

// Create parent directory
system "mkdir -p ",-1_"/" sv 1_"/" vs 1_string JOBS_FILE;

// Load existing table or start fresh
jobs:$[count key JOBS_FILE;get JOBS_FILE;backfill_jobs];

// ---------------------------------------------------------------------------
// Type conversion helpers
// ---------------------------------------------------------------------------

// Parse a date string "YYYY-MM-DD" → date (or null)
parseDate:{$[0=count x;0Nd;"D"$x]};

// Parse a kdb ts string "YYYY.MM.DDTHH:MM:SS.nnnnnnnnn" → timestamp (or null)
parseTs:{$[0=count x;0Np;"P"$x]};

// Timestamp → string for JSON output ("" for null)
tsStr:{$[x=0Np;"";string x]};

// Build a one-row table from a parsed JSON record dict.
// JSON strings arrive as char vectors — no need to call string first.
buildRow:{[r]
    ([]
        request_id:      enlist`$r`request_id;
        chunk_id:        enlist`$r`chunk_id;
        databento_job_id:enlist`$r`databento_job_id;
        dataset:         enlist`$r`dataset;
        schema:          enlist`$r`schema;
        symbols:         enlist r`symbols;
        date:            enlist parseDate r`date;
        status:          enlist`$r`status;
        retries:         enlist`int$r`retries;
        error_msg:       enlist enlist r`error_msg;
        failure_type:    enlist`$r`failure_type;
        file_path:       enlist`$r`file_path;
        file_paths:      enlist r`file_paths;
        checksum:        enlist`$r`checksum;
        row_count:       enlist`long$r`row_count;
        min_ts:          enlist parseTs r`min_ts;
        max_ts:          enlist parseTs r`max_ts;
        created_at:      enlist parseTs r`created_at;
        updated_at:      enlist parseTs r`updated_at
    )
 };

// Convert table to JSON-serializable form (dates/timestamps → strings)
tableToJson:{[t]
    t:update date:string date,
             created_at:tsStr each created_at,
             updated_at:tsStr each updated_at,
             min_ts:tsStr each min_ts,
             max_ts:tsStr each max_ts,
             request_id:string request_id,
             chunk_id:string chunk_id,
             databento_job_id:string databento_job_id,
             dataset:string dataset,
             schema:string schema,
             status:string status,
             failure_type:string failure_type,
             file_path:string file_path,
             checksum:string checksum
        from t;
    .j.j t
 };

// ---------------------------------------------------------------------------
// Dispatch on op
// ---------------------------------------------------------------------------

// Guard: exit with usage if JOBSTORE_CMD is not set
if[0=count getenv`JOBSTORE_CMD;
    -2 "error: JOBSTORE_CMD environment variable is required";
    -2 "usage: JOBSTORE_CMD='{\"op\":\"loadAll\"}' JOBS_FILE=<path> q -q code/backfill/jobstore.q";
    exit 1];

cmd:.j.k getenv`JOBSTORE_CMD;
op:cmd`op;

upsertOne:{[r]
    `jobs set delete from jobs where chunk_id=`$(r`chunk_id);
    `jobs set jobs,buildRow[r];
 };

$[op~"upsert";
    [upsertOne[cmd`record];
     JOBS_FILE set jobs;
     -1 "ok";
    ];
  op~"batchUpsert";
    [upsertOne each cmd`records;
     JOBS_FILE set jobs;
     -1 "ok";
    ];
  op~"load";
    [cid:`$cmd`chunk_id;
     t:select from jobs where chunk_id=cid;
     -1 tableToJson[t];
    ];
  op~"loadAll";
    [-1 tableToJson[jobs];
    ];
  op~"loadFailed";
    [mr:`int$cmd`max_retries;
     t:select from jobs where status=`failed,retries<mr;
     -1 tableToJson[t];
    ];
    [-2 "unknown op: ",op]
 ];

exit 0
