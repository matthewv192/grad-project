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
    t:update date:string date from t;
    t:update created_at:tsStr each created_at from t;
    t:update updated_at:tsStr each updated_at from t;
    t:update min_ts:tsStr each min_ts from t;
    t:update max_ts:tsStr each max_ts from t;
    t:update request_id:string request_id from t;
    t:update chunk_id:string chunk_id from t;
    t:update databento_job_id:string databento_job_id from t;
    t:update dataset:string dataset from t;
    t:update schema:string schema from t;
    t:update status:string status from t;
    t:update failure_type:string failure_type from t;
    t:update file_path:string file_path from t;
    t:update checksum:string checksum from t;
    .j.j t
 };

// ---------------------------------------------------------------------------
// Dispatch on op
// ---------------------------------------------------------------------------

cmd:.j.k getenv`JOBSTORE_CMD;
op:cmd`op;

$[op~"upsert";
    [`jobs set delete from jobs where chunk_id=`$(cmd`record)`chunk_id;
     `jobs set jobs,buildRow[cmd`record];
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
