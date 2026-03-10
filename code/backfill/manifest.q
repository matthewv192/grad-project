// manifest.q — read and validate JSON manifests written by orchestrator.py,
//              then dispatch each to loadChunk (defined in loader.q).
//
// Manifest JSON schema (one file per downloaded CSV):
// {
//   "request_id":       "req_20240115_001",
//   "chunk_id":         "req_20240115_001_chunk000",
//   "databento_job_id": "DBNJ-XXXXX",
//   "schema":           "trades",
//   "date":             "2024-01-15",
//   "symbols":          ["AAPL","MSFT"],
//   "file_path":        "/abs/path/to/chunk.csv",
//   "row_count":        12345,
//   "checksum":         "sha256:abcdef...",
//   "created_at":       "2024-01-15T10:00:00+00:00"
// }

// Fallback logger (overridden if TorQ is loaded before this file)
if[not `lg in key `.;
    .lg.o:{[proc;msg] -1 (string .z.p)," [",string[proc],"] ",msg;}
 ];

\l schema/schema.q

// ---------------------------------------------------------------------------
// readManifest — parse a single JSON manifest file.
// Returns a typed dictionary, or signals an error string.
// ---------------------------------------------------------------------------
readManifest:{[manifestPath]
    // Read the file and join lines, then parse as JSON
    raw:.j.k raze read0 hsym manifestPath;

    required:`request_id`chunk_id`schema`date`file_path`checksum;
    missing:required where not required in key raw;
    if[count missing;
        '"manifest missing keys: ",", " sv string missing
    ];

    // Build the dict in one expression.
    // kdb+5 specialises the value vector's type once all values share one type.
    // Building incrementally (m[`k]:v) after adding several symbols locks the
    // value vector to 11h (symbol list) and then rejects a date assignment.
    // Constructing via keys!values keeps the value vector as a generic list (0h).
    m:(`request_id`chunk_id`databento_job_id`exchange`schema`date`file_path`checksum`symbols`row_count`min_ts`max_ts)!
      (`$raw`request_id;
       `$raw`chunk_id;
       `$raw`databento_job_id;
       // "exchange" is the current key; "dataset" is the legacy key for old manifests
       `$$[`exchange in key raw; raw`exchange; `dataset in key raw; raw`dataset; "XNAS.ITCH"];
       `$ssr[raw`schema;"-";"_"];    // normalise Databento "ohlcv-1m" → `ohlcv_1m
       "D"$ssr[raw`date;"-";"."];     // Python writes YYYY-MM-DD; q needs YYYY.MM.DD
       hsym`$raw`file_path;
       `$raw`checksum;
       `$raw`symbols;
       `long$raw`row_count;
       $[0<count raw`min_ts; "P"$raw`min_ts; 0Np];
       $[0<count raw`max_ts; "P"$raw`max_ts; 0Np]);
    m
 };

// ---------------------------------------------------------------------------
// validateManifest — sanity checks on a parsed manifest dict.
// Returns 1b if valid, signals an error string if not.
// ---------------------------------------------------------------------------
validateManifest:{[m]
    if[not (m`file_path) in key m`file_path;
        '"file not found: ",string m`file_path
    ];
    if[not (m`schema) in `trades`ohlcv_1m;
        '"unknown schema: ",string m`schema
    ];
    if[(m`row_count)<=0;
        '"non-positive row_count: ",string m`row_count
    ];
    1b
 };

// ---------------------------------------------------------------------------
// scanManifestDir — list all *.json files in a directory.
// Returns a list of fully-qualified file handles.
// ---------------------------------------------------------------------------
scanManifestDir:{[stagingPath]
    p:hsym stagingPath;
    // key on a directory returns a list of file names as symbols
    allFiles:key p;
    if[0=count allFiles; :()];
    jsonFiles:allFiles where allFiles like "*.json";
    // Build absolute paths.
    // Use ((base),"/"),/:filenames so the full base string is formed first,
    // then /:distributes it across each filename correctly.
    hsym each `$((1_string p),"/"),/:string jsonFiles
 };

// ---------------------------------------------------------------------------
// readJobStatus — look up the current status of a chunk in the job store.
// Returns the status as a symbol, or `unknown if the record doesn't exist.
// jobsDir: hsym path to staging/metadata/jobs/
// chunkId: symbol
// ---------------------------------------------------------------------------
readJobStatus:{[jobsDir;chunkId]
    p:` sv jobsDir,`$(string chunkId),".json";
    if[not p in key p; :`unknown];
    jr:@[{.j.k raze read0 x};p;{[e]`$""}];
    if[jr~`$""; :`unknown];
    `$jr`status
 };

// ---------------------------------------------------------------------------
// processManifests — read, validate, and load all manifests in a directory.
// loadChunk must be defined before this is called (it lives in loader.q).
//
// Job store awareness:
//   verified → skip (already done; idempotency would catch it anyway)
//   loading  → retry with a warning (loader crashed mid-run last time)
//   anything else (pending, downloaded, failed, unknown) → process normally
// ---------------------------------------------------------------------------
processManifests:{[stagingPath]
    manifests:scanManifestDir stagingPath;

    if[0=count manifests;
        .lg.o[`manifest;"no manifests found in ",string stagingPath];
        :()
    ];

    .lg.o[`manifest;"processing ",string[count manifests]," manifest(s)"];

    // Derive jobs dir: staging/metadata/manifests → staging/metadata/jobs
    jobsDir:hsym`$ssr[1_string hsym`$string stagingPath;"manifests";"jobs"];

    results:{[jobsDir;mPath]
        .lg.o[`manifest;"reading ",string mPath];

        // readManifest can signal — catch errors, log, and return 0 rows for this chunk
        m:@[readManifest; mPath; {[e] .lg.o[`manifest;"read error: ",e]; 0b}];
        if[m~0b; :0j];

        // Check job store status before doing any work
        status:readJobStatus[jobsDir; m`chunk_id];
        if[status=`verified;
            .lg.o[`manifest;"skipping verified chunk: ",string m`chunk_id];
            // Archive to keep the active manifest dir lean.
            // Verified manifests accumulate over many runs and are scanned
            // (and skipped) on every invocation — archiving them to a
            // subdirectory removes them from the hot path entirely.
            archDir:ssr[1_string jobsDir;"jobs";"manifests/archive"];
            @[system;"mkdir -p ",archDir;::];
            @[system;"mv ",1_string[mPath]," ",archDir,"/";::];
            :0j
        ];
        if[status=`loading;
            .lg.o[`manifest;"WARNING: retrying chunk stuck in loading state: ",
                  string m`chunk_id]
        ];

        valid:@[validateManifest; m; {[e] .lg.o[`manifest;"validation error: ",e]; 0b}];
        if[valid~0b; :0j];

        // loadChunk is defined in loader.q which loads this file
        n:@[loadChunk; m; {[e] .lg.o[`manifest;"load error: ",e]; 0j}];
        n
    }[jobsDir;] each manifests;

    total:sum results;
    .lg.o[`manifest;"total rows loaded: ",string total];
    total
 };

// ---------------------------------------------------------------------------
// showJobsTable — materialise the JSON job store as a kdb+ table on demand.
//
// Parameters:
//   jobsDir - path to staging/metadata/jobs/ (symbol or string)
//
// Returns a table with one row per job record, columns:
//   request_id chunk_id dataset schema date status retries row_count
//   error_msg failure_type created_at updated_at
// Returns an empty typed table if the directory doesn't exist or is empty.
// ---------------------------------------------------------------------------
showJobsTable:{[jobsDir]
    p:hsym jobsDir;
    empty:([]
        request_id:`symbol$();
        chunk_id:`symbol$();
        dataset:`symbol$();
        schema:`symbol$();
        date:`date$();
        status:`symbol$();
        retries:`long$();
        row_count:`long$();
        error_msg:`symbol$();
        failure_type:`symbol$();
        created_at:`symbol$();
        updated_at:`symbol$());
    allFiles:@[key;p;{[e]0b}];
    if[allFiles~0b;
        .lg.o[`manifest;"showJobsTable: dir not found: ",string p];
        :empty
    ];
    jsonFiles:allFiles where allFiles like "*.json";
    if[0=count jsonFiles;:empty];
    paths:hsym each `$((1_string p),"/"),/:string jsonFiles;
    rows:{[fp]
        jr:@[{.j.k raze read0 x};fp;{[e]0b}];
        if[jr~0b;:()];
        ([]
            request_id:enlist`$$[`request_id in key jr;jr`request_id;""];
            chunk_id:enlist`$$[`chunk_id in key jr;jr`chunk_id;""];
            dataset:enlist`$$[`dataset in key jr;jr`dataset;
                                `exchange in key jr;jr`exchange;""];
            schema:enlist`$$[`schema in key jr;jr`schema;""];
            date:enlist "D"$ssr[$[`date in key jr;jr`date;"2000.01.01"];"-";"."];
            status:enlist`$$[`status in key jr;jr`status;"unknown"];
            retries:enlist`long$$[`retries in key jr;jr`retries;0];
            row_count:enlist`long$$[`row_count in key jr;jr`row_count;0];
            error_msg:enlist`$$[`error_msg in key jr;jr`error_msg;""];
            failure_type:enlist`$$[`failure_type in key jr;jr`failure_type;""];
            created_at:enlist`$$[`created_at in key jr;jr`created_at;""];
            updated_at:enlist`$$[`updated_at in key jr;jr`updated_at;""])
    } each paths;
    rows:rows where 0<count each rows;
    if[0=count rows;:empty];
    (uj/) rows
 };
