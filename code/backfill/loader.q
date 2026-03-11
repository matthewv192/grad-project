// loader.q — read staged CSV files and write them into kdb+ HDB partitions.
//
// Python writes CSVs + JSON manifests into staging/.
// This script reads those files, enforces types, and writes partitioned tables.
//
// Partition layout:
//   hdb/YYYY.MM.DD/trades/   (splayed table, parted on `sym)
//   hdb/YYYY.MM.DD/ohlcv_1m/ (splayed table, parted on `sym)
//
// Each partition is sorted by `sym`time before writing.
// .Q.chk is called after each load to fill missing tables across all dates.
//
// Run standalone (invoked via stdin piping from orchestrator.py):
//   echo "runLoader[];exit 0" | q code/backfill/loader.q

// Bootstrap TorQ logging if available, otherwise define a plain fallback.
// This lets loader.q work both inside a TorQ process and as a standalone script.
$[`TORQHOME in key .z.e;
    @[{system "l ",getenv[`TORQHOME],"/code/common/utils.q"};::;::];
    ::
 ];

// Fallback logger used when TorQ is not available
if[not `lg in key `.;
    .lg.o:{[proc;msg] -1 (string .z.p)," [",string[proc],"] ",msg;}
 ];

\l schema/schema.q
\l code/backfill/manifest.q
\l code/backfill/quality.q

// HDB root — set by setenv.sh → KDBHDB, defaulting to ./hdb relative to cwd.
// Use getenv directly: setenv updates the process env but NOT .z.e (startup snapshot).
HDB_DIR:hsym`$$[count s:getenv`KDBHDB;s;"hdb"];

// ---------------------------------------------------------------------------
// acquireWriteLock / releaseWriteLock — POSIX atomic mkdir-based partition lock.
//
// Only one writer may hold the lock for a given (partition, table) pair at a
// time.  `mkdir` is atomic on POSIX: exactly one caller succeeds; all others
// receive EEXIST.  Used for double-checked locking in loadTrades/loadOhlcv.
// ---------------------------------------------------------------------------
acquireWriteLock:{[partDateDir;tableName]
    // Ensure the partition date directory exists before attempting the atomic lock mkdir.
    // mkdir -p is idempotent; the atomic lock uses plain mkdir (no -p) so only one caller wins.
    @[system;"mkdir -p ",1_string partDateDir;::];
    lockDir:` sv partDateDir,`$".",string[tableName],".lock";
    err:@[system;"mkdir ",1_string lockDir;{[e]e}];
    if[count err; '"write lock held for ",1_string[partDateDir],"/",string[tableName]];
    lockDir
 };

releaseWriteLock:{[lockDir]
    @[system;"rmdir ",1_string lockDir;::]
 };

// ---------------------------------------------------------------------------
// unenumAll — unenumerate every enumerated symbol column (type 20h) in a table.
// .Q.dpft enumerates ALL symbol columns, not just `sym. When merging a newly-
// loaded dataset into an existing partition, every symbol column in the on-disk
// data must be unenumerated before concatenation, otherwise kdb+ signals 'mismatch
// or 'type on the concatenation step.
// ---------------------------------------------------------------------------
// unenumAll — unenumerate all type-20h columns.
// q lambdas do NOT close over outer-function parameters, so we cannot use a
// nested {type t[x]} lambda here — it would look for `t` globally and signal 'trror.
// Instead, extract column types directly: `type each value flip tbl` gives the
// numeric type of each column in the same order as `cols tbl`, with no nested lambda.
unenumAll:{[tbl]
    enumCols:(cols tbl) where 20h = type each value flip tbl;
    tbl {[t;c]@[t;c;{`$string x}]}/ enumCols
 };

// ---------------------------------------------------------------------------
// Column maps: Databento CSV header name → our schema column name
//
// Databento CSV columns (with map_symbols=True, pretty_px=True, pretty_ts=True):
//
//   trades: ts_recv, ts_event, rtype, publisher_id, instrument_id,
//           action, side, depth, price, size, flags, ts_in_delta, sequence, symbol
//
//   ohlcv-1m: ts_event, rtype, publisher_id, instrument_id,
//             open, high, low, close, volume, symbol
// ---------------------------------------------------------------------------

TRADES_COL_MAP:
    `ts_event`symbol`instrument_id`price`size`side`flags`sequence!
    `time`sym`instrument_id`price`size`side`conditions`sequence;

OHLCV_COL_MAP:
    `ts_event`symbol`instrument_id`open`high`low`close`volume!
    `time`sym`instrument_id`open`high`low`close`volume;

// ---------------------------------------------------------------------------
// readTradesCSV — parse the Databento trades CSV into a typed table.
//
// Databento trades CSV columns (14 total, map_symbols=True, pretty_ts=True,
// pretty_px=True):
//   1  ts_recv       P  — skip (we use ts_event for time)
//   2  ts_event      P  — bar/event timestamp → time
//   3  rtype         I  — skip
//   4  publisher_id  I  — skip
//   5  instrument_id J
//   6  action        S  — skip (always "T" in a trades file)
//   7  side          S  — A / B / N
//   8  depth         I  — skip
//   9  price         F  — float (pretty_px=True)
//  10  size          J
//  11  flags         I  — condition bitmask → stored as symbol string
//  12  ts_in_delta   J  — skip
//  13  sequence      J
//  14  symbol        S
// ---------------------------------------------------------------------------
readTradesCSV:{[csvPath]
    // 14-char type string — one char per CSV column; " " = skip that column
    raw:(" P  J S FJI JS";enlist csv) 0: csvPath;

    // Rename to our schema names; unmapped columns are dropped in the next step
    raw:TRADES_COL_MAP xcol raw;

    // Keep only columns present in our schema (drops action, depth, etc.)
    // Warn if any CSV-sourced schema column is absent — possible schema drift.
    // Exclude `date and `exchange: those are added by loadTrades after this
    // function returns and will never be present in the raw Databento CSV.
    csvSchemaCols:cols[trades] except `date`exchange;
    missingCols:csvSchemaCols where not csvSchemaCols in cols raw;
    if[count missingCols;
        .lg.e[`readTradesCSV;
            "CSV missing schema columns: ",", " sv string missingCols]
    ];
    raw:(cols[trades] inter cols raw)#raw;

    // Enforce exact types. conditions comes in as int (flags bitmask); convert
    // to a symbol string so it fits the `symbol$() schema column.
    raw:update
        sym:          `symbol$sym,
        time:         `timestamp$time,
        instrument_id:`long$instrument_id,
        price:        `float$price,
        size:         `long$size,
        side:         `symbol$side,
        conditions:   `$string conditions,   // int bitmask → symbol, e.g. `128
        sequence:     `long$sequence
    from raw;

    raw
 };

// ---------------------------------------------------------------------------
// readOhlcvCSV — parse the Databento ohlcv-1m CSV into a typed table.
//
// Databento ohlcv-1m CSV columns (10 total, map_symbols=True):
//   1  ts_event      P
//   2  rtype         I  — skip
//   3  publisher_id  I  — skip
//   4  instrument_id J
//   5  open          F
//   6  high          F
//   7  low           F
//   8  close         F
//   9  volume        J
//  10  symbol        S
// ---------------------------------------------------------------------------
readOhlcvCSV:{[csvPath]
    // 10-char type string
    raw:("P  JFFFFJS";enlist csv) 0: csvPath;
    raw:OHLCV_COL_MAP xcol raw;
    // `date and `exchange are added by loadOhlcv after this function returns.
    csvSchemaCols:cols[ohlcv_1m] except `date`exchange;
    missingCols:csvSchemaCols where not csvSchemaCols in cols raw;
    if[count missingCols;
        .lg.e[`readOhlcvCSV;
            "CSV missing schema columns: ",", " sv string missingCols]
    ];
    raw:(cols[ohlcv_1m] inter cols raw)#raw;
    raw:update
        sym:          `symbol$sym,
        time:         `timestamp$time,
        instrument_id:`long$instrument_id,
        open:  `float$open,
        high:  `float$high,
        low:   `float$low,
        close: `float$close,
        volume:`long$volume
    from raw;
    raw
 };

// ---------------------------------------------------------------------------
// loadTrades — load one trades CSV into the HDB for a given partition date.
// ---------------------------------------------------------------------------
// checkExchangeLoaded — returns 1b if the given exchange/dataset is already present
// in a partition, 0b if the partition is missing or the exchange is absent.
// Reads only the exchange column file for efficiency.
checkExchangeLoaded:{[partDateDir;tableName;exchange]
    if[not tableName in key partDateDir; :0b];
    exFile:` sv partDateDir,tableName,`exchange;
    if[not exFile in key exFile; :0b];   // pre-schema partition without exchange col
    // .Q.dpft enumerates ALL symbol columns; unenumerate before comparing
    exchange in `$string get exFile
 };

loadTrades:{[csvPath;partDate;dataset] '"deprecated - use loadChunkBatch"};

// ---------------------------------------------------------------------------
// loadOhlcv — load one ohlcv-1m CSV into the HDB for a given partition date.
// ---------------------------------------------------------------------------
loadOhlcv:{[csvPath;partDate;dataset] '"deprecated - use loadChunkBatch"};

// ---------------------------------------------------------------------------
// updateJobRecord — write quality/verified status back to the Python job store.
// Reads the existing JSON, updates status and optionally error_msg, writes back.
// This lets the q loader mark a chunk as `verified after quality checks pass.
// ---------------------------------------------------------------------------
updateJobRecord:{[chunkId;newStatus;errMsg]
    // Use getenv directly: in kdb+5, setenv does not update .z.e
    stagingStr:$[count s:getenv`STAGING_DIR;s;"staging"];
    jobsDir:hsym`$stagingStr,"/metadata/jobs";
    p:` sv jobsDir,`$(string chunkId),".json";
    if[not p in key p;
        .lg.o[`loader;"job record not found for chunk: ",string chunkId];
        :(::)
    ];
    raw:.j.k raze read0 p;
    raw[`status]:string newStatus;
    if[count errMsg; raw[`error_msg]:errMsg];
    raw[`updated_at]:string .z.p;
    // Write atomically via temp file then shell rename.
    // Detect mv failures explicitly — a silent failure leaves job state diverged.
    tmp:` sv jobsDir,`$(string[chunkId],".tmp");
    tmp 0: enlist .j.j raw;
    mvErr:@[system;"mv ",1_string[tmp]," ",1_string p;{[e]e}];
    if[count mvErr; @[hdel;tmp;::]; '"updateJobRecord: rename failed: ",mvErr];
    .lg.o[`loader;"job record updated: ",string[chunkId]," → ",string newStatus]
 };

// ---------------------------------------------------------------------------
// Symbology accumulator — collects (sym, instrument_id) pairs in memory
// during a loader run.  Written to disk once by flushSymbologyMap at the
// end of runLoader, avoiding N CSV read/write cycles for N-chunk backfills.
// ---------------------------------------------------------------------------
.loader.symPending:([] sym:`symbol$(); instrument_id:`long$(); exchange:`symbol$();
    valid_from:`date$(); valid_to:`date$());

// updateSymbologyMap — extract new (sym, instrument_id) pairs and stage them
// in the in-memory accumulator.  Does NOT write to disk.
updateSymbologyMap:{[raw;partDate]
    newRows:update valid_from:partDate, valid_to:9999.12.31
             from 0!(select by sym, instrument_id, exchange from raw);
    newRows:`sym`instrument_id`exchange`valid_from`valid_to#newRows;
    `.loader.symPending upsert newRows;
 };

// flushSymbologyMap — merge the in-memory accumulator with the on-disk CSV
// and write once.  Called by runLoader after all chunks are processed.
flushSymbologyMap:{[]
    if[0=count .loader.symPending; :(::)];
    stagingStr:$[count s:getenv`STAGING_DIR;s;"staging"];
    refDirStr:stagingStr,"/reference";
    refDir:hsym`$refDirStr;
    @[system;"mkdir -p ",refDirStr;::];
    mapPath:` sv refDir,`symbology_map.csv;
    existing:$[mapPath in key mapPath;
        ("SJSDD";enlist csv) 0: mapPath;
        ([] sym:`symbol$(); instrument_id:`long$(); exchange:`symbol$();
            valid_from:`date$(); valid_to:`date$())
    ];
    merged:existing,.loader.symPending;
    combined:0!(select first exchange, first valid_from, last valid_to
                by sym, instrument_id from merged);
    if[not (`sym`instrument_id xasc combined)~`sym`instrument_id xasc existing;
        mapPath 0: csv 0: combined;
        .lg.o[`loader;"symbology_map updated: total=",string[count combined]," sym-id pairs"]
    ];
    `.loader.symPending set 0#.loader.symPending
 };

// ---------------------------------------------------------------------------
// updateJobRecordTimestamps — write min_ts/max_ts back to the Python job store.
// Called after .Q.dpft completes to record the actual data time range.
// Observability only — no validation is performed.
// ---------------------------------------------------------------------------
updateJobRecordTimestamps:{[chunkId;minTs;maxTs]
    stagingStr:$[count s:getenv`STAGING_DIR;s;"staging"];
    jobsDir:hsym`$stagingStr,"/metadata/jobs";
    p:` sv jobsDir,`$(string chunkId),".json";
    if[not p in key p;
        .lg.o[`loader;"timestamp record not found for chunk: ",string chunkId];
        :(::)
    ];
    raw:.j.k raze read0 p;
    raw[`min_ts]:string minTs;
    raw[`max_ts]:string maxTs;
    raw[`updated_at]:string .z.p;
    tmp:` sv jobsDir,`$(string[chunkId],".tmp");
    tmp 0: enlist .j.j raw;
    mvErr:@[system;"mv ",1_string[tmp]," ",1_string p;{[e]e}];
    if[count mvErr; @[hdel;tmp;::]; '"updateJobRecordTimestamps: rename failed: ",mvErr]
 };

// ---------------------------------------------------------------------------
// updateMetrics — append load timing to the per-chunk metrics JSON file
// written by Python's metrics.py.  Silently skips if the file doesn't exist.
// ---------------------------------------------------------------------------
updateMetrics:{[chunkId;requestId;loadNs;rowCount]
    // Use getenv directly: in kdb+5, setenv does not update .z.e
    stagingStr:$[count s:getenv`STAGING_DIR;s;"staging"];
    metricsDir:hsym`$stagingStr,"/metrics/",string requestId;
    p:` sv metricsDir,`$(string chunkId),".json";
    if[not p in key p; :(::)];
    raw:.j.k raze read0 p;
    load_s:`float$loadNs%1000000000j;
    raw[`load_s]:load_s;
    raw[`rows_loaded]:rowCount;
    raw[`updated_at]:string .z.p;
    // Write atomically via tmp file then shell rename — same pattern as
    // updateJobRecord — prevents a q crash mid-write from corrupting the file.
    tmp:` sv metricsDir,`$(string[chunkId],".tmp");
    tmp 0: enlist .j.j raw;
    mvErr:@[system;"mv ",1_string[tmp]," ",1_string p;{[e]e}];
    if[count mvErr; @[hdel;tmp;::]; '"updateMetrics: rename failed: ",mvErr]
 };

// ---------------------------------------------------------------------------
// loadChunk — dispatch a validated manifest to the right loader function.
// Called by processManifests in manifest.q.
// ---------------------------------------------------------------------------
loadChunkBatch:{[manifests]
    if[0=count manifests; :0j];

    schema:first manifests`schema;
    partDate:first manifests`date;

    t0:.z.p;
    
    partDateDir:` sv HDB_DIR,`$string partDate;

    // Fast-path idempotency: exclude chunks loaded by checking exchange
    pendingIdx:where not {[partDateDir;schema;m] checkExchangeLoaded[partDateDir;schema;m`exchange]}[partDateDir;schema;] each manifests;
    pending:manifests pendingIdx;
    
    if[0=count pending;
        .lg.o[`loader;"all chunks in batch already loaded for date=",string[partDate]," schema=",string[schema]];
        :0j
    ];

    .lg.o[`loader;"loading batch: schema=",string[schema]," date=",string[partDate]," chunks=",string count pending];

    raws:{[m;schema]
        r:$[schema=`trades; readTradesCSV m`file_path; readOhlcvCSV m`file_path];
        if[not (count r)=m`row_count;
            .lg.e[`loader;"row_count MISMATCH for chunk ",string[m`chunk_id],": expected=",string[m`row_count]," actual=",string count r];
            updateJobRecord[m`chunk_id; `failed; "row_count mismatch: expected=",string[m`row_count]," actual=",string count r];
            :0b
        ];
        // Ensure exchange, chunk_id and date partition columns added
        if[count r; r:update date:m[`date], exchange:m[`exchange], chunk_id:m[`chunk_id] from r];
        r
    }[;schema] each pending;

    validIdx:where not raws~\:0b;
    validPending:pending validIdx;
    validRaws:raws validIdx;
    nValid:count validRaws;
    
    if[0=nValid; :0j];

    raw:(uj/) validRaws;
    
    nCombined:count raw;
    if[0=nCombined;
        .lg.o[`loader;"WARNING: empty combined batch"];
        {[m] updateJobRecord[m`chunk_id; `verified; ""]; updateJobRecord[m`chunk_id; `loaded; ""]} each validPending;
        :0j
    ];

    // Data quality checks on the freshly loaded table batch
    qResult:.[runQualityChecks; (raw; schema; partDate);
          {[e] .lg.e[`loader;"quality check error: ",e];
           `dups`ordering_errors`nulls`total_nulls`passed`failed`checks!(0j;0j;(`symbol$())!`long$();0j;0j;1j;3j)}];

    if[count[qResult] and qResult[`failed]>0j;
        msg:"quality failures: dups=",string[qResult`dups]," ordering=",string[qResult`ordering_errors]," nulls=",string[qResult`total_nulls];
        {[m;msg] updateJobRecord[m`chunk_id; `loaded; msg]; updateJobRecord[m`chunk_id; `failed; "quality checks failed"]} [;msg] each validPending;
        :0j
    ];

    // Proceed to dump to HDB
    raw:delete chunk_id from raw;
    raw:update date:partDate from `sym`time xasc delete date from raw;

    lockDir:acquireWriteLock[partDateDir;schema];

    merged:raw;
    if[schema in key partDateDir;
        existing:unenumAll get ` sv partDateDir,schema;
        existing:update date:partDate from existing;
        existing:(cols[value schema] inter cols existing)#existing;
        noDate:delete date from existing,((cols existing)#raw);
        merged:update date:partDate from `sym`time xasc noDate
    ];

    (`$string schema) set merged;
    .[.Q.dpft; (HDB_DIR; partDate; `sym; schema);
      {[lockDir;e] releaseWriteLock lockDir; 'e}[lockDir;]];
    releaseWriteLock lockDir;

    // Apply g# (grouped) attribute to the exchange column so that exchange-filtered
    // queries use a dictionary lookup rather than a linear scan.  Done after
    // .Q.dpft completes because .Q.dpft only attributes the parted column (sym).
    exchangeFile:` sv partDateDir,schema,`exchange;
    @[exchangeFile set; `g#get exchangeFile;
      {[e] .lg.o[`loader;"g# on exchange skipped: ",e]}];

    loadNs:(`long$.z.p-t0);
    timePath:` sv partDateDir,schema,`time;
    times:(); if[timePath in key timePath; times:get timePath];

    {[m;schema;partDate;loadNs;times;nValid;raw]
        chunkId:m`chunk_id;
        requestId:m`request_id;
        rowCount:m`row_count;
        dataset:m`exchange;
        
        updateJobRecord[chunkId; `verified; ""];
        updateJobRecord[chunkId; `loaded; ""];
        
        if[count times; @[updateJobRecordTimestamps; (chunkId; min times; max times); {[e] }]];
        
        // Per chunk updates
        @[updateMetrics; (chunkId; requestId; loadNs div nValid; rowCount); {[e] }];
    }[;schema;partDate;loadNs;times;nValid;raw] each validPending;
    
    // Batch updates
    @[updateSymbologyMap; (raw; partDate); {[e] .lg.e[`loader;"symbology map error: ",e]}];

    .lg.o[`loader;"batch loaded: date=",string[partDate]," schema=",string[schema]," chunks=",string[nValid]," new_rows=",string nCombined];

    nCombined
 };

// ---------------------------------------------------------------------------
// runLoader — main entry point.
// Scans the manifest directory for pending JSON files and processes each one.
// ---------------------------------------------------------------------------
runLoader:{[]
    manifestDir:`$$[count s:getenv`STAGING_DIR;s;"staging"],"/metadata/manifests";

    .lg.o[`loader;"runLoader: manifest dir=",string manifestDir];
    .lg.o[`loader;"runLoader: HDB dir=",string HDB_DIR];

    // Ensure HDB root exists
    @[system;"mkdir -p ",1_string HDB_DIR;::];

    // Pre-load the HDB sym into the global `sym so that enum domains resolve
    // correctly when unenumAll calls `get` on existing partitioned splayed tables.
    // kdb+ resolves an enum column's domain by looking up the domain name (here "sym")
    // as a global variable first.  Without this, a fresh q session that hasn't yet
    // called .Q.dpft has no `sym` in scope, and `get` on any partition with enumerated
    // symbol columns signals '..sym (sym file not found one level up).
    symPath:` sv HDB_DIR,`sym;
    if[symPath in key symPath; `sym set get symPath];  // global: visible to loadTrades/loadOhlcv

    processManifests manifestDir;

    // Flush accumulated symbology entries to disk once, after all chunks are loaded.
    // updateSymbologyMap stages entries in .loader.symPending during each loadChunk;
    // writing once here avoids N CSV read/write cycles for an N-chunk backfill.
    @[flushSymbologyMap; ::;
      {[e] .lg.e[`loader;"symbology flush error: ",e]}];

    // Fill missing tables across all partitions once, after all chunks are loaded.
    // Calling .Q.chk once here (vs once per chunk in loadChunk) avoids O(N*P) I/O
    // for multi-day backfills where N = chunks and P = existing partitions.
    // Error-trapped: mixed-schema HDBs (pre-dataset partitions alongside new ones)
    // can cause .Q.chk to signal 'type; treat as a non-fatal warning.
    @[.Q.chk; HDB_DIR;
      {[e] .lg.o[`loader;".Q.chk warning (schema mismatch in old partitions): ",e]}];

    // Best-effort: tell a running grad_hdb process to reload new partitions.
    // Port is read from KDBHDBPORT (default 6010, matching config/process.csv).
    // Silently skipped if no HDB is running — this is expected in standalone mode.
    hdbPort:`long$$[count s:getenv`KDBHDBPORT;s;"6010"];
    @[{[p] h:hopen`$"::",string p; h "system\"l .\""; hclose h};
      hdbPort;
      {[e] .lg.o[`loader;"HDB reload notification skipped (not running): ",e]}];

    .lg.o[`loader;"runLoader: complete"]
 };
