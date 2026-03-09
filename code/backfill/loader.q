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
// Run standalone:
//   q code/backfill/loader.q -e "runLoader[];exit 0"

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

// HDB root — set by setenv.sh → KDBHDB, defaulting to ./hdb relative to cwd
HDB_DIR:hsym`$$[`KDBHDB in key .z.e;getenv`KDBHDB;"hdb"];

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
loadTrades:{[csvPath;partDate]
    // Idempotency: skip if this date's trades partition already exists WITH data.
    // An empty directory (0 rows) is a .Q.chk cross-fill placeholder — overwrite it.
    partDateDir:` sv HDB_DIR,`$string partDate;
    if[`trades in key partDateDir;
        n:count get ` sv partDateDir,`trades`sym;
        if[n>0;
            .lg.o[`loader;"trades already loaded for ",string[partDate],
                  " (",string[n]," rows) — skipping"];
            :n
        ]
    ];

    .lg.o[`loader;"loading trades: ",string csvPath];
    t0:.z.p;

    raw:readTradesCSV csvPath;

    if[0=count raw;
        .lg.o[`loader;"WARNING: empty CSV, skipping: ",string csvPath];
        :0j
    ];

    // Add the date partition column (not in Databento CSV — derived from filename)
    raw:update date:partDate from raw;

    // Sort by sym then time — required for binary search queries in the HDB
    raw:`sym`time xasc raw;

    // .Q.dpft[d;p;f;t] writes a splayed partition and updates the sym file.
    // It requires `t` to be a global symbol whose value is the table to write.
    // We stage into the global `trades` variable (clobbers the empty schema table,
    // which is expected — after a load run you reload from the HDB anyway).
    `trades set raw;
    .Q.dpft[HDB_DIR; partDate; `sym; `trades];

    n:count raw;
    elapsed:(`long$(.z.p-t0))%1000000;
    .lg.o[`loader;"trades loaded: date=",string[partDate]," rows=",string[n],
          " elapsed=",string[elapsed],"ms"];
    n
 };

// ---------------------------------------------------------------------------
// loadOhlcv — load one ohlcv-1m CSV into the HDB for a given partition date.
// ---------------------------------------------------------------------------
loadOhlcv:{[csvPath;partDate]
    // Idempotency: skip if this date's ohlcv_1m partition already exists WITH data.
    // An empty directory (0 rows) is a .Q.chk cross-fill placeholder — overwrite it.
    partDateDir:` sv HDB_DIR,`$string partDate;
    if[`ohlcv_1m in key partDateDir;
        n:count get ` sv partDateDir,`ohlcv_1m`sym;
        if[n>0;
            .lg.o[`loader;"ohlcv_1m already loaded for ",string[partDate],
                  " (",string[n]," rows) — skipping"];
            :n
        ]
    ];

    .lg.o[`loader;"loading ohlcv_1m: ",string csvPath];
    t0:.z.p;

    raw:readOhlcvCSV csvPath;

    if[0=count raw;
        .lg.o[`loader;"WARNING: empty CSV, skipping: ",string csvPath];
        :0j
    ];

    raw:update date:partDate from raw;
    raw:`sym`time xasc raw;

    `ohlcv_1m set raw;
    .Q.dpft[HDB_DIR; partDate; `sym; `ohlcv_1m];

    n:count raw;
    elapsed:(`long$(.z.p-t0))%1000000;
    .lg.o[`loader;"ohlcv_1m loaded: date=",string[partDate]," rows=",string[n],
          " elapsed=",string[elapsed],"ms"];
    n
 };

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
    // Write atomically via temp file then shell rename
    tmp:` sv jobsDir,`$(string[chunkId],".tmp");
    tmp 0: enlist .j.j raw;
    @[system;"mv ",1_string[tmp]," ",1_string p;::];
    .lg.o[`loader;"job record updated: ",string[chunkId]," → ",string newStatus]
 };

// ---------------------------------------------------------------------------
// updateSymbologyMap — extract distinct (sym, instrument_id) pairs from the
// loaded table and upsert into staging/reference/symbology_map.csv.
// Accumulates across all loads; never removes existing entries.
// ---------------------------------------------------------------------------
updateSymbologyMap:{[raw;partDate]
    // Use getenv directly: in kdb+5, setenv does not update .z.e
    stagingStr:$[count s:getenv`STAGING_DIR;s;"staging"];
    refDirStr:stagingStr,"/reference";
    refDir:hsym`$refDirStr;
    @[system;"mkdir -p ",refDirStr;::];
    mapPath:` sv refDir,`symbology_map.csv;

    // Extract distinct (sym, instrument_id) pairs from this partition
    newRows:update dataset:`unknown, valid_from:partDate, valid_to:9999.12.31
             from 0!(select by sym, instrument_id from raw);
    newRows:`sym`instrument_id`dataset`valid_from`valid_to#newRows;

    // Load existing map or start with empty schema-compatible table
    existing:$[mapPath in key mapPath;
        ("SJSDD";enlist csv) 0: mapPath;
        ([] sym:`symbol$(); instrument_id:`long$(); dataset:`symbol$();
            valid_from:`date$(); valid_to:`date$())
    ];

    // Merge: group by (sym,instrument_id), keeping first valid_from (oldest seen)
    merged:existing,newRows;
    combined:0!(select first dataset, first valid_from, last valid_to
                by sym, instrument_id from merged);

    if[count[combined]>count existing;
        mapPath 0: csv 0: combined;
        .lg.o[`loader;"symbology_map: total=",string[count combined]," sym-id pairs"]
    ]
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
    raw[`row_count]:rowCount;
    raw[`updated_at]:string .z.p;
    p 0: enlist .j.j raw
 };

// ---------------------------------------------------------------------------
// loadChunk — dispatch a validated manifest to the right loader function.
// Called by processManifests in manifest.q.
// ---------------------------------------------------------------------------
loadChunk:{[manifest]
    schema:manifest`schema;
    csvPath:manifest`file_path;
    partDate:manifest`date;
    chunkId:manifest`chunk_id;
    requestId:manifest`request_id;

    t0:.z.p;

    // Route to the correct loader based on schema
    n:$[schema=`trades;
        loadTrades[csvPath; partDate];
        schema=`ohlcv_1m;
        loadOhlcv[csvPath; partDate];
        '"unsupported schema: ",string schema
    ];

    // Fill missing tables in other partitions so cross-date queries don't error.
    // E.g. if only trades exists for 2024-01-15, ohlcv_1m is created as empty there.
    .Q.chk HDB_DIR;

    loadNs:`long$.z.p-t0;

    // Feature 5: row-count verification against manifest
    expRows:manifest`row_count;
    $[n=expRows;
        .lg.o[`loader;"row_count verified: expected=",string[expRows]," actual=",string n];
        .lg.e[`loader;"row_count MISMATCH: expected=",string[expRows]," actual=",string n]
    ];

    // Feature 4: data quality checks on the freshly loaded table global
    qResult:$[n>0;
        .[runQualityChecks; (value schema; schema; partDate);
          {[e] .lg.e[`loader;"quality check error: ",e];
           `dups`ordering_errors`nulls`total_nulls`passed`failed`checks!
           (0j;0j;(`symbol$())!`long$();0j;0j;1j;3j)}];
        ()
    ];

    // Update job record: verified if all quality checks pass; else keep loaded
    $[0<count qResult;
        $[qResult[`failed]=0j;
            updateJobRecord[chunkId; `verified; ""];
            updateJobRecord[chunkId; `loaded;
                "quality failures: dups=",string[qResult`dups],
                " ordering=",string[qResult`ordering_errors],
                " nulls=",string[qResult`total_nulls]]
        ];
        ::
    ];

    // Feature 1: update symbology map with (sym,instrument_id) pairs from this load
    if[n>0;
        @[updateSymbologyMap; (value schema; partDate);
          {[e] .lg.e[`loader;"symbology update error: ",e]}]
    ];

    // Feature 3: update per-chunk metrics file with load timing
    @[updateMetrics; (chunkId; requestId; loadNs; n);
      {[e] .lg.o[`loader;"metrics update skipped (file not found)"]}];

    .lg.o[`loader;"chunk done: chunk_id=",string[chunkId]," rows=",string n];
    n
 };

// ---------------------------------------------------------------------------
// runLoader — main entry point.
// Scans the manifest directory for pending JSON files and processes each one.
// ---------------------------------------------------------------------------
runLoader:{[]
    manifestDir:`$$[`STAGING_DIR in key .z.e;getenv`STAGING_DIR;"staging"],"/metadata/manifests";

    .lg.o[`loader;"runLoader: manifest dir=",string manifestDir];
    .lg.o[`loader;"runLoader: HDB dir=",string HDB_DIR];

    // Ensure HDB root exists
    @[system;"mkdir -p ",1_string HDB_DIR;::];

    processManifests manifestDir;
    .lg.o[`loader;"runLoader: complete"]
 };
