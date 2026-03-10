// quality.q — data quality checks run after each partition load.
//
// Functions:
//   checkDuplicateKeys[t]       — count rows with duplicate natural keys
//                                 (sym,time,exchange,sequence for trades;
//                                  sym,time,exchange for ohlcv_1m)
//   checkTimeOrdering[t]        — 0j if sorted by sym,time; else count t
//   checkNulls[t;keyCols]       — dict: column → null count
//   runQualityChecks[t;tn;dt]   — run all checks; return result dict
//   checkQualityFromHDB[tn;dt]  — load from partitioned HDB then run checks
//
// loader.q calls: runQualityChecks[value schema; schema; partDate]
// check_quality.sh calls: checkQualityScript[] (reads QUALITY_TABLE/QUALITY_DATE env)
//
// Result dict keys:
//   dups            — count of rows with duplicate natural keys
//   ordering_errors — 0j if sorted; count t if not
//   nulls           — dict col→null count for key columns
//   total_nulls     — sum of nulls dict
//   passed          — number of checks that passed (max 3)
//   failed          — number of checks that failed
//   checks          — total checks run (3)

\l schema/schema.q

if[not `lg in key `.;
    .lg.o:{[proc;msg] -1 (string .z.p)," [",string[proc],"] ",msg;};
    .lg.e:{[proc;msg] -1 (string .z.p)," [ERROR][",string[proc],"] ",msg;}
 ];

// ---------------------------------------------------------------------------
// checkDuplicateKeys — count rows with duplicate natural keys.
//
// Key columns used (intersected with actual table columns so the function
// works on both trades and ohlcv_1m):
//   sym      — ticker
//   time     — nanosecond event timestamp
//   exchange — exchange source (multi-exchange partitions have same sym+time
//              from different exchanges — not a duplicate)
//   sequence — exchange sequence number; tick data legitimately has multiple
//              trades at the same nanosecond for the same sym+exchange, so
//              sequence is required to distinguish them.  ohlcv_1m has no
//              sequence column so falls back to sym+time+exchange (one bar
//              per sym per minute per exchange is the correct invariant).
// ---------------------------------------------------------------------------
checkDuplicateKeys:{[t]
    keyCols:`sym`time`exchange`sequence inter cols t;
    `long$count[t] - count distinct keyCols#t
 };

// ---------------------------------------------------------------------------
// checkTimeOrdering — 0j if perfectly sorted by sym,time; else count t.
// Sorts only the two key columns rather than the full table, avoiding the
// cost of materialising a complete sorted copy for wide tables.
// ---------------------------------------------------------------------------
checkTimeOrdering:{[t]
    keyCols:`sym`time#t;
    $[keyCols~`sym`time xasc keyCols; 0j; `long$count t]
 };

// ---------------------------------------------------------------------------
// checkNulls — dict: column → count of null values, for given key columns
// ---------------------------------------------------------------------------
checkNulls:{[t;keyCols]
    keyCols!{[t;c] `long$sum null t[c]}[t;] each keyCols
 };

// ---------------------------------------------------------------------------
// runQualityChecks — run all checks on an in-memory table slice.
//
//   t         : table value (result of readTradesCSV, etc.)
//   tableName : `trades | `ohlcv_1m  (for logging and column selection)
//   partDate  : date (for logging)
//
// Returns: `dups`ordering_errors`nulls`total_nulls`passed`failed`checks
// ---------------------------------------------------------------------------
runQualityChecks:{[t;tableName;partDate]
    if[0=count t;
        .lg.o[`quality;"no data to check for ",string[tableName]," on ",string partDate];
        :`dups`ordering_errors`nulls`total_nulls`passed`failed`checks!
          (0j;0j;(`symbol$())!`long$();0j;`skipped;0j;0j)
    ];

    dups:checkDuplicateKeys t;

    ord:checkTimeOrdering t;

    // Key columns for null check differ by schema
    keyCols:$[tableName=`trades;
        `sym`time`price`size;
        tableName=`ohlcv_1m;
        `sym`time`open`high`low`close;
        `sym`time
    ];
    nullCounts:checkNulls[t;keyCols];
    totalNulls:sum nullCounts;

    numChecks:3j;
    failed:`long$(dups>0j)+(ord>0j)+(totalNulls>0j);
    passed:numChecks-failed;

    result:(`dups`ordering_errors`nulls`total_nulls`passed`failed`checks)!
           (dups; ord; nullCounts; totalNulls; passed; failed; numChecks);

    .lg.o[`quality;
        "quality ",string[tableName]," date=",string[partDate],
        " rows=",string[count t],
        " dups=",string[dups],
        " ordering_errors=",string[ord],
        " null_cells=",string[totalNulls],
        " passed=",string[passed],"/",string numChecks];

    if[failed>0j;
        .lg.e[`quality;
            "QUALITY FAILURES ",string[tableName]," date=",string[partDate],
            ": dups=",string[dups],
            " ordering_errors=",string[ord],
            " null_cells=",string[totalNulls]]
    ];

    result
 };

// ---------------------------------------------------------------------------
// checkQualityFromHDB — load a date slice from a partitioned HDB table,
//                       then run quality checks.
//
// Used by check_quality.sh when querying an already-loaded HDB.
// ---------------------------------------------------------------------------
checkQualityFromHDB:{[tableName;partDate]
    t:?[tableName;enlist(=;`date;partDate);0b;()];
    runQualityChecks[t;tableName;partDate]
 };

// ---------------------------------------------------------------------------
// checkQualityScript — entry point for check_quality.sh.
// Reads QUALITY_TABLE and QUALITY_DATE from environment, loads the HDB,
// runs checks, exits 0 on pass / 1 on failure.
// ---------------------------------------------------------------------------
checkQualityScript:{[]
    hdbDir:hsym`$$[count s:getenv`KDBHDB;s;"hdb"];
    hdbPath:1_string hdbDir;

    // Verify HDB directory exists and has at least one date partition before loading.
    // Give a clear diagnostic rather than a cryptic q error if the loader hasn't run.
    loadErr:@[system;"l ",hdbPath;{[e]e}];
    if[count loadErr;
        .lg.e[`quality;"Cannot load HDB from: ",hdbPath,
              " — run the backfill loader first (error: ",loadErr,")"];
        exit 1
    ];

    tname:`$$[count s:getenv`QUALITY_TABLE;s;"trades"];
    dtStr:$[count s:getenv`QUALITY_DATE;s;"2024-01-15"];
    dt:"D"$ssr[dtStr;"-";"."];

    // Check the requested partition date exists in the HDB
    if[not dt in date;
        .lg.e[`quality;"Partition date ",string[dt]," not found in HDB at: ",hdbPath,
              " — available dates: ",","sv string asc distinct date];
        exit 1
    ];

    .lg.o[`quality;"running quality checks: table=",string[tname]," date=",string dt];

    r:checkQualityFromHDB[tname;dt];

    .lg.o[`quality;"result: ",(.j.j r)];

    $[r[`failed]>0j; exit 1; exit 0]
 };
