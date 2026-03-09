// quality.q — data quality checks run after each partition load.
//
// Functions:
//   checkDuplicateKeys[t]       — count (sym,time) duplicate rows
//   checkTimeOrdering[t]        — 0j if sorted by sym,time; else count t
//   checkNulls[t;keyCols]       — dict: column → null count
//   runQualityChecks[t;tn;dt]   — run all checks; return result dict
//   checkQualityFromHDB[tn;dt]  — load from partitioned HDB then run checks
//
// loader.q calls: runQualityChecks[value schema; schema; partDate]
// check_quality.sh calls: checkQualityScript[] (reads QUALITY_TABLE/QUALITY_DATE env)
//
// Result dict keys:
//   dups            — count of duplicate (sym,time) rows
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
// checkDuplicateKeys — count rows with duplicate (sym, time) pairs
// ---------------------------------------------------------------------------
checkDuplicateKeys:{[t]
    `long$count[t] - count distinct `sym`time#t
 };

// ---------------------------------------------------------------------------
// checkTimeOrdering — 0j if perfectly sorted by sym,time; else count t
// ---------------------------------------------------------------------------
checkTimeOrdering:{[t]
    $[t~`sym`time xasc t; 0j; `long$count t]
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
    hdbDir:hsym`$$[`KDBHDB in key .z.e;getenv`KDBHDB;"hdb"];
    system "l ",1_string hdbDir;

    tname:`$$[`QUALITY_TABLE in key .z.e;getenv`QUALITY_TABLE;"trades"];
    dtStr:$[`QUALITY_DATE in key .z.e;getenv`QUALITY_DATE;"2024-01-15"];
    dt:"D"$ssr[dtStr;"-";"."];

    .lg.o[`quality;"running quality checks: table=",string[tname]," date=",string dt];

    r:checkQualityFromHDB[tname;dt];

    .lg.o[`quality;"result: ",(.j.j r)];

    $[r[`failed]>0j; exit 1; exit 0]
 };
