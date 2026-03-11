// test_loader.q — unit tests: loader reads CSV, writes HDB, and is idempotent.
//
// Run from PACKAGEHOME:
//   q tests/test_loader.q
// Exits 0 on pass, 1 on any failure.
//
// Uses a temp HDB under /tmp so the real hdb/ directory is untouched.

\l schema/schema.q

failures:();

assertEq:{[testName;actual;expected]
    if[not actual~expected;
        msg:testName,": expected ",(raze -3!expected)," got ",(raze -3!actual);
        `failures set failures,enlist`$msg
    ]
 };

assertGt:{[testName;actual;threshold]
    if[not actual>threshold;
        `failures set failures,enlist`$(testName,": expected > ",string[threshold]," got ",string actual)
    ]
 };

// ---------------------------------------------------------------------------
// Override HDB_DIR before loading loader.q so .Q.dpft writes to /tmp.
// The timestamp makes collisions across test runs impossible.
// ---------------------------------------------------------------------------

HDB_DIR:hsym`$"/tmp/grad_loader_test_",string`long$.z.p;
// Set KDBHDB in .z.e so loader.q picks it up when it re-evaluates the env var
setenv[`KDBHDB;1_string HDB_DIR];

// ---------------------------------------------------------------------------
// Write a Databento-format trades CSV (14 columns, same header as real data).
// Columns: ts_recv, ts_event, rtype, publisher_id, instrument_id,
//          action, side, depth, price, size, flags, ts_in_delta, sequence, symbol
// ---------------------------------------------------------------------------

testCSV:"/tmp/grad_test_trades.csv";
csvLines:(
    "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,symbol";
    "2024-01-15T09:30:00.000000100,2024-01-15T09:30:00.000000000,80,1,1001,T,A,0,185.50,100,128,1000,1,AAPL";
    "2024-01-15T09:30:01.000000100,2024-01-15T09:30:01.000000000,80,1,1001,T,B,0,185.55,200,128,1000,2,AAPL";
    "2024-01-15T09:30:00.500000100,2024-01-15T09:30:00.500000000,80,1,1002,T,A,0,374.20,50,128,1000,3,MSFT"
 );
(hsym`$testCSV) 0: csvLines;

// ---------------------------------------------------------------------------
// Load loader.q — HDB_DIR is already set so loader.q gets the right path
// ---------------------------------------------------------------------------

\l code/backfill/loader.q

// ---------------------------------------------------------------------------
// Helper: build a single-row manifest table for loadChunkBatch calls.
// loadTrades/loadOhlcv were deprecated in favour of loadChunkBatch which
// takes a table of manifests matching the JSON schema produced by Python.
// ---------------------------------------------------------------------------
mkM:{[cid;sch;dt;exch;fp;rc]
    ([] chunk_id:enlist cid; request_id:enlist`r_test;
        schema:enlist sch; date:enlist dt; exchange:enlist exch;
        file_path:enlist hsym`$fp; row_count:enlist rc)};

// ---------------------------------------------------------------------------
// Test 1: loadChunkBatch writes partition and returns correct row count
// ---------------------------------------------------------------------------

rowCount:loadChunkBatch mkM[`c001;`trades;2024.01.15;`$"XNAS.ITCH";testCSV;3j];
assertEq["loadChunkBatch row count"; rowCount; 3j];

// ---------------------------------------------------------------------------
// Test 2: Partition directory was created
// ---------------------------------------------------------------------------

assertEq["partition dir exists"; `2024.01.15 in key HDB_DIR; 1b];

// ---------------------------------------------------------------------------
// Test 3: Idempotency — loading the same file twice should not duplicate rows
// ---------------------------------------------------------------------------

rowCount2:loadChunkBatch mkM[`c001;`trades;2024.01.15;`$"XNAS.ITCH";testCSV;3j];
assertEq["idempotent row count"; rowCount2; 0j];

// Load the HDB and verify row count is still 3 (not 6)
// system "l path" loads an HDB directory into the q session
system "l ",1_string HDB_DIR;  // 1_ strips the leading `:` from the file handle
actualRows:count select from trades where date=2024.01.15;
assertEq["no duplicate rows"; actualRows; 3j];

// ---------------------------------------------------------------------------
// Test 4: Data sorted by sym then time
// ---------------------------------------------------------------------------

t:select from trades where date=2024.01.15;
assertEq["sorted by sym time"; t~`sym`time xasc t; 1b];

// ---------------------------------------------------------------------------
// Test 5: Column types from a partitioned HDB query
// In the HDB, columns are returned as list types (positive type numbers).
// sym is type 20h (enumerated symbol — linked to the top-level sym file).
// ---------------------------------------------------------------------------

assertEq["trades date type";  type t`date;  14h];   // date list
assertEq["trades sym type";   type t`sym;   20h];   // enumerated sym
assertEq["trades time type";  type t`time;  12h];   // timestamp list
assertEq["trades price type"; type t`price; 9h];    // float list
assertEq["trades size type";  type t`size;  7h];    // long list

// ---------------------------------------------------------------------------
// Setup for tests 6-11: additional CSV files
// ---------------------------------------------------------------------------

// Databento ohlcv-1m CSV (10 columns, 3 rows across 2 syms, date 2024.01.16)
testOhlcvCSV:"/tmp/grad_test_ohlcv.csv";
(hsym`$testOhlcvCSV) 0: (
    "ts_event,rtype,publisher_id,instrument_id,open,high,low,close,volume,symbol";
    "2024-01-16T09:30:00.000000000,33,1,1001,185.50,186.00,185.00,185.75,10000,AAPL";
    "2024-01-16T09:31:00.000000000,33,1,1001,185.75,186.25,185.50,186.00,8000,AAPL";
    "2024-01-16T09:30:00.000000000,33,1,1002,374.20,375.00,373.50,374.75,5000,MSFT" );

// Header-only CSVs (no data rows)
emptyTradesCSV:"/tmp/grad_test_empty_trades.csv";
(hsym`$emptyTradesCSV) 0: enlist
    "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,symbol";

emptyOhlcvCSV:"/tmp/grad_test_empty_ohlcv.csv";
(hsym`$emptyOhlcvCSV) 0: enlist
    "ts_event,rtype,publisher_id,instrument_id,open,high,low,close,volume,symbol";

// XNYS.PILLAR trades CSV for 2024.01.15 (same date as XNAS data loaded in Tests 1-3)
xnysTradesCSV:"/tmp/grad_test_xnys_trades.csv";
(hsym`$xnysTradesCSV) 0: (
    "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,symbol";
    "2024-01-15T09:30:00.000000100,2024-01-15T09:30:00.000000000,80,2,2001,T,A,0,185.45,300,128,1000,1,AAPL" );

// ---------------------------------------------------------------------------
// Test 6: loadTrades on empty CSV returns 0 without error
// ---------------------------------------------------------------------------

emptyTradesCount:loadChunkBatch mkM[`c_emp_t;`trades;2024.01.20;`$"XNAS.ITCH";emptyTradesCSV;0j];
assertEq["empty trades CSV returns 0"; emptyTradesCount; 0j];

// ---------------------------------------------------------------------------
// Test 7: loadOhlcv on empty CSV returns 0 without error
// ---------------------------------------------------------------------------

emptyOhlcvCount:loadChunkBatch mkM[`c_emp_o;`ohlcv_1m;2024.01.20;`$"XNAS.ITCH";emptyOhlcvCSV;0j];
assertEq["empty ohlcv CSV returns 0"; emptyOhlcvCount; 0j];

// ---------------------------------------------------------------------------
// Test 8: readOhlcvCSV parses all column types correctly
// ---------------------------------------------------------------------------

rawOhlcv:readOhlcvCSV hsym`$testOhlcvCSV;
assertEq["readOhlcvCSV row count";          count rawOhlcv;         3j];
assertEq["readOhlcvCSV sym type";           type rawOhlcv`sym;      11h];
assertEq["readOhlcvCSV time type";          type rawOhlcv`time;     12h];
assertEq["readOhlcvCSV open type";          type rawOhlcv`open;      9h];
assertEq["readOhlcvCSV close type";         type rawOhlcv`close;     9h];
assertEq["readOhlcvCSV volume type";        type rawOhlcv`volume;    7h];
assertEq["readOhlcvCSV instrument_id type"; type rawOhlcv`instrument_id; 7h];

// ---------------------------------------------------------------------------
// Test 9: loadOhlcv writes partition and returns correct row count
// ---------------------------------------------------------------------------

ohlcvCount:loadChunkBatch mkM[`c_ohlcv;`ohlcv_1m;2024.01.16;`$"XNAS.ITCH";testOhlcvCSV;3j];
assertEq["loadOhlcv row count";        ohlcvCount;                       3j];
assertEq["ohlcv partition dir exists"; `2024.01.16 in key HDB_DIR;       1b];
assertEq["ohlcv table dir exists";     `ohlcv_1m in key ` sv HDB_DIR,`2024.01.16; 1b];

// ---------------------------------------------------------------------------
// Test 10: loadOhlcv is idempotent — loading same file twice keeps row count at 3
// ---------------------------------------------------------------------------

ohlcvCount2:loadChunkBatch mkM[`c_ohlcv;`ohlcv_1m;2024.01.16;`$"XNAS.ITCH";testOhlcvCSV;3j];
assertEq["loadOhlcv idempotent row count"; ohlcvCount2; 0j];

// ---------------------------------------------------------------------------
// Test 11: Multi-exchange merge — XNAS.ITCH and XNYS.PILLAR in same partition date
//
// Tests 1-3 already loaded 3 XNAS.ITCH rows for 2024.01.15.
// We now load 1 XNYS.PILLAR row for the same date and verify the partition
// contains rows from both exchanges without dropping or duplicating anything.
//
// IMPORTANT: loadTrades must be called BEFORE any `system "l hdb"` in this
// session.  Once an HDB is loaded, xasc routes through .Q.xasc which treats
// any table with a `date` column as partitioned, causing 'dup date.
// ---------------------------------------------------------------------------

xnysCount:loadChunkBatch mkM[`c_xnys;`trades;2024.01.15;`$"XNYS.PILLAR";xnysTradesCSV;1j];
assertEq["XNYS.PILLAR load row count"; xnysCount; 1j];

// Single HDB reload covers both Test 10 (ohlcv idempotency) and Test 11 (merge)
system "l ",1_string HDB_DIR;
assertEq["no duplicate ohlcv rows"; count select from ohlcv_1m where date=2024.01.16; 3j];

mergedRaw:select from trades where date=2024.01.15;
// unenumAll resolves all type-20h enumerated columns to plain symbols
mergedTrades:unenumAll mergedRaw;
exchangeSyms:distinct mergedTrades`exchange;
assertEq["XNAS.ITCH in merged partition";  (`$"XNAS.ITCH")  in exchangeSyms; 1b];
assertEq["XNYS.PILLAR in merged partition"; (`$"XNYS.PILLAR") in exchangeSyms; 1b];
assertEq["merged partition total rows";    count mergedTrades;              4j];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all loader tests passed";
exit 0
