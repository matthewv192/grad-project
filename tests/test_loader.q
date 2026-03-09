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
// Test 1: loadTrades writes partition and returns correct row count
// ---------------------------------------------------------------------------

rowCount:loadTrades[hsym`$testCSV; 2024.01.15; `$"XNAS.ITCH"];
assertEq["loadTrades row count"; rowCount; 3j];

// ---------------------------------------------------------------------------
// Test 2: Partition directory was created
// ---------------------------------------------------------------------------

assertEq["partition dir exists"; `2024.01.15 in key HDB_DIR; 1b];

// ---------------------------------------------------------------------------
// Test 3: Idempotency — loading the same file twice should not duplicate rows
// ---------------------------------------------------------------------------

rowCount2:loadTrades[hsym`$testCSV; 2024.01.15; `$"XNAS.ITCH"];
assertEq["idempotent row count"; rowCount2; 3j];

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
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all loader tests passed";
exit 0
