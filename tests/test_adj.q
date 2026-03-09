// test_adj.q — unit tests for the adjustment library (adjlib.q).
//
// Run from PACKAGEHOME:
//   q tests/test_adj.q
// Exits 0 on pass, 1 on any failure.
//
// Key scenario: AAPL has a 2:1 stock split on 2024-06-10.
//   cumulative_factor = 0.5 on pre-split dates (prices halved, volume doubled)
//   cumulative_factor = 1.0 on post-split dates (no change)

\l schema/schema.q

// Point ref_tables.q at a temp dir — we inject synthetic factors directly
// into ref_adj_factors rather than loading from CSV.
system "mkdir -p /tmp/grad_adj_test/reference";
setenv[`STAGING_DIR;"/tmp/grad_adj_test"];

\l code/adjlib/adjlib.q
REF_DIR:hsym`$"/tmp/grad_adj_test/reference";

failures:();

assertEq:{[testName;actual;expected]
    if[not actual~expected;
        msg:testName,": expected ",(raze -3!expected)," got ",(raze -3!actual);
        `failures set failures,enlist`$msg
    ]
 };

assertClose:{[testName;actual;expected;tol]
    if[tol < abs actual-expected;
        msg:testName,": expected ~",string[expected]," got ",string actual;
        `failures set failures,enlist`$msg
    ]
 };

// ---------------------------------------------------------------------------
// Synthetic data
// ---------------------------------------------------------------------------

// Pre-split AAPL bars (2024-06-07 to 2024-06-09) — factor = 0.5
preSplitBars:([]
    date:  2024.06.07 2024.06.08 2024.06.09;
    sym:   `AAPL`AAPL`AAPL;
    time:  3#2024.06.07D09:30:00.000000000;
    instrument_id: 3#1001;
    open:  190.0 191.0 192.0;
    high:  195.0 196.0 197.0;
    low:   189.0 190.0 191.0;
    close: 193.0 194.0 195.0;
    volume:1000 1100 1200
 );

// Post-split AAPL bars (2024-06-10, 2024-06-11) — factor = 1.0
postSplitBars:([]
    date:  2024.06.10 2024.06.11;
    sym:   `AAPL`AAPL;
    time:  2#2024.06.10D09:30:00.000000000;
    instrument_id: 2#1001;
    open:  97.5 98.0;
    high:  99.0 99.5;
    low:   96.5 97.0;
    close: 98.0 98.5;
    volume:2000 2100
 );

// Factors covering the full test window
syntheticFactors:([]
    sym:              `AAPL`AAPL`AAPL`AAPL`AAPL;
    date:             2024.06.07 2024.06.08 2024.06.09 2024.06.10 2024.06.11;
    cumulative_factor:0.5 0.5 0.5 1.0 1.0;
    split_factor:     0.5 0.5 0.5 1.0 1.0;
    dividend_factor:  1.0 1.0 1.0 1.0 1.0
 );

// Inject synthetic factors into the in-memory ref table so getAdjustedClose
// can find them without needing a CSV file or a loaded HDB.
`ref_adj_factors set syntheticFactors;

// ---------------------------------------------------------------------------
// Test 1: applyAdj schema
// ---------------------------------------------------------------------------

adjusted:applyAdj[preSplitBars;syntheticFactors];

assertEq["applyAdj returns table";       type adjusted;  98h];
assertEq["applyAdj returns ohlcv_1m cols"; cols adjusted; cols ohlcv_1m];

// ---------------------------------------------------------------------------
// Test 2: pre-split price adjustment (factor = 0.5)
// ---------------------------------------------------------------------------

assertClose["pre-split open  adjusted"; (adjusted`open) [0]; 95.0;  0.001];
assertClose["pre-split high  adjusted"; (adjusted`high) [0]; 97.5;  0.001];
assertClose["pre-split low   adjusted"; (adjusted`low)  [0]; 94.5;  0.001];
assertClose["pre-split close adjusted"; (adjusted`close)[0]; 96.5;  0.001];

// ---------------------------------------------------------------------------
// Test 3: pre-split volume adjustment (volume / factor = volume * 2)
// ---------------------------------------------------------------------------

assertEq["pre-split volume adjusted"; (adjusted`volume)[0]; 2000j];
assertEq["pre-split volume row 2";    (adjusted`volume)[1]; 2200j];
assertEq["pre-split volume row 3";    (adjusted`volume)[2]; 2400j];

// ---------------------------------------------------------------------------
// Test 4: post-split bars pass through unchanged (factor = 1.0)
// ---------------------------------------------------------------------------

adjPost:applyAdj[postSplitBars;syntheticFactors];

assertClose["post-split close unchanged"; (adjPost`close)[0]; 98.0; 0.001];
assertEq[   "post-split volume unchanged"; (adjPost`volume)[0]; 2000j];

// ---------------------------------------------------------------------------
// Test 5: missing factor defaults to 1.0 (no adjustment)
// ---------------------------------------------------------------------------

// Bar for a date with no entry in factors — should pass through unchanged
barNoFactor:([]
    date:  enlist 2024.07.01;
    sym:   enlist `AAPL;
    time:  enlist 2024.07.01D09:30:00.000000000;
    instrument_id: enlist 1001;
    open:  enlist 150.0;
    high:  enlist 152.0;
    low:   enlist 149.0;
    close: enlist 151.0;
    volume:enlist 5000
 );

adjNoFactor:applyAdj[barNoFactor;syntheticFactors];
assertClose["no-factor close unchanged"; (adjNoFactor`close)[0]; 151.0; 0.001];
assertEq[   "no-factor volume unchanged"; (adjNoFactor`volume)[0]; 5000j];

// ---------------------------------------------------------------------------
// Test 6: applyAdj on empty table returns empty ohlcv_1m-shaped table
// ---------------------------------------------------------------------------

emptyAdj:applyAdj[0#ohlcv_1m;syntheticFactors];
assertEq["empty applyAdj schema"; cols emptyAdj; cols ohlcv_1m];
assertEq["empty applyAdj rows";   count emptyAdj; 0j];

// ---------------------------------------------------------------------------
// Test 7: getAdjustedClose returns correct schema
// ---------------------------------------------------------------------------

// ohlcv_1m is empty (no HDB loaded) — result will be empty but schema correct
result:getAdjustedClose[`AAPL;2024.06.07;2024.06.11;`split];
assertEq["getAdjustedClose returns table";   type result; 98h];
assertEq["getAdjustedClose has adj_close";   `adj_close in cols result; 1b];

// ---------------------------------------------------------------------------
// Test 8: getUnadjusted returns correct schema for both table types
// ---------------------------------------------------------------------------

rawTrades:getUnadjusted[`trades;`AAPL;2024.01.01;2024.12.31];
assertEq["getUnadjusted trades schema"; cols rawTrades; cols trades];

rawOhlcv:getUnadjusted[`ohlcv_1m;`AAPL;2024.01.01;2024.12.31];
assertEq["getUnadjusted ohlcv_1m schema"; cols rawOhlcv; cols ohlcv_1m];

// ---------------------------------------------------------------------------
// Test 9: combined pre+post split bars adjusted correctly as one call
// ---------------------------------------------------------------------------

allBars:preSplitBars,postSplitBars;    // 5-row table spanning the split
adjAll:applyAdj[allBars;syntheticFactors];

// Pre-split rows (0-2): factor=0.5
assertClose["combined pre-split close[0]";  (adjAll`close)[0]; 96.5; 0.001];
assertClose["combined pre-split close[2]";  (adjAll`close)[2]; 97.5; 0.001];
// Post-split rows (3-4): factor=1.0 — close unchanged
assertClose["combined post-split close[3]"; (adjAll`close)[3]; 98.0; 0.001];
assertClose["combined post-split close[4]"; (adjAll`close)[4]; 98.5; 0.001];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all adj tests passed";
exit 0
