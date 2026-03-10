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
    volume:1000 1100 1200;
    exchange:3#`$"XNAS.ITCH"
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
    volume:2000 2100;
    exchange:2#`$"XNAS.ITCH"
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
    volume:enlist 5000;
    exchange:enlist `$"XNAS.ITCH"
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
result:getAdjustedClose[`AAPL;2024.06.07;2024.06.11;`backward];
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
// Test 10: PIT anti-pattern — wrong (post-split-only) factors give wrong result
//
// Applying only post-split factors (factor=1.0) to pre-split bars results in
// unadjusted prices.  This demonstrates why point-in-time factor selection matters.
// ---------------------------------------------------------------------------

futureFactors:([]
    sym:              2#`AAPL;
    date:             2024.06.10 2024.06.11;
    cumulative_factor:1.0 1.0;
    split_factor:     1.0 1.0;
    dividend_factor:  1.0 1.0 );

// futureFactors has no entry for 2024.06.07..09 → factor defaults to 1.0 → no adjustment
wrongAdj:applyAdj[preSplitBars;futureFactors];
assertClose["wrong factors: close unchanged at 193"; (wrongAdj`close)[0]; 193.0; 0.001];

// Correct backward-adjusted close = 96.5; wrong factors leave it at 193.0 → NOT equal
assertEq["wrong factors != correct adjustment";
    not (wrongAdj`close)[0] ~ (adjusted`close)[0]; 1b];

// ---------------------------------------------------------------------------
// Test 11: PIT correctness of getAdjustedClose — inject allBars into ohlcv_1m
//
// Pre-split bars should show adj_close ≠ raw close (factor=0.5 halves them).
// Post-split bars should have adj_close equal to raw close (factor=1.0).
// ---------------------------------------------------------------------------

`ohlcv_1m set allBars;  // inject 5-row table spanning the 2:1 split

adjResult:getAdjustedClose[`AAPL;2024.06.07;2024.06.11;`backward];
assertEq["PIT result is table";       type adjResult;  98h];
assertEq["PIT result has adj_close";  `adj_close in cols adjResult; 1b];
assertEq["PIT result row count";      count adjResult; 5j];

// Pre-split row 0: backward adj_close = 193.0 * 0.5 = 96.5 ≠ raw 193.0
assertEq["PIT pre-split adj occurred";
    not (adjResult`adj_close)[0] ~ (allBars`close)[0]; 1b];
assertClose["PIT pre-split adj_close=96.5"; (adjResult`adj_close)[0]; 96.5; 0.001];

// Post-split row 3: factor=1.0 → adj_close = raw close = 98.0
assertClose["PIT post-split adj_close=raw"; (adjResult`adj_close)[3]; 98.0; 0.001];

`ohlcv_1m set 0#ohlcv_1m;  // restore empty global

// ---------------------------------------------------------------------------
// Test 12: forward vs backward adjustment methods
//
// Simple scenario: pre-split close=100 (factor=0.5), post-split close=50 (factor=1.0).
//   backward: both bars → adj_close = 50  (all in post-split / current terms)
//   forward:  both bars → adj_close = 100 (all in pre-split / historical terms)
// ---------------------------------------------------------------------------

fwdFactors:([]
    sym:              `AAPL`AAPL;
    date:             2024.06.09 2024.06.10;
    cumulative_factor:0.5 1.0;
    split_factor:     0.5 1.0;
    dividend_factor:  1.0 1.0 );

fwdPre:([]
    date: enlist 2024.06.09; sym: enlist `AAPL;
    time: enlist 2024.06.09D09:30:00.000000000;
    instrument_id: enlist 1001j;
    open: enlist 100.0; high: enlist 102.0; low: enlist 99.0; close: enlist 100.0;
    volume: enlist 1000j; exchange: enlist `$"XNAS.ITCH" );

fwdPost:([]
    date: enlist 2024.06.10; sym: enlist `AAPL;
    time: enlist 2024.06.10D09:30:00.000000000;
    instrument_id: enlist 1001j;
    open: enlist 50.0; high: enlist 51.0; low: enlist 49.0; close: enlist 50.0;
    volume: enlist 2000j; exchange: enlist `$"XNAS.ITCH" );

`ohlcv_1m set fwdPre,fwdPost;
`ref_adj_factors set fwdFactors;

bkwdRes:getAdjustedClose[`AAPL;2024.06.09;2024.06.10;`backward];
fwdRes: getAdjustedClose[`AAPL;2024.06.09;2024.06.10;`forward];

// backward: both bars expressed in post-split terms → adj_close = 50
assertClose["backward pre-split adj_close=50";  (bkwdRes`adj_close)[0]; 50.0; 0.001];
assertClose["backward post-split adj_close=50"; (bkwdRes`adj_close)[1]; 50.0; 0.001];

// forward: both bars expressed in pre-split terms → adj_close = 100
assertClose["forward pre-split adj_close=100";  (fwdRes`adj_close)[0]; 100.0; 0.001];
assertClose["forward post-split adj_close=100"; (fwdRes`adj_close)[1]; 100.0; 0.001];

// Unsupported method should signal an error
badMethodResult:@[{getAdjustedClose[`AAPL;2024.06.09;2024.06.10;x]}; `total; {[e]0b}];
assertEq["unsupported method signals error"; badMethodResult; 0b];

// Restore globals
`ohlcv_1m set 0#ohlcv_1m;
`ref_adj_factors set syntheticFactors;

// ---------------------------------------------------------------------------
// Test 13: resolveSymbol PIT — FB → META ticker change scenario
//
// FB was the ticker for instrument_id=1003 until 2022-10-27.
// META took effect from 2022-10-28 onward (same instrument_id).
// Querying with the correct asOf date gives the correct symbol.
// Querying with the wrong asOf date (the PIT anti-pattern) gives the wrong symbol.
// ---------------------------------------------------------------------------

fbMetaMap:([]
    sym:          `FB`META;
    instrument_id:1003 1003j;
    exchange:     2#`$"XNAS.ITCH";
    valid_from:   2010.01.01 2022.10.28;
    valid_to:     2022.10.27 9999.12.31 );

`ref_symbology_map set fbMetaMap;

// Correct asOf: before the name change → FB
assertEq["resolve FB before change";
    resolveSymbol[1003j;`$"XNAS.ITCH";2022.10.26]; `FB];

// Correct asOf: on and after the name change → META
assertEq["resolve META on change day";
    resolveSymbol[1003j;`$"XNAS.ITCH";2022.10.28]; `META];
assertEq["resolve META after change";
    resolveSymbol[1003j;`$"XNAS.ITCH";2022.11.01]; `META];

// PIT anti-pattern: same instrument_id, different asOf → different symbols
// Using wrong asOf (post-change date) while processing pre-change data gives META, not FB
assertEq["PIT anti-pattern: wrong asOf gives wrong sym";
    not (resolveSymbol[1003j;`$"XNAS.ITCH";2022.10.30]) ~
        (resolveSymbol[1003j;`$"XNAS.ITCH";2022.10.26]);
    1b];

// Reverse lookup: resolveInstrumentId is also PIT-aware
assertEq["resolve instId for FB";
    resolveInstrumentId[`FB;`$"XNAS.ITCH";2022.10.26]; 1003j];
assertEq["resolve instId for META";
    resolveInstrumentId[`META;`$"XNAS.ITCH";2022.10.28]; 1003j];
assertEq["META instId pre-change is null";
    resolveInstrumentId[`META;`$"XNAS.ITCH";2022.10.26]; 0Nj];

`ref_symbology_map set 0#ref_symbology_map;  // restore empty global

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all adj tests passed";
exit 0
