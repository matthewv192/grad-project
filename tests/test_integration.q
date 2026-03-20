// test_integration.q — integration test: verify HDB output after a real backfill.
//
// INTEGRATION_TEST_DATE and INTEGRATION_TEST_SYM are optional.
// When omitted the test auto-discovers the most recent partition date and
// the first available sym so it works against any populated HDB without
// any configuration.
//
// Skips gracefully when the HDB is empty or has no trades data.
//
// Required:
//   KDBHDB — path to HDB root (falls back to ./hdb if not set)
// Optional:
//   INTEGRATION_TEST_DATE — e.g. "2024-01-17"   (default: most recent partition)
//   INTEGRATION_TEST_SYM  — e.g. "AAPL"         (default: first sym with data)

\l schema/schema.q
\l code/adjlib/adjlib.q

failures:0N!0#`;

assertEq:{[testName;actual;expected]
    if[not actual~expected;
        `failures set failures,enlist`$testName,
            ": expected ",(raze -3!expected),
            " got ",(raze -3!actual)
    ]
 };

assertGt:{[testName;actual;threshold]
    if[not actual>threshold;
        `failures set failures,enlist`$testName,
            ": expected > ",string[threshold]," got ",string actual
    ]
 };

// ---------------------------------------------------------------------------
// Setup: load HDB, then resolve date and sym
// ---------------------------------------------------------------------------

HDB_PATH:hsym`$$[count s:getenv`KDBHDB;s;(getenv`PWD),"/hdb"];
-1 "HDB path: ",string HDB_PATH;

// Load the HDB using the resolved path (not a hardcoded relative `\l hdb`)
system "l ",1_string HDB_PATH;

// Guard: skip if the HDB has no partition dates at all
if[0=count date; -1 "SKIP: HDB has no partitions — run a backfill first"; exit 0];

// Resolve TEST_DATE: env var wins; otherwise use the most recent partition.
TEST_DATE:$[count s:getenv`INTEGRATION_TEST_DATE;
    "D"$ssr[s;"-";"."];
    last date];

// Resolve TEST_SYM: env var wins; otherwise pick the first sym (alphabetically)
// with trades data on TEST_DATE.  Convert to plain symbol so HDB enum comparisons work.
testSyms:distinct `$string (select sym from trades where date=TEST_DATE)`sym;
TEST_SYM:$[count s:getenv`INTEGRATION_TEST_SYM;
    `$s;
    $[count testSyms; first asc testSyms; `]];

// Guard: skip if no trades data found for the resolved date
skipMsg:"SKIP: no trades data for date=",string[TEST_DATE]," — set INTEGRATION_TEST_DATE or load more data first";
if[null TEST_SYM; -1 skipMsg; exit 0];

-1 "Integration test: date=",string[TEST_DATE]," sym=",string[TEST_SYM];

// ---------------------------------------------------------------------------
// Test 1: HDB partition directory exists for test date
// ---------------------------------------------------------------------------

partDir:` sv HDB_PATH,`$string[TEST_DATE];
// key HDB_PATH returns plain sym names (e.g. `2024.01.17), not full paths
assertEq["partition dir exists"; (`$string TEST_DATE) in key HDB_PATH; 1b];

// ---------------------------------------------------------------------------
// Test 2: trades table exists in partition
// ---------------------------------------------------------------------------

assertEq["trades dir exists"; `trades in key partDir; 1b];

// ---------------------------------------------------------------------------
// Test 3: Row count > 0 for test sym on test date
// ---------------------------------------------------------------------------

rowCount:count select from trades where date=TEST_DATE, sym=TEST_SYM;
assertGt["row count > 0"; rowCount; 0j];

// ---------------------------------------------------------------------------
// Test 4: Data is sorted by sym, then time within the partition
// ---------------------------------------------------------------------------

t:select from trades where date=TEST_DATE, sym=TEST_SYM;
assertEq["sorted by sym time"; t~`sym`time xasc t; 1b];

// ---------------------------------------------------------------------------
// Test 5: No duplicate (sym; time; exchange; sequence) keys
// ---------------------------------------------------------------------------

// Compare total rows vs distinct (sym,time,exchange,sequence) rows.
// Any difference means duplicate natural keys exist.
natkeys:select sym,time,exchange,sequence from trades where date=TEST_DATE, sym=TEST_SYM;
dupeCount:(count natkeys)-(count distinct natkeys);
assertEq["no duplicate natural keys"; dupeCount; 0j];

// ---------------------------------------------------------------------------
// Test 6: price and size are positive
// ---------------------------------------------------------------------------

badPrice:count select from trades
    where date=TEST_DATE, sym=TEST_SYM, price<=0;
assertEq["prices positive"; badPrice; 0j];

badSize:count select from trades
    where date=TEST_DATE, sym=TEST_SYM, size<=0;
assertEq["sizes positive"; badSize; 0j];

// ---------------------------------------------------------------------------
// Test 7: ohlcv_1m partition exists
// ---------------------------------------------------------------------------

assertEq["ohlcv_1m dir exists"; `ohlcv_1m in key partDir; 1b];

// ---------------------------------------------------------------------------
// Adj tests (Tests 8-10): getAdjustedClose on real HDB data.
//
// Skipped gracefully when:
//   - No ohlcv_1m rows for (TEST_SYM, TEST_DATE) in the HDB
//   - ref_adj_factors is empty after attempting to load from CSV
// ---------------------------------------------------------------------------

adjFactorCount:@[loadRefAdjFactors; ::; {[e] -1 "adj factors load error: ",e; 0}];
ohlcvCount:count select from ohlcv_1m where date=TEST_DATE, sym=TEST_SYM;

skipAdj:0b;
if[ohlcvCount=0; skipAdj:1b; -1 "SKIP adj: no ohlcv_1m rows for ",string[TEST_SYM]," on ",string TEST_DATE];
if[adjFactorCount=0; skipAdj:1b; -1 "SKIP adj: no adj factors loaded — run ref_ingest.py --symbols ",string[TEST_SYM]," first"];

if[not skipAdj;

    // Test 8: getAdjustedClose returns correct schema with adj_close column
    adjResult:getAdjustedClose[TEST_SYM;TEST_DATE;TEST_DATE;`backward;0Np];
    assertEq["adj: result is table";             type adjResult;                  98h];
    assertEq["adj: has adj_close col";           `adj_close in cols adjResult;    1b];
    assertEq["adj: row count matches ohlcv_1m";  count adjResult;                 ohlcvCount];

    // Test 9: adj_close is positive and non-null for all bars
    badAdj:count select from adjResult where (null adj_close) or adj_close<=0;
    assertEq["adj: adj_close positive non-null"; badAdj; 0j];

    // Test 10: asOf before any data could have been loaded → factors excluded
    // → adj_close defaults to raw close (1.0 fill) via no-factor path
    earlyAsOf:1900.01.01D00:00:00.000000000;
    noFctResult:getAdjustedClose[TEST_SYM;TEST_DATE;TEST_DATE;`backward;earlyAsOf];
    assertEq["adj: asOf=past leaves adj_close=close";
        (noFctResult`adj_close) ~ (noFctResult`close); 1b];

 ];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1
 ];
-1 "PASS: all integration tests passed (",string[rowCount]," rows for ",
   string[TEST_SYM]," on ",string[TEST_DATE],")";
exit 0
