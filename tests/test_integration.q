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
