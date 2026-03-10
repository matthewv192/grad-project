// test_integration.q — integration test: verify HDB output after a real backfill.
//
// Expects environment variables set by run_integration.sh:
//   INTEGRATION_TEST_DATE  — e.g. "2024-01-15"
//   INTEGRATION_TEST_SYM   — e.g. "AAPL"
//   KDBHDB                 — path to HDB root

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
// Setup
// ---------------------------------------------------------------------------

TEST_DATE:"D"$ssr[$[count s:getenv`INTEGRATION_TEST_DATE;s;"2024-01-15"];"-";"."];
TEST_SYM:`$$[count s:getenv`INTEGRATION_TEST_SYM;s;"AAPL"];
HDB_PATH:hsym`$$[count s:getenv`KDBHDB;s;"hdb"];

-1 "Integration test: date=",string[TEST_DATE]," sym=",string[TEST_SYM];
-1 "HDB path: ",string HDB_PATH;

// Load the HDB
\l hdb

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
// Test 5: No duplicate (sym; time; sequence) keys
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

$[count failures;
    (-1 "FAIL: ",string[count failures]," test(s) failed:"; -1 each string failures; exit 1);
    0N
 ];
-1 "PASS: all integration tests passed (",string[rowCount]," rows for ",
   string[TEST_SYM]," on ",string[TEST_DATE],")";
exit 0
