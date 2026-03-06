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

TEST_DATE:"D"$ssr[$[`INTEGRATION_TEST_DATE in key .z.e;getenv`INTEGRATION_TEST_DATE;"2024-01-15"];"-";"."];
TEST_SYM:`$$[`INTEGRATION_TEST_SYM in key .z.e;getenv`INTEGRATION_TEST_SYM;"AAPL"];
HDB_PATH:hsym`$$[`KDBHDB in key .z.e;getenv`KDBHDB;"hdb"];

-1 "Integration test: date=",string[TEST_DATE]," sym=",string[TEST_SYM];
-1 "HDB path: ",string HDB_PATH;

// Load the HDB
\l hdb

// ---------------------------------------------------------------------------
// Test 1: HDB partition directory exists for test date
// ---------------------------------------------------------------------------

partDir:` sv HDB_PATH,`$string[TEST_DATE];
assertEq["partition dir exists"; partDir in key HDB_PATH; 1b];

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

// Group by the natural key and check no group has more than 1 row
dupeCount:count select from trades
    where date=TEST_DATE, sym=TEST_SYM,
    {1<count x} fby ([]sym;time;sequence);
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
