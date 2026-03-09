// test_quality.q — unit tests for code/backfill/quality.q
//
// Run from PACKAGEHOME:
//   q tests/test_quality.q
// Exits 0 on pass, 1 on any failure.

\l schema/schema.q
\l code/backfill/quality.q

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
// Build test tables
// ---------------------------------------------------------------------------

// Clean table: 4 rows, sorted, no duplicates, no nulls
cleanTrades:([]
    date:4#2024.01.15;
    sym:`AAPL`AAPL`MSFT`MSFT;
    time:2024.01.15D09:30:00.000000000 2024.01.15D09:31:00.000000000
         2024.01.15D09:30:00.000000000 2024.01.15D09:31:00.000000000;
    instrument_id:1001 1001 1002 1002j;
    price:185.5 185.6 374.2 374.3f;
    size:100 200 50 75j;
    side:`A`B`A`B;
    conditions:`128`128`128`128;
    sequence:1 2 3 4j
 );

// Table with duplicate (sym, time) pair
dupTrades:([]
    date:3#2024.01.15;
    sym:`AAPL`AAPL`AAPL;
    time:3#2024.01.15D09:30:00.000000000;   // all three are the same time
    instrument_id:3#1001j;
    price:185.5 185.5 185.5f;
    size:100 100 100j;
    side:3#`A;
    conditions:3#`128;
    sequence:1 2 3j
 );

// Out-of-order table (MSFT before AAPL but AAPL < MSFT alphabetically)
unsortedTrades:([]
    date:4#2024.01.15;
    sym:`MSFT`AAPL`MSFT`AAPL;
    time:2024.01.15D09:30:00.000000000 2024.01.15D09:31:00.000000000
         2024.01.15D09:32:00.000000000 2024.01.15D09:33:00.000000000;
    instrument_id:1002 1001 1002 1001j;
    price:374.2 185.6 374.3 185.7f;
    size:50 200 75 150j;
    side:`A`B`A`B;
    conditions:4#`128;
    sequence:1 2 3 4j
 );

// Table with null price
nullTrades:update price:0nf from cleanTrades where sym=`AAPL,size=100j;

// ---------------------------------------------------------------------------
// Test 1: checkDuplicateKeys on clean table → 0
// ---------------------------------------------------------------------------
assertEq["no duplicates in clean"; checkDuplicateKeys[cleanTrades]; 0j];

// ---------------------------------------------------------------------------
// Test 2: checkDuplicateKeys on dup table → 2 (3 rows, 1 distinct pair → 2 dups)
// ---------------------------------------------------------------------------
assertEq["dups detected"; checkDuplicateKeys[dupTrades]; 2j];

// ---------------------------------------------------------------------------
// Test 3: checkTimeOrdering on sorted table → 0
// ---------------------------------------------------------------------------
assertEq["sorted table passes ordering"; checkTimeOrdering[cleanTrades]; 0j];

// ---------------------------------------------------------------------------
// Test 4: checkTimeOrdering on unsorted table → count[t]
// ---------------------------------------------------------------------------
assertEq["unsorted table fails ordering"; checkTimeOrdering[unsortedTrades]; `long$count unsortedTrades];

// ---------------------------------------------------------------------------
// Test 5: checkNulls on clean table → all zeros
// ---------------------------------------------------------------------------
nullRes:checkNulls[cleanTrades;`sym`time`price`size];
assertEq["no nulls in clean table"; sum nullRes; 0j];

// ---------------------------------------------------------------------------
// Test 6: checkNulls detects the null price
// ---------------------------------------------------------------------------
nullRes2:checkNulls[nullTrades;`sym`time`price`size];
assertEq["null price detected"; nullRes2`price; 1j];

// ---------------------------------------------------------------------------
// Test 7: runQualityChecks on clean table — all pass
// ---------------------------------------------------------------------------
r:runQualityChecks[cleanTrades;`trades;2024.01.15];
assertEq["clean table: passed=3"; r`passed; 3j];
assertEq["clean table: failed=0"; r`failed; 0j];
assertEq["clean table: dups=0";   r`dups;   0j];

// ---------------------------------------------------------------------------
// Test 8: runQualityChecks on dup table — dups check fails
// ---------------------------------------------------------------------------
r2:runQualityChecks[dupTrades;`trades;2024.01.15];
assertGt["dup table: dups>0"; r2`dups; 0j];
assertGt["dup table: failed>0"; r2`failed; 0j];

// ---------------------------------------------------------------------------
// Test 9: runQualityChecks on null table — nulls check fails
// ---------------------------------------------------------------------------
r3:runQualityChecks[nullTrades;`trades;2024.01.15];
assertGt["null table: total_nulls>0"; r3`total_nulls; 0j];
assertGt["null table: failed>0"; r3`failed; 0j];

// ---------------------------------------------------------------------------
// Test 10: runQualityChecks on empty table — returns skipped
// ---------------------------------------------------------------------------
emptyT:0#cleanTrades;
r4:runQualityChecks[emptyT;`trades;2024.01.15];
assertEq["empty table: status=skipped"; r4`passed; `skipped];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all quality tests passed";
exit 0
