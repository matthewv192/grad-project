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

// Table with duplicate natural keys — same sym, time, exchange, AND sequence.
// These represent the same tick loaded twice (e.g. a re-run without idempotency).
dupTrades:([]
    date:3#2024.01.15;
    sym:`AAPL`AAPL`AAPL;
    time:3#2024.01.15D09:30:00.000000000;
    exchange:3#`$"XNAS.ITCH";
    instrument_id:3#1001j;
    price:185.5 185.5 185.5f;
    size:100 100 100j;
    side:3#`A;
    conditions:3#`128;
    sequence:3#42j                          // same sequence → genuine duplicates
 );

// Table with same (sym, time) but DIFFERENT exchanges — not a duplicate
multiExchangeTrades:([]
    date:2#2024.01.15;
    sym:`AAPL`AAPL;
    time:2#2024.01.15D09:30:00.000000000;
    exchange:(`$"XNAS.ITCH";`$"XNYS.PILLAR");  // different exchanges → not dups
    instrument_id:2#1001j;
    price:185.5 185.5f;
    size:100 100j;
    side:`A`A;
    conditions:2#`128;
    sequence:1 2j
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
// Test 2: checkDuplicateKeys on dup table → 2 (3 rows, 1 distinct key → 2 dups)
// ---------------------------------------------------------------------------
assertEq["dups detected"; checkDuplicateKeys[dupTrades]; 2j];

// ---------------------------------------------------------------------------
// Test 2b: checkDuplicateKeys on multi-exchange table → 0
// Same (sym, time) from different exchanges must NOT be counted as duplicates.
// ---------------------------------------------------------------------------
assertEq["multi-exchange no false-positive dups"; checkDuplicateKeys[multiExchangeTrades]; 0j];

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
// Test 11: runQualityChecks on a clean ohlcv_1m table — all pass
//
// ohlcv_1m uses different key columns (sym time open high low close) from
// trades (sym time price size).  This test confirms the tableName dispatch
// in runQualityChecks selects the right columns for null checking.
// ---------------------------------------------------------------------------

cleanOhlcv:([]
    date:    4#2024.01.15;
    sym:     `AAPL`AAPL`MSFT`MSFT;
    time:    2024.01.15D09:30:00.000000000 2024.01.15D09:31:00.000000000
             2024.01.15D09:30:00.000000000 2024.01.15D09:31:00.000000000;
    exchange:4#`$"XNAS.ITCH";
    instrument_id:1001 1001 1002 1002j;
    open:    100.0 101.0 200.0 201.0f;
    high:    102.0 103.0 202.0 203.0f;
    low:     99.0  100.0 199.0 200.0f;
    close:   101.0 102.0 201.0 202.0f;
    volume:  1000 1100 2000 2100j
 );

r5:runQualityChecks[cleanOhlcv;`ohlcv_1m;2024.01.15];
assertEq["ohlcv clean: passed=3";  r5`passed; 3j];
assertEq["ohlcv clean: failed=0";  r5`failed; 0j];
assertEq["ohlcv clean: dups=0";    r5`dups;   0j];

// ---------------------------------------------------------------------------
// Test 12: runQualityChecks on ohlcv_1m table with null close — nulls check fails
// ---------------------------------------------------------------------------

nullOhlcv:update close:0nf from cleanOhlcv where sym=`AAPL,volume=1000j;
r6:runQualityChecks[nullOhlcv;`ohlcv_1m;2024.01.15];
assertGt["ohlcv null close: total_nulls>0"; r6`total_nulls; 0j];
assertGt["ohlcv null close: failed>0";      r6`failed;      0j];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all quality tests passed";
exit 0
