// test_symbology.q — unit tests for symbology map: schema, ref_tables, loader integration.
//
// Run from PACKAGEHOME:
//   q tests/test_symbology.q
// Exits 0 on pass, 1 on any failure.

\l schema/schema.q

failures:();

assertEq:{[testName;actual;expected]
    if[not actual~expected;
        msg:testName,": expected ",(raze -3!expected)," got ",(raze -3!actual);
        `failures set failures,enlist`$msg
    ]
 };

// ---------------------------------------------------------------------------
// Test 1: ref_symbology_map schema has expected columns
// ---------------------------------------------------------------------------
assertEq["schema has sym col";     `sym in cols ref_symbology_map;         1b];
assertEq["schema has instr_id col"; `instrument_id in cols ref_symbology_map; 1b];
assertEq["schema has exchange col"; `exchange in cols ref_symbology_map;     1b];
assertEq["schema has valid_from";  `valid_from in cols ref_symbology_map;  1b];
assertEq["schema has valid_to";    `valid_to in cols ref_symbology_map;    1b];

// ---------------------------------------------------------------------------
// Test 2: ref_symbology_map column types
// ---------------------------------------------------------------------------
assertEq["sym type is symbol";          type ref_symbology_map`sym;          11h];
assertEq["instrument_id type is long";  type ref_symbology_map`instrument_id; 7h];
assertEq["exchange type is symbol";      type ref_symbology_map`exchange;       11h];
assertEq["valid_from type is date";     type ref_symbology_map`valid_from;    14h];
assertEq["valid_to type is date";       type ref_symbology_map`valid_to;      14h];

// ---------------------------------------------------------------------------
// Test 3: Load ref_tables.q and test resolveSymbol/resolveInstrumentId
// ---------------------------------------------------------------------------

\l code/reference/ref_tables.q

// Seed the in-memory table directly (no CSV needed for unit test)
testMap:([]
    sym:`AAPL`MSFT`TSLA;
    instrument_id:1001 1002 1003j;
    exchange:3#`$"XNAS.ITCH";
    valid_from:3#2020.01.01;
    valid_to:3#9999.12.31
 );
`ref_symbology_map set testMap;

// resolveSymbol: known instrument_id → correct sym
assertEq["resolveSymbol AAPL";
    resolveSymbol[1001j; `$"XNAS.ITCH"; 2024.06.03];
    `AAPL];

assertEq["resolveSymbol MSFT";
    resolveSymbol[1002j; `$"XNAS.ITCH"; 2024.06.03];
    `MSFT];

// resolveSymbol: unknown instrument_id → `
assertEq["resolveSymbol unknown → null sym";
    resolveSymbol[9999j; `$"XNAS.ITCH"; 2024.06.03];
    `];

// resolveInstrumentId: known sym → correct id
assertEq["resolveInstrumentId AAPL";
    resolveInstrumentId[`AAPL; `$"XNAS.ITCH"; 2024.06.03];
    1001j];

// resolveInstrumentId: unknown sym → 0N
assertEq["resolveInstrumentId unknown → 0N";
    resolveInstrumentId[`UNKNOWN; `$"XNAS.ITCH"; 2024.06.03];
    0Nj];

// ---------------------------------------------------------------------------
// Test 4: updateSymbologyMap (from loader.q) writes to a temp CSV
// ---------------------------------------------------------------------------

// Override STAGING_DIR so loader.q writes to /tmp
tmpStaging:"/tmp/grad_symtest_",string`long$.z.p;
setenv[`STAGING_DIR; tmpStaging];
setenv[`KDBHDB; tmpStaging,"/hdb"];

\l code/backfill/loader.q

// Build a minimal trades table
rawData:([]
    date:2#2024.06.03;
    sym:`AAPL`MSFT;
    time:2024.06.03D09:30:00.000000000 2024.06.03D09:31:00.000000000;
    instrument_id:1001 1002j;
    price:185.5 374.2f;
    size:100 50j;
    side:`A`B;
    conditions:`128`128;
    sequence:1 2j
 );

// updateSymbologyMap now accumulates in .loader.symPending (no disk write).
// flushSymbologyMap merges the accumulator with the on-disk CSV and writes once.
updateSymbologyMap[rawData; 2024.06.03; `$"XNAS.ITCH"];
flushSymbologyMap[];

// Verify the CSV was created
mapPath:hsym`$(tmpStaging,"/reference/symbology_map.csv");
assertEq["symbology CSV created"; mapPath in key mapPath; 1b];

// Verify contents
written:("SJSDD";enlist csv) 0: mapPath;
assertEq["symbology CSV has 2 rows"; count written; 2j];
assertEq["AAPL in symbology map"; `AAPL in written`sym; 1b];
assertEq["MSFT in symbology map"; `MSFT in written`sym; 1b];

// Verify idempotency: accumulating same data and flushing again should not add rows
updateSymbologyMap[rawData; 2024.06.03; `$"XNAS.ITCH"];
flushSymbologyMap[];
written2:("SJSDD";enlist csv) 0: mapPath;
assertEq["idempotent: still 2 rows after second call"; count written2; 2j];

// Verify new sym is added on subsequent call
newData:([]
    date:enlist 2024.06.04;
    sym:enlist `TSLA;
    time:enlist 2024.06.04D09:30:00.000000000;
    instrument_id:enlist 1003j;
    price:enlist 250.0f;
    size:enlist 75j;
    side:enlist `A;
    conditions:enlist `128;
    sequence:enlist 5j
 );
updateSymbologyMap[newData; 2024.06.04; `$"XNAS.ITCH"];
flushSymbologyMap[];
written3:("SJSDD";enlist csv) 0: mapPath;
assertEq["new sym TSLA added"; count written3; 3j];
assertEq["TSLA in symbology map"; `TSLA in written3`sym; 1b];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all symbology tests passed";
exit 0
