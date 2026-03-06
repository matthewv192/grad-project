// test_ref_tables.q — unit tests for reference data loading.
//
// Run from PACKAGEHOME:
//   q tests/test_ref_tables.q
// Exits 0 on pass, 1 on any failure.
//
// Writes synthetic CSVs under /tmp and overrides REF_DIR after loading
// ref_tables.q so the real staging/ directory is never touched.

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
// Setup — write small synthetic CSVs under /tmp
// ---------------------------------------------------------------------------

TEST_REF_DIR:"/tmp/grad_ref_test/reference";
system "mkdir -p ",TEST_REF_DIR;

// security_master.csv — 3 rows
(hsym`$TEST_REF_DIR,"/security_master.csv") 0: (
    "sym,instrument_id,name,exchange,currency,valid_from,valid_to";
    "AAPL,1000,AAPL Inc.,XNAS,USD,2000-01-01,9999-12-31";
    "MSFT,1001,MSFT Inc.,XNAS,USD,2000-01-01,9999-12-31";
    "GOOGL,1002,GOOGL Inc.,XNAS,USD,2004-08-19,9999-12-31"
 );

// corp_actions.csv — 2 rows
(hsym`$TEST_REF_DIR,"/corp_actions.csv") 0: (
    "sym,action_type,ex_date,record_date,effective_date,factor,description";
    "AAPL,split,2024-06-10,2024-06-09,2024-06-10,0.5,2-for-1 stock split";
    "MSFT,dividend,2024-05-15,2024-05-14,2024-05-15,0.998,Quarterly dividend"
 );

// adj_factors.csv — 4 rows (two symbols, two dates each)
(hsym`$TEST_REF_DIR,"/adj_factors.csv") 0: (
    "sym,date,cumulative_factor,split_factor,dividend_factor";
    "AAPL,2024-06-09,0.5,0.5,1.0";
    "AAPL,2024-06-10,1.0,1.0,1.0";
    "MSFT,2024-06-09,0.998,1.0,0.998";
    "MSFT,2024-06-10,1.0,1.0,1.0"
 );

// ---------------------------------------------------------------------------
// Load ref_tables.q then override REF_DIR to the test directory.
// ref_tables.q sets REF_DIR as a global variable at load time; the load
// functions read it at CALL time, so overriding it here is sufficient.
// ---------------------------------------------------------------------------

\l code/reference/ref_tables.q
REF_DIR:hsym`$TEST_REF_DIR;

// ---------------------------------------------------------------------------
// Test 1: loadRefSecurityMaster — row count and column types
// ---------------------------------------------------------------------------

n:loadRefSecurityMaster[];
assertEq["security_master row count";  n; 3j];
assertEq["security_master sym type";         type ref_security_master`sym;          11h];
assertEq["security_master instrument_id type"; type ref_security_master`instrument_id; 7h];
assertEq["security_master exchange type";    type ref_security_master`exchange;      11h];
assertEq["security_master valid_from type";  type ref_security_master`valid_from;    14h];
assertEq["security_master valid_to type";    type ref_security_master`valid_to;      14h];

// Spot-check: AAPL instrument_id
aaplId:exec first instrument_id from ref_security_master where sym=`AAPL;
assertEq["AAPL instrument_id"; aaplId; 1000j];

// ---------------------------------------------------------------------------
// Test 2: loadRefCorpActions — row count, types, and data values
// ---------------------------------------------------------------------------

n:loadRefCorpActions[];
assertEq["corp_actions row count"; n; 2j];
assertEq["corp_actions sym type";         type ref_corp_actions`sym;         11h];
assertEq["corp_actions action_type type"; type ref_corp_actions`action_type;  11h];
assertEq["corp_actions ex_date type";     type ref_corp_actions`ex_date;      14h];
assertEq["corp_actions factor type";      type ref_corp_actions`factor;        9h];

// Spot-check values
aaplFactor:exec first factor from ref_corp_actions where sym=`AAPL, action_type=`split;
assertEq["AAPL split factor"; aaplFactor; 0.5f];

msftExDate:exec first ex_date from ref_corp_actions where sym=`MSFT, action_type=`dividend;
assertEq["MSFT dividend ex_date"; msftExDate; 2024.05.15];

// ---------------------------------------------------------------------------
// Test 3: loadRefAdjFactors — row count, types, and data values
// ---------------------------------------------------------------------------

n:loadRefAdjFactors[];
assertEq["adj_factors row count"; n; 4j];
assertEq["adj_factors sym type";               type ref_adj_factors`sym;               11h];
assertEq["adj_factors date type";              type ref_adj_factors`date;              14h];
assertEq["adj_factors cumulative_factor type"; type ref_adj_factors`cumulative_factor;  9h];

preFactor:exec first cumulative_factor from ref_adj_factors where sym=`AAPL, date=2024.06.09;
assertEq["AAPL pre-split cumulative_factor"; preFactor; 0.5f];

postFactor:exec first cumulative_factor from ref_adj_factors where sym=`AAPL, date=2024.06.10;
assertEq["AAPL post-split cumulative_factor"; postFactor; 1.0f];

// ---------------------------------------------------------------------------
// Test 4: loadAllRefData — resets tables then repopulates all three
// ---------------------------------------------------------------------------

`ref_security_master set 0#ref_security_master;
`ref_corp_actions    set 0#ref_corp_actions;
`ref_adj_factors     set 0#ref_adj_factors;

loadAllRefData[];

assertGt["loadAllRefData populated security_master"; count ref_security_master; 0j];
assertGt["loadAllRefData populated corp_actions";    count ref_corp_actions;    0j];
assertGt["loadAllRefData populated adj_factors";     count ref_adj_factors;     0j];

// ---------------------------------------------------------------------------
// Test 5: missing file returns 0 (graceful degradation)
// ---------------------------------------------------------------------------

system "mkdir -p /tmp/grad_ref_missing/reference";
REF_DIR:hsym`$"/tmp/grad_ref_missing/reference";

n:loadRefSecurityMaster[];
assertEq["missing file returns 0"; n; 0j];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all ref_tables tests passed";
exit 0
