// test_schema.q — unit tests: verify table schemas match expected columns and types.
//
// Run from PACKAGEHOME:
//   q tests/test_schema.q
// Exits 0 on pass, 1 on any failure.

\l schema/schema.q

failures:();

// Build the full failure message string first, THEN cast to symbol.
// (q evaluates right-to-left so parens are needed to control precedence.)
assertEq:{[testName;actual;expected]
    if[not actual~expected;
        msg:testName,": expected ",(raze string expected)," got ",(raze string actual);
        `failures set failures,enlist`$msg
    ]
 };

// ---------------------------------------------------------------------------
// trades
// ---------------------------------------------------------------------------

assertEq["trades cols";
    cols trades;
    `date`sym`time`instrument_id`exchange`price`size`side`conditions`sequence];

// Type chars: d=date s=symbol p=timestamp j=long s=symbol f=float j=long s=symbol s=symbol j=long
assertEq["trades types";
    {exec t from meta x} trades;
    "dspjsfjssj"];

// ---------------------------------------------------------------------------
// ohlcv_1m
// ---------------------------------------------------------------------------

assertEq["ohlcv_1m cols";
    cols ohlcv_1m;
    `date`sym`time`exchange`instrument_id`open`high`low`close`volume];

// d=date s=symbol p=timestamp s=symbol j=long f=float x4 j=long
assertEq["ohlcv_1m types";
    {exec t from meta x} ohlcv_1m;
    "dspsjffffj"];

// ---------------------------------------------------------------------------
// backfill_jobs
// ---------------------------------------------------------------------------

expectedJobCols:`request_id`chunk_id`databento_job_id`schema`symbols`start_date`end_date`status`retries`error_msg`file_path`file_paths`checksum`row_count`min_ts`max_ts`created_at`updated_at;

assertEq["backfill_jobs cols"; cols backfill_jobs; expectedJobCols];

// ---------------------------------------------------------------------------
// Reference tables
// ---------------------------------------------------------------------------

assertEq["ref_security_master cols";
    cols ref_security_master;
    `sym`instrument_id`name`exchange`currency`valid_from`valid_to];

assertEq["ref_corp_actions cols";
    cols ref_corp_actions;
    `sym`action_type`ex_date`record_date`effective_date`factor`description];

assertEq["ref_adj_factors cols";
    cols ref_adj_factors;
    `sym`date`cumulative_factor`split_factor`dividend_factor];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all schema tests passed";
exit 0
