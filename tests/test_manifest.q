// test_manifest.q — unit tests: manifest parsing and validation.
//
// Run from PACKAGEHOME:
//   q tests/test_manifest.q
// Exits 0 on pass, 1 on any failure.

\l schema/schema.q
\l code/backfill/manifest.q

failures:();

// Build the full message string before casting to symbol (avoids kdb+5 precedence trap).
// Note: symbols containing hyphens cannot be written as `sym-with-hyphen in q source —
// they must be constructed at runtime via `$"string".
assertEq:{[testName;actual;expected]
    if[not actual~expected;
        msg:testName,": expected ",(raze -3!expected)," got ",(raze -3!actual);
        `failures set failures,enlist`$msg
    ]
 };

assertErr:{[testName;fn]
    result:@[fn;::;{1b}];
    if[not result~1b;
        `failures set failures,enlist`$(testName,": expected error but none raised")
    ]
 };

// ---------------------------------------------------------------------------
// Write a real CSV file so the file-exists check in validateManifest passes
// ---------------------------------------------------------------------------

`:/tmp/grad_test_chunk.csv 0: enlist
    "ts_recv,ts_event,rtype,publisher_id,instrument_id,action,side,depth,price,size,flags,ts_in_delta,sequence,symbol";

// ---------------------------------------------------------------------------
// Build and write a sample manifest
// sampleManifest values are all strings (char vectors) because .j.j serialises
// q dicts with mixed types; .j.k will parse them back to strings on the other side.
// ---------------------------------------------------------------------------

sampleManifest:()!();
sampleManifest[`request_id]        :"req_test_001";
sampleManifest[`chunk_id]          :"req_test_001_chunk000";
sampleManifest[`databento_job_id]  :"DBNJ-TEST123";
sampleManifest[`schema]            :"trades";
sampleManifest[`date]              :"2024-01-15";
sampleManifest[`symbols]           :("AAPL";"MSFT");
sampleManifest[`file_path]         :"/tmp/grad_test_chunk.csv";
sampleManifest[`row_count]         :12345;
sampleManifest[`checksum]          :"sha256:abc123";
sampleManifest[`min_ts]            :"";
sampleManifest[`max_ts]            :"";
sampleManifest[`created_at]        :"2024-01-15T10:00:00+00:00";

// Use 0: (overwrite) so test re-runs don't append stale JSON to the file
manifestPath:"/tmp/grad_test_manifest.json";
(hsym`$manifestPath) 0: enlist .j.j sampleManifest;

// ---------------------------------------------------------------------------
// readManifest round-trip
// ---------------------------------------------------------------------------

m:readManifest`$manifestPath;

assertEq["request_id";       m`request_id;       `req_test_001];
assertEq["chunk_id";         m`chunk_id;         `req_test_001_chunk000];
// Hyphens in symbols must be constructed from strings — `DBNJ-TEST123 in source
// would be parsed as `DBNJ minus variable TEST123.
assertEq["databento_job_id"; m`databento_job_id; `$"DBNJ-TEST123"];
assertEq["schema";           m`schema;           `trades];
assertEq["date";             m`date;             2024.01.15];
assertEq["row_count";        m`row_count;        12345j];
assertEq["symbols";          m`symbols;          `AAPL`MSFT];
assertEq["min_ts is null";   null m`min_ts;      1b];

// ---------------------------------------------------------------------------
// validateManifest — valid manifest should return 1b
// ---------------------------------------------------------------------------

assertEq["validateManifest pass"; validateManifest[m]; 1b];

// ---------------------------------------------------------------------------
// Error cases
// ---------------------------------------------------------------------------

// Non-existent file_path should fail
badPath:m,enlist[`file_path]!enlist hsym`$"/tmp/nonexistent_grad_test_file.csv";
assertErr["file not found"; {validateManifest badPath}];

// Unknown schema should fail
badSchema:m,enlist[`schema]!enlist`unknown_schema;
assertErr["bad schema"; {validateManifest badSchema}];

// Zero row count should fail
zeroRows:m,enlist[`row_count]!enlist 0j;
assertErr["zero row count"; {validateManifest zeroRows}];

// ---------------------------------------------------------------------------
// scanManifestDir — should find the test manifest
// ---------------------------------------------------------------------------

system "mkdir -p /tmp/grad_test_manifests";
system "cp ",manifestPath," /tmp/grad_test_manifests/";
found:scanManifestDir`$"/tmp/grad_test_manifests";
assertEq["scanManifestDir finds manifest"; 1<=count found; 1b];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all manifest tests passed";
exit 0
