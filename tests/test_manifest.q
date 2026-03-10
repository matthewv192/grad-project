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
sampleManifest[`exchange]          :"XNAS.ITCH";
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
assertEq["exchange";         m`exchange;         `$"XNAS.ITCH"];
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
// Test: readManifest falls back to "dataset" key (legacy manifests)
//
// Pre-multi-exchange manifests used the key "dataset" where current ones use
// "exchange".  readManifest must accept both.
// ---------------------------------------------------------------------------

legacyManifest:()!();
legacyManifest[`request_id]       :"req_test_legacy";
legacyManifest[`chunk_id]         :"req_test_legacy_chunk000";
legacyManifest[`databento_job_id] :"DBNJ-LEGACY1";
legacyManifest[`dataset]          :"XNAS.ITCH";    // old key — no `exchange` present
legacyManifest[`schema]           :"trades";
legacyManifest[`date]             :"2024-01-15";
legacyManifest[`symbols]          :enlist "AAPL";
legacyManifest[`file_path]        :"/tmp/grad_test_chunk.csv";
legacyManifest[`row_count]        :12345;
legacyManifest[`checksum]         :"sha256:abc123";
legacyManifest[`min_ts]           :"";
legacyManifest[`max_ts]           :"";
legacyManifest[`created_at]       :"2024-01-15T10:00:00+00:00";

legacyManifestPath:"/tmp/grad_test_legacy_manifest.json";
(hsym`$legacyManifestPath) 0: enlist .j.j legacyManifest;

mLegacy:readManifest`$legacyManifestPath;
assertEq["legacy dataset key parsed as exchange"; mLegacy`exchange; `$"XNAS.ITCH"];

// Manifest with neither exchange nor dataset — should default to XNAS.ITCH
minimalManifest:()!();
minimalManifest[`request_id]       :"req_test_minimal";
minimalManifest[`chunk_id]         :"req_test_minimal_chunk000";
minimalManifest[`databento_job_id] :"DBNJ-MINIMAL1";
// intentionally no `exchange` or `dataset` key
minimalManifest[`schema]           :"trades";
minimalManifest[`date]             :"2024-01-15";
minimalManifest[`symbols]          :enlist "AAPL";
minimalManifest[`file_path]        :"/tmp/grad_test_chunk.csv";
minimalManifest[`row_count]        :12345;
minimalManifest[`checksum]         :"sha256:abc123";
minimalManifest[`min_ts]           :"";
minimalManifest[`max_ts]           :"";
minimalManifest[`created_at]       :"2024-01-15T10:00:00+00:00";

minimalManifestPath:"/tmp/grad_test_minimal_manifest.json";
(hsym`$minimalManifestPath) 0: enlist .j.j minimalManifest;

mMinimal:readManifest`$minimalManifestPath;
assertEq["no exchange/dataset defaults to XNAS.ITCH"; mMinimal`exchange; `$"XNAS.ITCH"];

// ---------------------------------------------------------------------------
// Test: scanManifestDir on empty directory returns empty list
// ---------------------------------------------------------------------------

system "mkdir -p /tmp/grad_empty_manifests";
emptyFound:scanManifestDir`$"/tmp/grad_empty_manifests";
assertEq["empty dir returns 0 manifests"; count emptyFound; 0j];

// ---------------------------------------------------------------------------
// Test: showJobsTable — materialise job store as a kdb+ table
// ---------------------------------------------------------------------------

// Write a synthetic job record to a temp jobs dir
jobsDir:"/tmp/grad_test_jobs";
system "mkdir -p ",jobsDir;

jobRecord:()!();
jobRecord[`request_id]  :"req_show_001";
jobRecord[`chunk_id]    :"req_show_001_chunk000";
jobRecord[`dataset]     :"XNAS.ITCH";
jobRecord[`schema]      :"trades";
jobRecord[`date]        :"2024-01-15";
jobRecord[`status]      :"verified";
jobRecord[`retries]     :0;
jobRecord[`row_count]   :5000;
jobRecord[`error_msg]   :"";
jobRecord[`failure_type]:"";
jobRecord[`created_at]  :"2024-01-15T10:00:00+00:00";
jobRecord[`updated_at]  :"2024-01-15T10:05:00+00:00";

(hsym`$jobsDir,"/req_show_001_chunk000.json") 0: enlist .j.j jobRecord;

jt:showJobsTable`$jobsDir;

assertEq["showJobsTable returns table";     type jt; 98h];
assertEq["showJobsTable has 1 row";         count jt; 1j];
assertEq["showJobsTable request_id";        jt[0;`request_id]; `req_show_001];
assertEq["showJobsTable chunk_id";          jt[0;`chunk_id];   `req_show_001_chunk000];
assertEq["showJobsTable status";            jt[0;`status];     `verified];
assertEq["showJobsTable row_count";         jt[0;`row_count];  5000j];
assertEq["showJobsTable date";              jt[0;`date];       2024.01.15];

// Empty dir returns empty typed table
system "mkdir -p /tmp/grad_empty_jobs";
jtEmpty:showJobsTable`$"/tmp/grad_empty_jobs";
assertEq["showJobsTable empty dir is table";  type jtEmpty; 98h];
assertEq["showJobsTable empty dir has 0 rows"; count jtEmpty; 0j];

// Non-existent dir returns empty typed table
jtMissing:showJobsTable`$"/tmp/grad_nonexistent_jobs_xyz";
assertEq["showJobsTable missing dir is table"; type jtMissing; 98h];

// ---------------------------------------------------------------------------
// Report
// ---------------------------------------------------------------------------

if[count failures;
    -1 "FAIL: ",string[count failures]," test(s) failed:";
    -1 each string failures;
    exit 1];
-1 "PASS: all manifest tests passed";
exit 0
