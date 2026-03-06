// manifest.q — read and validate JSON manifests written by orchestrator.py,
//              then dispatch each to loadChunk (defined in loader.q).
//
// Manifest JSON schema (one file per downloaded CSV):
// {
//   "request_id":       "req_20240115_001",
//   "chunk_id":         "req_20240115_001_chunk000",
//   "databento_job_id": "DBNJ-XXXXX",
//   "schema":           "trades",
//   "date":             "2024-01-15",
//   "symbols":          ["AAPL","MSFT"],
//   "file_path":        "/abs/path/to/chunk.csv",
//   "row_count":        12345,
//   "checksum":         "sha256:abcdef...",
//   "created_at":       "2024-01-15T10:00:00+00:00"
// }

// Fallback logger (overridden if TorQ is loaded before this file)
if[not `lg in key `.;
    .lg.o:{[proc;msg] -1 (string .z.p)," [",string[proc],"] ",msg;}
 ];

\l schema/schema.q

// ---------------------------------------------------------------------------
// readManifest — parse a single JSON manifest file.
// Returns a typed dictionary, or signals an error string.
// ---------------------------------------------------------------------------
readManifest:{[manifestPath]
    // Read the file and join lines, then parse as JSON
    raw:.j.k raze read0 hsym manifestPath;

    required:`request_id`chunk_id`schema`date`file_path`checksum;
    missing:required where not required in key raw;
    if[count missing;
        '"manifest missing keys: ",", " sv string missing
    ];

    // Build the dict in one expression.
    // kdb+5 specialises the value vector's type once all values share one type.
    // Building incrementally (m[`k]:v) after adding several symbols locks the
    // value vector to 11h (symbol list) and then rejects a date assignment.
    // Constructing via keys!values keeps the value vector as a generic list (0h).
    m:(`request_id`chunk_id`databento_job_id`schema`date`file_path`checksum`symbols`row_count`min_ts`max_ts)!
      (`$raw`request_id;
       `$raw`chunk_id;
       `$raw`databento_job_id;
       `$raw`schema;
       "D"$ssr[raw`date;"-";"."];     // Python writes YYYY-MM-DD; q needs YYYY.MM.DD
       hsym`$raw`file_path;
       `$raw`checksum;
       `$raw`symbols;
       `long$raw`row_count;
       $[0<count raw`min_ts; "P"$raw`min_ts; 0Np];
       $[0<count raw`max_ts; "P"$raw`max_ts; 0Np]);
    m
 };

// ---------------------------------------------------------------------------
// validateManifest — sanity checks on a parsed manifest dict.
// Returns 1b if valid, signals an error string if not.
// ---------------------------------------------------------------------------
validateManifest:{[m]
    if[not (m`file_path) in key m`file_path;
        '"file not found: ",string m`file_path
    ];
    if[not (m`schema) in `trades`ohlcv_1m;
        '"unknown schema: ",string m`schema
    ];
    if[(m`row_count)<=0;
        '"non-positive row_count: ",string m`row_count
    ];
    1b
 };

// ---------------------------------------------------------------------------
// scanManifestDir — list all *.json files in a directory.
// Returns a list of fully-qualified file handles.
// ---------------------------------------------------------------------------
scanManifestDir:{[stagingPath]
    p:hsym stagingPath;
    // key on a directory returns a list of file names as symbols
    allFiles:key p;
    if[0=count allFiles; :()];
    jsonFiles:allFiles where allFiles like "*.json";
    // Build absolute paths.
    // Use ((base),"/"),/:filenames so the full base string is formed first,
    // then /:distributes it across each filename correctly.
    hsym each `$((1_string p),"/"),/:string jsonFiles
 };

// ---------------------------------------------------------------------------
// processManifests — read, validate, and load all manifests in a directory.
// loadChunk must be defined before this is called (it lives in loader.q).
// ---------------------------------------------------------------------------
processManifests:{[stagingPath]
    manifests:scanManifestDir stagingPath;

    if[0=count manifests;
        .lg.o[`manifest;"no manifests found in ",string stagingPath];
        :()
    ];

    .lg.o[`manifest;"processing ",string[count manifests]," manifest(s)"];

    results:{[mPath]
        .lg.o[`manifest;"reading ",string mPath];

        // readManifest can signal — catch errors, log, and return 0 rows for this chunk
        m:@[readManifest; mPath; {[e] .lg.o[`manifest;"read error: ",e]; 0b}];
        if[m~0b; :0j];

        valid:@[validateManifest; m; {[e] .lg.o[`manifest;"validation error: ",e]; 0b}];
        if[valid~0b; :0j];

        // loadChunk is defined in loader.q which loads this file
        n:@[loadChunk; m; {[e] .lg.o[`manifest;"load error: ",e]; 0j}];
        n
    } each manifests;

    total:sum results;
    .lg.o[`manifest;"total rows loaded: ",string total];
    total
 };
