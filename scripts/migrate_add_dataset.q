// migrate_add_dataset.q — one-time migration: add `exchange column to legacy
// partitions that were written before multi-exchange support was added.
//
// Assigns `$"XNAS.ITCH" to all rows (correct for all pre-migration backfills).
//
// Run from PACKAGEHOME:
//   q scripts/migrate_add_dataset.q
// Exits 0 on success, 1 on any failure.

\l schema/schema.q

HDB_DIR:hsym`$$[count s:getenv`KDBHDB;s;"hdb"];
DEFAULT_DS:`$"XNAS.ITCH";

failures:0;

migratePart:{[hdbDir;dt;tname;defaultDs]
    partDir:` sv hdbDir,`$string dt;
    if[not tname in key partDir; :(::)];
    exFile:` sv partDir,tname,`exchange;
    if[exFile in key exFile; :(::)];          // already migrated — skip
    n:count get ` sv partDir,tname,`sym;
    if[0=n; :(::)];                           // empty placeholder — skip
    -1 "Migrating ",string[dt],"/",string[tname]," (",string[n]," rows) ...";
    t:get ` sv partDir,tname;
    t:@[t;`sym;{`$string x}];                 // unenumerate sym
    // Only keep columns the current schema knows about (drop any stale extras)
    t:(cols[value tname] inter cols t)#t;
    // Add exchange column: flip to dict, add key, flip back to table
    d:flip t;
    d[`exchange]:count[t]#defaultDs;
    t:flip d;
    // Set global so .Q.dpft writes the migrated data (not the empty schema table)
    tname set t;
    err:.[.Q.dpft;(hdbDir;dt;`sym;tname);{[e]e}];
    $[10h=type err;
        [-1 "  FAILED: ",err; `failures set failures+1];
        -1 "  OK: ",string[count t]," rows → ",string defaultDs
    ]
 };

{[dt]
    migratePart[HDB_DIR;dt;`trades;DEFAULT_DS];
    migratePart[HDB_DIR;dt;`ohlcv_1m;DEFAULT_DS]
 } each asc key[HDB_DIR] except `sym;

if[failures>0;
    -1 "MIGRATION FAILED: ",string[failures]," partition(s) errored";
    exit 1];
-1 "Migration complete";
exit 0
