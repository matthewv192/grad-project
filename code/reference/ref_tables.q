// ref_tables.q — STUB: load reference data CSVs into in-memory reference tables.
//
// Milestone 3: reads CSVs produced by ref_ingest.py and populates the
// ref_security_master, ref_corp_actions, and ref_adj_factors tables defined
// in schema/schema.q.
//
// In production, replace the CSV paths with your actual data provider's output.

\l schema/schema.q

if[not `lg in key `.;
    .lg.o:{[proc;msg] -1 (string .z.p)," [",string[proc],"] ",msg;}
 ];

REF_DIR:hsym`$$[count s:getenv`STAGING_DIR;s;"staging"],"/reference";

// ---------------------------------------------------------------------------
// loadRefTable — generic helper: load a reference CSV into a named table.
// ---------------------------------------------------------------------------
loadRefTable:{[tableSymbol;filename;typeStr]
    p:` sv REF_DIR,`$filename;
    if[not p in key p;
        .lg.o[`ref;filename," not found — run ref_ingest.py first"];
        :0
    ];
    raw:(typeStr;enlist csv) 0: p;
    (tableSymbol) set raw;
    .lg.o[`ref;"loaded ",string[count raw]," rows into ",string tableSymbol];
    count raw
 };

loadRefSecurityMaster:{[] loadRefTable[`ref_security_master;"security_master.csv";"SJSSSDD"]};
loadRefCorpActions:{[]    loadRefTable[`ref_corp_actions;"corp_actions.csv";"SSDDDFSP"]};
loadRefAdjFactors:{[]     loadRefTable[`ref_adj_factors;"adj_factors.csv";"SDFFFP"]};

// ---------------------------------------------------------------------------
// loadSymbologyMap — read the auto-generated symbology_map.csv into memory
// ---------------------------------------------------------------------------
loadSymbologyMap:{[]
    p:` sv REF_DIR,`symbology_map.csv;
    if[not p in key p;
        .lg.o[`ref;"symbology_map.csv not found — run backfill first to populate"];
        :0
    ];
    // sym(S) instrument_id(J) exchange(S) valid_from(D) valid_to(D)
    raw:("SJSDD";enlist csv) 0: p;
    `ref_symbology_map set raw;
    .lg.o[`ref;"loaded ",string[count raw]," rows into ref_symbology_map"];
    count raw
 };

// ---------------------------------------------------------------------------
// resolveSymbol — look up sym for an instrument_id in a dataset as of a date.
// Uses aj (asof join) to handle valid_from/valid_to ranges.
// Returns ` (null symbol) if no match.
// ---------------------------------------------------------------------------
resolveSymbol:{[instId;dsname;asofDate]
    if[0=count ref_symbology_map; :`];
    // Filter to matching instrument_id and dataset, then find row valid on asofDate
    t:select sym, valid_from from ref_symbology_map
       where instrument_id=instId, exchange=dsname, valid_from<=asofDate, valid_to>=asofDate;
    $[count t; first t`sym; `]
 };

// ---------------------------------------------------------------------------
// resolveInstrumentId — reverse lookup: sym → instrument_id as of a date.
// Returns 0N (null long) if no match.
// ---------------------------------------------------------------------------
resolveInstrumentId:{[symName;dsname;asofDate]
    if[0=count ref_symbology_map; :0Nj];
    t:select instrument_id, valid_from from ref_symbology_map
       where sym=symName, exchange=dsname, valid_from<=asofDate, valid_to>=asofDate;
    $[count t; first t`instrument_id; 0Nj]
 };

// ---------------------------------------------------------------------------
// loadAllRefData — convenience wrapper; call this on startup
// ---------------------------------------------------------------------------
loadAllRefData:{[]
    loadRefSecurityMaster[];
    loadRefCorpActions[];
    loadRefAdjFactors[];
    loadSymbologyMap[];
    .lg.o[`ref;"all reference tables loaded"]
 };
