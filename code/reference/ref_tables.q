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

REF_DIR:hsym`$$[`STAGING_DIR in key .z.e;getenv`STAGING_DIR;"staging"],"/reference";

// ---------------------------------------------------------------------------
// loadRefSecurityMaster
// ---------------------------------------------------------------------------
loadRefSecurityMaster:{[]
    p:` sv REF_DIR,`security_master.csv;
    if[not p in key p;
        .lg.o[`ref;"security_master.csv not found — run ref_ingest.py first"];
        :0
    ];
    // sym(S) instrument_id(J) name(S) exchange(S) currency(S) valid_from(D) valid_to(D)
    raw:("SJSSSDD";enlist csv) 0: p;
    `ref_security_master set raw;
    .lg.o[`ref;"loaded ",string[count raw]," rows into ref_security_master"];
    count raw
 };

// ---------------------------------------------------------------------------
// loadRefCorpActions
// ---------------------------------------------------------------------------
loadRefCorpActions:{[]
    p:` sv REF_DIR,`corp_actions.csv;
    if[not p in key p;
        .lg.o[`ref;"corp_actions.csv not found — run ref_ingest.py first"];
        :0
    ];
    // sym(S) action_type(S) ex_date(D) record_date(D) effective_date(D) factor(F) description(S)
    raw:("SSDDDFS";enlist csv) 0: p;
    `ref_corp_actions set raw;
    .lg.o[`ref;"loaded ",string[count raw]," rows into ref_corp_actions"];
    count raw
 };

// ---------------------------------------------------------------------------
// loadRefAdjFactors
// ---------------------------------------------------------------------------
loadRefAdjFactors:{[]
    p:` sv REF_DIR,`adj_factors.csv;
    if[not p in key p;
        .lg.o[`ref;"adj_factors.csv not found — run ref_ingest.py first"];
        :0
    ];
    // sym, date, cumulative_factor, split_factor, dividend_factor
    raw:("SDFFF";enlist csv) 0: p;
    `ref_adj_factors set raw;
    .lg.o[`ref;"loaded ",string[count raw]," rows into ref_adj_factors"];
    count raw
 };

// ---------------------------------------------------------------------------
// loadSymbologyMap — read the auto-generated symbology_map.csv into memory
// ---------------------------------------------------------------------------
loadSymbologyMap:{[]
    p:` sv REF_DIR,`symbology_map.csv;
    if[not p in key p;
        .lg.o[`ref;"symbology_map.csv not found — run backfill first to populate"];
        :0
    ];
    // sym(S) instrument_id(J) dataset(S) valid_from(D) valid_to(D)
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
       where instrument_id=instId, dataset=dsname, valid_from<=asofDate, valid_to>=asofDate;
    $[count t; first t`sym; `]
 };

// ---------------------------------------------------------------------------
// resolveInstrumentId — reverse lookup: sym → instrument_id as of a date.
// Returns 0N (null long) if no match.
// ---------------------------------------------------------------------------
resolveInstrumentId:{[symName;dsname;asofDate]
    if[0=count ref_symbology_map; :0Nj];
    t:select instrument_id, valid_from from ref_symbology_map
       where sym=symName, dataset=dsname, valid_from<=asofDate, valid_to>=asofDate;
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
