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
// loadAllRefData — convenience wrapper; call this on startup
// ---------------------------------------------------------------------------
loadAllRefData:{[]
    loadRefSecurityMaster[];
    loadRefCorpActions[];
    loadRefAdjFactors[];
    .lg.o[`ref;"all reference tables loaded"]
 };
