// schema.q — canonical table schemas for the grad-project backfill package.
//
// All q processes that need to know about table structure should load this file.
// Keeping schemas in one place means a column-type change only needs one edit.
//
// Load via:  \l schema/schema.q    (relative to PACKAGEHOME)

// ---------------------------------------------------------------------------
// Market data tables
// ---------------------------------------------------------------------------

// trades — individual print-level trade records from Databento.
// Works with any Databento dataset that supports the "trades" schema
// (e.g. XNAS.ITCH, GLBX.MDP3, OPRA.PILLAR).
// Databento field mapping:
//   ts_event  -> time (nanosecond timestamp)
//   instrument_id -> instrument_id
//   price     -> price  (scaled integer converted to float)
//   size      -> size
//   side      -> side   (A=Ask/sell-initiated, B=Bid/buy-initiated, N=None)
//   conditions -> conditions (exchange condition codes)
//   action    -> ignored at this stage (only "T" trade records kept)
// Guard: do not redefine if already loaded from HDB (would clobber partition data)
if[not `trades in tables[];
    trades:([]
        date:`date$();
        time:`timestamp$();
        sym:`symbol$();
        exchange:`symbol$();
        price:`float$();
        size:`long$();
        side:`symbol$();
        conditions:`symbol$();
        sequence:`long$();
        instrument_id:`long$()
    )];

// ohlcv_1m — one-minute OHLCV bars from Databento.
// Works with any Databento dataset that supports the "ohlcv-1m" schema.
// 'time' is the bar *open* time (start of the 1-minute window).
// Guard: do not redefine if already loaded from HDB (would clobber partition data)
if[not `ohlcv_1m in tables[];
    ohlcv_1m:([]
        date:`date$();
        time:`timestamp$();
        sym:`symbol$();
        exchange:`symbol$();
        open:`float$();
        high:`float$();
        low:`float$();
        close:`float$();
        volume:`long$();
        instrument_id:`long$()
    )];

// ---------------------------------------------------------------------------
// Job tracking table
// ---------------------------------------------------------------------------

// backfill_jobs — persisted state machine for every chunk submitted to Databento.
// Status lifecycle:  submitted -> running -> downloaded -> loaded -> verified
//                                                               \-> failed
// One row per (request_id; chunk_id). Persisted as a binary kdb table at
// staging/metadata/backfill_jobs — written and read by jobstore.q.
backfill_jobs:([]
    request_id:`symbol$();        // top-level user request id, e.g. "req_20240115_001"
    chunk_id:`symbol$();          // unique chunk, e.g. "req001_2024.01.15_AAPL"
    databento_job_id:`symbol$();  // job id returned by Databento API
    dataset:`symbol$();           // Databento dataset, e.g. `XNAS.ITCH
    schema:`symbol$();            // `trades or `ohlcv_1m
    symbols:();                   // generic list — sym batch for this chunk
    date:`date$();                // trading date for this chunk
    status:`symbol$();
    retries:`int$();
    error_msg:();                 // generic list — string or null
    failure_type:`symbol$();      // api_error | download_error | parse_error | load_error | quality_error
    file_path:`symbol$();
    file_paths:();                // generic list — all CSV paths for multi-file jobs
    checksum:`symbol$();
    row_count:`long$();
    min_ts:`timestamp$();
    max_ts:`timestamp$();
    created_at:`timestamp$();
    updated_at:`timestamp$()
 );

// ---------------------------------------------------------------------------
// Reference data tables (Milestone 3 — schemas defined now, data stubbed)
// ---------------------------------------------------------------------------

// ref_security_master — static security attributes
ref_security_master:([]
    sym:`symbol$();
    instrument_id:`long$();
    name:`symbol$();
    exchange:`symbol$();
    currency:`symbol$();
    valid_from:`date$();
    valid_to:`date$()
 );

// ref_corp_actions — corporate action events (splits, dividends, mergers)
// loaded_at records when each event was ingested, enabling PIT queries.
// Rows are append-only — no upsert; each ingestion adds a new revision.
ref_corp_actions:([]
    sym:`symbol$();
    action_type:`symbol$();   // `split`dividend`merger
    ex_date:`date$();
    record_date:`date$();
    effective_date:`date$();
    factor:`float$();
    description:()            // generic list — string description or null
    loaded_at:`timestamp$()
 );

// ref_adj_factors — cumulative price adjustment factors per sym per date.
// Used by adjlib.q to scale prices and volumes for corporate actions.
// loaded_at records when each batch of factors was ingested, enabling
// point-in-time (PIT) queries: "what factors did I have as of time T?"
ref_adj_factors:([]
    sym:`symbol$();
    date:`date$();
    cumulative_factor:`float$();
    split_factor:`float$();
    dividend_factor:`float$();
    loaded_at:`timestamp$()
 );

// ref_symbology_map — maps Databento instrument_id → normalised sym, per dataset.
// Auto-populated by loader.q after each partition write; also loadable from CSV
// via loadSymbologyMap[] in ref_tables.q.
// valid_from/valid_to support point-in-time lookups via aj.
ref_symbology_map:([]
    sym:`symbol$();
    instrument_id:`long$();
    exchange:`symbol$();
    valid_from:`date$();
    valid_to:`date$()
 );
