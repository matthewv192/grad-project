// schema.q — canonical table schemas for the grad-project backfill package.
//
// All q processes that need to know about table structure should load this file.
// Keeping schemas in one place means a column-type change only needs one edit.
//
// Load via:  \l schema/schema.q    (relative to PACKAGEHOME)

// ---------------------------------------------------------------------------
// Market data tables
// ---------------------------------------------------------------------------

// trades — individual print-level trade records from Databento XNAS.ITCH
// Databento field mapping:
//   ts_event  -> time (nanosecond timestamp)
//   instrument_id -> instrument_id
//   price     -> price  (scaled integer converted to float)
//   size      -> size
//   side      -> side   (A=Ask/sell-initiated, B=Bid/buy-initiated, N=None)
//   conditions -> conditions (exchange condition codes)
//   action    -> ignored at this stage (only "T" trade records kept)
trades:([]
    date:`date$();
    sym:`symbol$();
    time:`timestamp$();
    instrument_id:`long$();
    price:`float$();
    size:`long$();
    side:`symbol$();
    conditions:`symbol$();
    sequence:`long$()
 );

// ohlcv_1m — one-minute OHLCV bars from Databento XNAS.ITCH
// 'time' is the bar *open* time (start of the 1-minute window).
ohlcv_1m:([]
    date:`date$();
    sym:`symbol$();
    time:`timestamp$();
    instrument_id:`long$();
    open:`float$();
    high:`float$();
    low:`float$();
    close:`float$();
    volume:`long$()
 );

// ---------------------------------------------------------------------------
// Job tracking table
// ---------------------------------------------------------------------------

// backfill_jobs — persisted state machine for every chunk submitted to Databento.
// Status lifecycle:  submitted -> running -> downloaded -> loaded -> verified
//                                                               \-> failed
// One row per (request_id; chunk_id).
backfill_jobs:([]
    request_id:`symbol$();        // top-level user request id, e.g. "req_20240115_001"
    chunk_id:`symbol$();          // unique chunk, e.g. "req001_2024.01.15_batch0"
    databento_job_id:`symbol$();  // job id returned by Databento API
    schema:`symbol$();            // `trades or `ohlcv_1m
    symbols:();                   // list of symbols in this chunk
    start_date:`date$();
    end_date:`date$();
    status:`symbol$();
    retries:`int$();
    error_msg:();                 // generic list — string or null
    file_path:`symbol$();
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
ref_corp_actions:([]
    sym:`symbol$();
    action_type:`symbol$();   // `split`dividend`merger
    ex_date:`date$();
    record_date:`date$();
    effective_date:`date$();
    factor:`float$();
    description:()            // generic list — string description or null
 );

// ref_adj_factors — cumulative price adjustment factors per sym per date.
// Used by adjlib.q to scale prices and volumes for corporate actions.
ref_adj_factors:([]
    sym:`symbol$();
    date:`date$();
    cumulative_factor:`float$();
    split_factor:`float$();
    dividend_factor:`float$()
 );

// ref_symbology_map — maps Databento instrument_id → normalised sym, per dataset.
// Auto-populated by loader.q after each partition write; also loadable from CSV
// via loadSymbologyMap[] in ref_tables.q.
// valid_from/valid_to support point-in-time lookups via aj.
ref_symbology_map:([]
    sym:`symbol$();
    instrument_id:`long$();
    dataset:`symbol$();
    valid_from:`date$();
    valid_to:`date$()
 );
