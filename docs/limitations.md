# Known Limitations

This document describes current constraints and known gaps in the system. Items listed here are either by design, or require further work before the system is production-ready.

---

## Stub Components (Not Production-Ready)

The following components are implemented but use synthetic data. They must be replaced with real data sources before using this system in a live environment.

| Component | Current State | What to Replace |
|---|---|---|
| `ref_ingest.py` | Generates synthetic security master, corp actions, and adj factor CSVs for five test symbols | Integrate a real data provider (Bloomberg, Refinitiv, etc.) |
| `ref_tables.q` | Reads from CSV files; no live feed | Sufficient for batch backfill; add a real-time source for production |
| `adjlib.q` | `backward` and `forward` adjustment methods are implemented and tested against synthetic factors | Factors must be sourced from a real corporate actions provider for production use |

> **Important:** `ref_ingest.py` assigns `instrument_id` values starting at 1000 for its test symbols. Real Databento instrument IDs are exchange-assigned integers that differ. Using the stub reference data alongside real backfill output will cause `resolveSymbol` / `resolveInstrumentId` lookups to return null for all real instruments.

---

## Data Loading Behaviour

### Empty CSV files are not written to the HDB
When Databento returns a zero-row CSV (e.g. a public holiday, trading halt, or no activity for the requested symbol), `loadChunkBatch` logs a warning and returns without writing to the HDB. The `.Q.chk` call at the end of `runLoader` will create an empty placeholder directory for the missing table, but the partition date itself is not created. Queries that span a date range will see gaps rather than empty rows for those dates.

### Quality checks run on in-memory data only
`runQualityChecks` in `quality.q` checks the freshly-read data before it is written to disk. Empty placeholder rows added by `.Q.chk` are not quality-checked.

### Type casting errors abort the current load
The type casts in `readTradesCSV` and `readOhlcvCSV` (in `loader.q`) are not wrapped in error traps. A single malformed value in a CSV will abort `loadChunkBatch` for that chunk. If the error occurs after the partition write lock has been acquired, the lock directory is left on disk (stale lock).

**Recovery:** Clear stale locks with:
```bash
find hdb -name ".*.lock" -type d -exec rmdir {} +
```
Then reset the chunk status to `downloaded` in `staging/metadata/jobs/` and re-run.

### Only `trades` and `ohlcv-1m` schemas are supported
Adding support for other Databento schemas (e.g. `mbp-1`, `tbbo`, `ohlcv-1d`) requires:
1. A new column map in `loader.q`
2. A new schema table in `schema.q`
3. A new CSV parser function (`readXxxCSV`)

---

## Cost Safeguard

The `BACKFILL_MAX_COST_USD` cost guard (default $50) calls the Databento metadata API before each batch submission. If Databento returns an API error for that call (e.g. unknown dataset, invalid symbol type), the cost estimate defaults to `0.0` and the guard is **disabled for that chunk** — the job is submitted without a cost check. A warning is logged when this happens.

Unexpected failures (network timeout, SDK bug) abort the run entirely, so the guard is never silently bypassed by an infrastructure fault.

Always use `--dry-run` first when working with a new date range or symbol list to see the cost estimate before committing.

---

## Scalability

### Chunk volume scales with symbol × date count
Each chunk is one symbol × one exchange × one day. A request for 100 symbols over 252 trading days produces 25,200 Databento batch jobs. Large requests should use `--dry-run` first to preview the job count and cost before submitting.

### `--status` reads all job records on every call
`python orchestrator.py --status` reads and parses every `.json` file in the job store directory each time it is called. For large backfills with thousands of chunks this can be slow. There is currently no indexing or caching.

---

## Concurrency

### Partition write lock is not safe on network filesystems
`acquireWriteLock` in `loader.q` uses `mkdir` as an atomic lock. POSIX guarantees atomicity only on local filesystems. On NFS or CIFS, two processes can both succeed, potentially corrupting a partition. Keep the HDB on local storage.

### Python-side flock only prevents conflicts on a single machine
The `fcntl.flock` call in `run_q_loader` (in `orchestrator.py`) prevents two concurrent loader invocations from the same Python process on the same host. It does not prevent two separate machines from writing to the same HDB simultaneously.

---

## Operational Constraints

### No intraday partition updates
`.Q.dpft` writes an entire partition in one operation. There is no mechanism to append individual late-arriving rows to an existing date partition. Adding a late trade to an already-loaded date requires re-loading the full partition.

### Configuration is locked at process start
`config/settings.q` values are loaded into q at startup. Changing an environment variable after the process has started has no effect. Restart the process to pick up configuration changes.

---

## Data Scope

- **Supported exchanges:** Any Databento dataset that provides `trades` or `ohlcv-1m` schema. Tested with `XNAS.ITCH`, `XNYS.PILLAR`, `IEXG.TOPS`, and `EQUS.MINI`.
- **Extended hours:** Databento's `trades` schema includes pre- and post-market trades. Filter on `time` if you only want regular session data.
- **Asset classes:** Only US equities have been tested. Futures, options, and FX are untested.

---

## Not Supported

- Real-time or streaming data ingestion — batch only
- Non-equity asset classes (futures, options, FX) — untested
- Databento streaming API — not used
- Windows — shell scripts are bash only; use WSL on Windows
