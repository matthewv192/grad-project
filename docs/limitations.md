# Known Limitations

This document describes current constraints and known gaps in the system. Items listed here are either by design, or require further work before the system is production-ready.

---

## Stub Components (Not Production-Ready)

The following components are implemented but use synthetic data. They must be replaced with real data sources before using this system in a live environment.

| Component | Current State | What to Replace |
|---|---|---|
| `ref_ingest.py` | Fetches real corp actions and dividends via yfinance; called automatically after each backfill. Security master is still a stub (no free provider) — populated with placeholders. `--synthetic` flag available for CI/offline use. | Replace security master with a real provider (Bloomberg, Refinitiv, etc.) |
| `ref_tables.q` | Reads from CSV files; no live feed | Sufficient for batch backfill; add a real-time source for production |
| `adjlib.q` | `backward` and `forward` adjustment methods with point-in-time `asOf` filtering; factors sourced from yfinance via `ref_ingest.py` | yfinance coverage may be incomplete for older history or non-US securities |

> **Important:** `ref_ingest.py` assigns `instrument_id` values starting at 1000 for its stub security master entries. Real Databento instrument IDs are exchange-assigned integers that differ. Using the stub security master alongside real backfill output will cause `resolveSymbol` / `resolveInstrumentId` lookups to return null for all real instruments.

---

## Data Loading Behaviour

### Empty CSV files are not written to the HDB
When Databento returns a zero-row CSV (e.g. a public holiday, trading halt, or no activity for the requested symbol), `loadChunkBatch` logs a warning and returns without writing to the HDB. The `.Q.chk` call at the end of `runLoader` will create an empty placeholder directory for the missing table, but the partition date itself is not created. Queries that span a date range will see gaps rather than empty rows for those dates.

The chunk is still marked `verified` in the job store, so `--status` will show it as successful. There is no distinct status for "completed but empty" — a clean status display does not guarantee data exists in the HDB for that date.

### No post-write row count verification
Python counts rows in the downloaded CSV and stores that count in the manifest. The q loader checks that the parsed row count matches the manifest before writing. However, after `.Q.dpft` writes the partition to disk, the actual row count on disk is never re-verified. A partial write caused by a filesystem error during `.Q.dpft` would not be detected and the chunk would still be marked `verified`.

### Quality checks run on in-memory data only
`runQualityChecks` in `quality.q` checks the freshly-read data before it is written to disk. Empty placeholder rows added by `.Q.chk` are not quality-checked.

### Type casting errors abort the current load
The type casts in `readTradesCSV` and `readOhlcvCSV` (in `loader.q`) are not wrapped in error traps. A single malformed value in a CSV will abort `loadChunkBatch` for that chunk.

The partition merge and write block is wrapped in a protected eval that releases the write lock before signalling, so a type cast failure during the merge step will not leave a stale lock on disk. However, a failure during the initial CSV parse (before the lock is acquired) is not error-trapped and will propagate as an unhandled signal.

**Recovery from stale locks (if they occur):** Clear with:
```bash
find hdb -name ".*.lock" -type d -exec rmdir {} +
```
Then re-run with `--retry-failed` to requeue the affected chunks.

### Only `trades` and `ohlcv-1m` schemas are supported
Adding support for other Databento schemas (e.g. `mbp-1`, `tbbo`, `ohlcv-1d`) requires:
1. A new column map in `loader.q`
2. A new schema table in `schema.q`
3. A new CSV parser function (`readXxxCSV`)

---

## Cost Safeguard

The `BACKFILL_MAX_COST_USD` cost guard (default $50) calls the Databento metadata API before each batch submission. If Databento returns an API error for that call (e.g. unknown dataset, invalid symbol type), the run is aborted with a `RuntimeError` — the job is never submitted without a cost estimate.

To disable the cost guard entirely (e.g. for a trusted internal dataset), set `BACKFILL_MAX_COST_USD=0`.

Always use `--dry-run` first when working with a new date range or symbol list to see the cost estimate before committing.

---

## Scalability

### Chunk volume scales with symbol × date count
Each chunk is one symbol × one exchange × one day. A request for 100 symbols over 252 trading days produces 25,200 Databento batch jobs. Large requests should use `--dry-run` first to preview the job count and cost before submitting.

### `--status` reads all job records on every call
`./bin/backfill --status` loads and deserialises the full `staging/metadata/backfill_jobs` kdb table on every call. For large backfills with thousands of chunks this involves spawning a q subprocess and parsing the full table. There is currently no indexing or caching.

---

## Concurrency

### Partition write lock is not safe on network filesystems
`acquireWriteLock` in `loader.q` uses `mkdir` as an atomic lock. POSIX guarantees atomicity only on local filesystems. On NFS or CIFS, two processes can both succeed, potentially corrupting a partition. Keep the HDB on local storage.

### Python-side flock serialises loader invocations on a single host
The `fcntl.flock` call in `run_q_loader` serialises q loader invocations from concurrent orchestrator processes on the same host, preventing concurrent `.Q.dpft` calls on the same partition. The `REQUEST_ID` env var passed to each q loader invocation ensures each process only loads its own manifests, preventing cross-run data contamination in the shared staging directory. Neither mechanism protects against two separate hosts writing to the same HDB simultaneously.

---

## Adjustment Factors

### Factors are only updated for recently-backfilled symbols
`ref_ingest.py` runs automatically after each backfill, but only fetches and updates factors for the symbols included in that request. If a symbol was last backfilled months ago and a corporate action has occurred since, its adjustment factors will not be updated until it appears in a new backfill request. There is no background process that keeps factors current for all symbols in the HDB.

To manually refresh factors for a symbol without downloading new market data:
```bash
source ../venv/bin/activate
python code/reference/ref_ingest.py --symbols AAPL --start 2020-01-01 --end $(date +%Y-%m-%d)
```

### Adjustment factor gaps between non-contiguous backfill windows
Factors are only computed and stored for dates that appear in the HDB or the current backfill request. If you backfill a symbol for two separate date ranges with a gap between them (e.g. Jan 1–5 and Jun 1–5), there will be no adjustment factor rows for the dates in between. A query spanning that gap will silently use `cumulative_factor=1.0` for the missing dates, which is only correct if no corporate actions occurred in the gap period. A split during the gap would produce wrong adjusted prices for those dates with no warning.

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
