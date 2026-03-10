# Known Limitations

## Implementation Status

All milestones (M0–M4) are complete, including multi-exchange support. The following items remain stubs by design:

| Component | Status | Notes |
|---|---|---|
| `ref_ingest.py` | Generates synthetic data only | Replace `generate_*` functions with a real provider (Bloomberg, Refinitiv, etc.) |
| `ref_tables.q` — live queries | Reads from CSVs, not a live feed | Sufficient for batch backfill; add a real-time source for production |
| `adjlib.q` — `applyAdj` / `getAdjustedClose` | Return adjusted values from synthetic factors | Factors must be real for production use |

---

## Data Completeness

### Multiple CSVs per Databento job — each file gets its own manifest
`run_chunk` in `orchestrator.py` now writes one manifest per CSV when a download returns multiple files. Extra files receive chunk IDs suffixed `_part2`, `_part3`, etc., and a warning is logged. All CSV paths are stored in the job record so that a resumed run re-writes manifests for every part, not just the primary file.

### Empty partitions not HDB-cross-filled
When Databento returns a zero-row CSV (holiday, halt, no activity), `loadTrades`/`loadOhlcv` log a warning and return without writing to the HDB. The `.Q.chk` call at the end of `runLoader` will create an empty placeholder for the missing table, but the date partition itself is never created, so `date` values returned by cross-fill queries will have gaps rather than empty rows.

### Quality checks run on in-memory data only
`runQualityChecks` in `quality.q` operates on the freshly-read table before `.Q.dpft` writes it. After `.Q.chk` cross-fills missing tables with empty placeholders, no quality check is re-run on those synthetic rows.

### Synthetic reference data is incompatible with real Databento IDs
`ref_ingest.py` assigns `instrument_id` values starting at 1000 for the five test symbols. Real Databento instrument IDs are exchange-assigned integers that differ. Using the stub reference data with real backfill output will cause `resolveSymbol` / `resolveInstrumentId` lookups to return null for all real instruments.

---

## Data Correctness

### Timestamps assumed UTC — q loader process is now forced to UTC
`readTradesCSV` and `readOhlcvCSV` parse Databento's ISO 8601 timestamps with `"P"$ts`. The orchestrator now sets `TZ=UTC` in the environment passed to the q subprocess, so loader timestamps are always UTC regardless of the host system timezone.

### `infer_date_from_filename` raises on parse failure
If Databento changes their filename format and the regex no longer matches, `orchestrator.py` now raises `ValueError` with a descriptive message instead of writing a manifest with `date: ""`. The chunk is marked `failed` and can be retried once the filename issue is diagnosed.

### Column intersection logs a warning on schema drift
`readTradesCSV` and `readOhlcvCSV` in `loader.q` now log an error listing any schema columns that are absent from the delivered CSV before applying the `inter` filter. Added Databento columns are still silently dropped (as before), but missing required columns are flagged explicitly.

### Type casting errors crash the loader mid-partition
The `update ... from raw` casts in `readTradesCSV` / `readOhlcvCSV` are not wrapped in protected evaluation. A single malformed value in a large CSV signals an error that aborts the current `loadChunk` call, potentially leaving the partition lock directory behind (stale lock) if the error propagates before `releaseWriteLock` runs.
**Recovery:** Clear stale locks with `find hdb -name ".*.lock" -type d -exec rmdir {} +`, reset the chunk status to `downloaded` in the job store, and rerun.

---

## Financial Risk

### Cost estimation failure handling
`estimate_cost` in `orchestrator.py` now distinguishes Databento API errors (e.g. unknown dataset, invalid symbol) from unexpected failures (network timeout, SDK bug). API errors return `0.0` and log a warning, disabling the safeguard for that chunk. Unexpected errors are re-raised and abort the run, ensuring the cost guard is never silently bypassed by an infrastructure fault.

---

## Scalability

### Row counting uses `wc -l`
`count_csv_rows` in `orchestrator.py` delegates to `wc -l` via `subprocess` instead of reading the file line-by-line in Python. For large trade files this is significantly faster as it avoids decoding and object creation per line.

### Chunk size not validated against Databento API limits
`BACKFILL_CHUNK_SIZE` (default 10 symbols) is not checked against Databento's actual batch submission limits. If the limit changes or is lower than expected, jobs will fail at submission with a generic API error rather than a pre-submission validation message.

---

## Concurrency

### mkdir-based partition lock is not safe on NFS
`acquireWriteLock` in `loader.q` uses `system "mkdir"` as an atomic lock. POSIX guarantees atomicity only on local filesystems. Over NFS (or CIFS), `mkdir` is not atomic and two processes can both succeed, defeating the lock. If `KDBHDB` points to a network-mounted path, concurrent loaders can corrupt partitions.
**Mitigation:** Keep the HDB on local storage, or replace the lock with a kernel file lock (e.g., `flock`).

### `fcntl.flock` only serialises loaders within one machine
The Python-side exclusive lock in `run_q_loader` prevents concurrent loader invocations from the same orchestrator process on the same host. It does not prevent two separate machines from running the loader against the same shared HDB simultaneously.

---

## Operations

### No intraday partition updates
`.Q.dpft` writes an entire partition atomically. There is no mechanism to append late-arriving trades to an existing date partition. A late trade for an already-loaded date requires deleting the partition, merging in the new row, and re-loading.

### Duplicate `--request-id` detection
When an explicit `--request-id` is provided, the orchestrator checks the job store at startup. If existing records are found with a different symbol set or date range, the run aborts with a clear error. If the parameters match, the run proceeds as an idempotent resume with a warning logged.

### Config is locked at process start
`config/settings.q` values are compiled into q at load time. Changing an environment variable after the process has started has no effect. The loader must be restarted to pick up config changes.

### Metrics file written at chunk start
`run_chunk` now writes a stub metrics JSON immediately after marking `total_start`, before any API call or file I/O. If the orchestrator crashes mid-run, a partial record is present on disk and will be overwritten on the next retry, so no chunk is ever invisible in the metrics directory.

---

## Data Scope

- Any Databento dataset that provides `trades` or `ohlcv-1m` schema is supported. Tested datasets include `XNAS.ITCH`, `XNYS.PILLAR`, `IEXG.TOPS`, and `EQUS.MINI`.
- Multiple exchanges can coexist in the same HDB partition date. Each row carries an `exchange` column identifying its source. Exchange-aware idempotency ensures a `(date, exchange)` pair is never loaded twice.
- Extended hours trades are included in Databento's `trades` schema. Filter on `time` if you only want regular session data.
- Only `trades` and `ohlcv-1m` schemas are supported. Adding `mbp-1`, `tbbo`, or other Databento schemas requires new column maps in `loader.q` and new schema tables in `schema.q`.

---

## TorQ Integration

- The package uses TorQ environment variable conventions but is not started via `torq.sh`. Processes are run manually or via the shell scripts in `scripts/`.
- `config/process.csv` defines the `hdb` and `loader` process entries but they are not wired into a full TorQ gateway stack.
- The HDB reload notification in `runLoader` connects to `KDBHDBPORT` (default 6010) and sends `system "l ."`. This is fire-and-forget — if the HDB is not running, the notification is silently skipped.

---

## Cost Considerations

- Databento charges per gigabyte of data returned. XNAS.ITCH `trades` data can be large for liquid symbols over long date ranges.
- The `BACKFILL_MAX_COST_USD` safeguard (default $50) blocks requests that exceed the threshold **before** submitting, but will not fire if the cost estimation API call fails (see Financial Risk above).
- Always use `--dry-run` first when testing with a new date range or symbol universe.
- OHLCV data is significantly cheaper than tick-level trades data for the same period.

---

## Not Supported

- Real-time / live data ingestion — batch only
- Non-equity asset classes (futures, options, FX) — untested
- Databento streaming API — not used
- Windows — shell scripts are bash only; use WSL
