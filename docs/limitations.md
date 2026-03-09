# Known Limitations

## Implementation Status

All milestones (M0–M4) are complete. The following items remain stubs by design:

| Component | Status | Notes |
|---|---|---|
| `ref_ingest.py` | Generates synthetic data only | Replace `generate_*` functions with a real provider (Bloomberg, Refinitiv, etc.) |
| `ref_tables.q` — live queries | Reads from CSVs, not a live feed | Sufficient for batch backfill; add a real-time source for production |
| `adjlib.q` — `applyAdj` / `getAdjustedClose` | Return adjusted values from synthetic factors | Factors must be real for production use |

---

## Data Completeness

### Multiple CSVs per Databento job — only first is used
`run_chunk` in `orchestrator.py` takes `csv_paths[0]` when a download returns multiple files. Databento occasionally splits a single job into multiple CSVs for very large date ranges. If this happens, only the first file is staged and loaded; the rest are silently ignored.
**Workaround:** Keep chunk sizes small (default 10 symbols × 1 day) to avoid multi-file deliveries.

### Empty partitions not HDB-cross-filled
When Databento returns a zero-row CSV (holiday, halt, no activity), `loadTrades`/`loadOhlcv` log a warning and return without writing to the HDB. The `.Q.chk` call at the end of `runLoader` will create an empty placeholder for the missing table, but the date partition itself is never created, so `date` values returned by cross-fill queries will have gaps rather than empty rows.

### Quality checks run on in-memory data only
`runQualityChecks` in `quality.q` operates on the freshly-read table before `.Q.dpft` writes it. After `.Q.chk` cross-fills missing tables with empty placeholders, no quality check is re-run on those synthetic rows.

### Synthetic reference data is incompatible with real Databento IDs
`ref_ingest.py` assigns `instrument_id` values starting at 1000 for the five test symbols. Real Databento instrument IDs are exchange-assigned integers that differ. Using the stub reference data with real backfill output will cause `resolveSymbol` / `resolveInstrumentId` lookups to return null for all real instruments.

---

## Data Correctness

### Timestamps assumed UTC — non-UTC process timezone silently corrupts data
`readTradesCSV` and `readOhlcvCSV` parse Databento's ISO 8601 timestamps with `"P"$ts`. kdb+ interprets the resulting timestamps in the process's local timezone. If `q` is started in a non-UTC timezone (or during a DST transition), all stored timestamps will be off by the TZ offset with no error or warning.
**Mitigation:** Always start the loader process with `TZ=UTC` set in the environment.

### `infer_date_from_filename` silently returns an empty string on parse failure
If Databento changes their filename format, the regex in `orchestrator.py` returns `""`. The manifest is then written with `date: ""`, causing the q loader to fail with an opaque type error rather than a meaningful message about the filename.

### Column intersection silently drops unexpected columns
`(cols[trades] inter cols raw)#raw` in `loader.q` retains only columns that exist in both the schema and the CSV. If Databento adds or removes columns, the mismatch is silently absorbed. New required columns would be missing from the written partition without any error.

### Type casting errors crash the loader mid-partition
The `update ... from raw` casts in `readTradesCSV` / `readOhlcvCSV` (e.g., `` `timestamp$time ``, `` `float$price ``) are not wrapped in protected evaluation. A single malformed value in a large CSV signals an error that aborts the current `loadChunk` call, potentially leaving the partition lock directory behind (stale lock) if the error propagates before `releaseWriteLock` runs.

---

## Financial Risk

### Cost estimation failure disables the cost safeguard
`estimate_cost` in `orchestrator.py` catches all exceptions and returns `0.0`. The guard `if MAX_COST_USD > 0 and cost > MAX_COST_USD` will never fire when estimation fails, allowing any-cost job to be submitted. This can happen if the Databento metadata API is temporarily unavailable.

---

## Scalability

### Row counting scans the entire CSV twice
`count_csv_rows` in `orchestrator.py` reads the file line-by-line to count rows. For multi-million-row trade files this is a second full-file pass after the download. For very large backfills this adds significant latency. A faster alternative is `wc -l` via `subprocess`.

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

### No duplicate `--request-id` detection
If the same `--request-id` is supplied on two separate invocations, the second run will overwrite job records from the first. There is no uniqueness check or collision warning.

### Config is locked at process start
`config/settings.q` values are compiled into q at load time. Changing an environment variable after the process has started has no effect. The loader must be restarted to pick up config changes.

### Metrics file gaps when orchestrator crashes before metrics are written
`updateMetrics` in `loader.q` silently skips if the per-chunk JSON file doesn't exist. If the Python orchestrator crashes before writing the initial metrics file, the q loader will complete successfully but leave a gap in the metrics for that chunk.

---

## Data Scope

- Any Databento dataset that supports the `trades` or `ohlcv-1m` schema is supported. The dataset is stored as a column in the HDB tables and in the symbology map. Pass `--dataset GLBX.MDP3` (or any valid dataset identifier) to the orchestrator.
- Only one dataset per HDB partition is supported. Mixing e.g. XNAS.ITCH and GLBX.MDP3 trades in the same partition date is not supported — run separate HDB instances per dataset or use separate table names.
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
- Multi-dataset single HDB — partitioned tables are not namespaced by dataset; loading XNAS.ITCH and OPRA.PILLAR into the same HDB would require schema changes
