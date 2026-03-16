# TorQ Databento Backfill Package — Technical Walkthrough

**Audience:** CTO & Head of Software Engineering
**Format:** 20–30 minute verbal walkthrough with code reference
**Repo:** `github.com/matthewv192/grad-project` (branch: `dev`)

---

## 1. What the Project Does and Why

The goal was to build a production-grade historical data backfill pipeline as a standalone TorQ package. The pipeline connects to Databento — a financial market data vendor — downloads historical trade and OHLCV data, and writes it into a kdb+ Historical Database (HDB) in TorQ's standard partitioned format.

The reason this is non-trivial:

- **Scale:** A backfill of even a few hundred symbols over a few months generates thousands of individual API jobs. Each one can fail independently, so you need a proper state machine, not a simple loop.
- **Data integrity:** kdb+ HDB partitions are splayed tables on disk. Writing them incorrectly, or concurrently, corrupts the entire database. The loading step needs to be careful, locked, and idempotent.
- **Operational resilience:** If the process crashes halfway through, it must be resumable without re-downloading data or duplicating rows in the HDB.

The project was built in four milestones, each building on the last, and is fully tested with both unit and integration tests.

---

## 2. High-Level Architecture

The most important design decision in the whole project is the **Python/q split**.

```
Python (orchestrator.py)          q (loader.q / manifest.q)
--------------------------------  --------------------------------
Databento API calls               CSV parsing + type enforcement
Download management               kdb+ HDB partition writes
Job store (state machine)         Data quality checks
Chunking + retry logic            HDB reload notification
Cost safeguard                    Symbology map maintenance
Metrics collection
        |                                   ^
        |   CSV files + JSON manifests      |
        +------- staging/ directory --------+
```

**Why this split?**

Python is the right tool for HTTP APIs, JSON, file system orchestration, and retry logic. q/kdb+ is the right tool for parsing columnar data with tight type control and writing splayed partitions correctly. Mixing them would mean either writing kdb+ in Python (fragile, loses type safety) or writing HTTP clients in q (painful). The two sides communicate entirely through files on disk — CSV data files and JSON manifest files — which means neither side needs to know anything about the other's internals, and you can test each side independently.

---

## 3. File-by-File Walkthrough

### 3.1 `setenv.sh` — Environment Bootstrap

**What it does:** Sets all environment variables needed by both Python and q processes before anything runs.

**Key variables set:**
- `TORQHOME` — path to the read-only TorQ framework
- `PACKAGEHOME` — root of this package
- `KDBHDB` — where the HDB lives on disk
- `STAGING_DIR` — where downloaded CSVs and manifests are written
- `DATABENTO_API_KEY` — the API credential (stored directly in `setenv.sh`)

**Why it matters:** Both Python and q need to agree on where the staging directory and HDB are. Rather than hardcoding paths, everything reads from environment variables so the package works in any environment without code changes. The script uses `${BASH_SOURCE[0]}` rather than `$0` so it resolves correctly whether sourced interactively or from another script.

**Usage:** The `bin/backfill` entrypoint sources `setenv.sh` automatically. Manual sourcing is only needed for interactive q sessions:
```bash
source setenv.sh
q hdb
```

---

### 3.2 `schema/schema.q` — Single Source of Truth for Table Structure

**What it does:** Defines every table in the system as an empty typed kdb+ table. All q scripts load this file first.

**Tables defined:**
- `trades` — individual trade records (time, sym, exchange, price, size, side, conditions, sequence, instrument_id)
- `ohlcv_1m` — one-minute OHLCV bars (time, sym, exchange, open, high, low, close, volume, instrument_id)
- `backfill_jobs` — job state machine tracking (used by the loader to check status)
- `ref_security_master`, `ref_corp_actions`, `ref_adj_factors`, `ref_symbology_map` — reference data tables

**Why one file?** If column types or ordering are defined in multiple places, a change to a schema requires finding and updating every place it's referenced. Centralising it means a single edit propagates everywhere. It also means any q process can load `schema.q` and immediately know the correct column types and order, which is critical for `.Q.dpft` (the kdb+ function that writes HDB partitions) to work correctly.

**Notable design choice:** The `exchange` column is present in both `trades` and `ohlcv_1m`. This was added to support multi-exchange data — the same symbol can trade on NASDAQ (XNAS.ITCH), NYSE (XNYS.PILLAR), and IEX (IEXG.TOPS) on the same date, and all of that should live in the same HDB partition without the rows colliding.

---

### 3.3 `orchestrator.py` — The Python Brain

This is the largest file and does the most work. It is a CLI tool with several modes.

#### 3.3.1 Chunking (`generate_chunks`)

A backfill request (e.g. 50 symbols, 30 days) is immediately broken into small independent units called **chunks**: one chunk = one day × one batch of symbols (default batch size: 10).

```
50 symbols × 30 days ÷ 10 per batch = 150 chunks
```

**Why chunk at all?** Databento charges per job. Small jobs are cheaper to retry if they fail. A single large job that fails after 29 days of downloading wastes everything. With chunks, only the one failed day/batch needs to be resubmitted.

#### 3.3.2 Job Store (`JobStore`, `JobRecord`)

Every chunk has a `JobRecord` persisted to a kdb binary table at `staging/metadata/backfill_jobs`. The record tracks the full lifecycle:

```
submitted → running → downloaded → loaded → verified
                                         ↘ failed
```

Every state transition is written to disk **before** the action is taken. This means if the process crashes, on restart it reads the job store and knows exactly which chunks were completed, which were in-flight, and which failed. There is no need to re-query the Databento API or re-download data that already exists on disk.

`KdbJobStore` in `orchestrator.py` owns all reads and writes. Each operation spawns a q subprocess running `code/backfill/jobstore.q`; the JSON command is passed via the `JOBSTORE_CMD` environment variable and results are returned as JSON on stdout. A `threading.Lock` plus `fcntl.flock` serialise concurrent writes within and across processes.

#### 3.3.3 The Per-Chunk Pipeline (`run_chunk`)

For each chunk, the pipeline does:

1. **Check job store** — if already `loaded` or `verified`, skip immediately (idempotency)
2. **Resume check** — if previously downloaded, verify the SHA-256 checksum of the file on disk. If it matches, skip straight to manifest writing. If it doesn't match (file corrupted), re-download.
3. **Cost estimate** — before submitting to Databento, call their cost estimation API. If the estimated cost exceeds `BACKFILL_MAX_COST_USD` (default $50), abort. This prevents accidental runaway spend.
4. **Submit** — send the batch job to Databento, requesting CSV output with human-readable timestamps and prices (`pretty_ts=True`, `pretty_px=True`). Store the Databento job ID.
5. **Poll** — check every 10 seconds until the job is done (timeout: 1 hour).
6. **Download** — download the CSV(s) to `staging/<chunk_id>/`.
7. **Write manifest** — write a JSON manifest file describing the CSV (path, row count, checksum, date, schema). This is the handoff document to the q side.
8. **Mark loaded** — update the job store.

#### 3.3.4 Parallel Execution

Chunks run in a `ThreadPoolExecutor` (default: 4 workers). The download phase is the bottleneck — it's I/O-bound and network-bound — so parallelism here gives a real speedup. The q loader is invoked **once** after all chunks complete, because concurrent `Q.dpft` calls on the same partition would corrupt the HDB. A `fcntl.flock` file lock enforces this even if multiple orchestrator processes are running simultaneously.

#### 3.3.5 CLI Modes

```bash
# Normal run
python orchestrator.py --symbols AAPL,MSFT --start 2024-01-15 --end 2024-01-17

# Preview what would run and what it would cost, without submitting anything
python orchestrator.py --symbols AAPL,MSFT --start 2024-01-15 --end 2024-01-17 --dry-run

# See the status of all chunks in the job store
python orchestrator.py --status

# Retry all failed chunks (with exponential backoff)
python orchestrator.py --retry-failed

# Download data but don't write to the HDB yet
python orchestrator.py --symbols AAPL --start 2024-01-15 --end 2024-01-15 --download-only

# Run the q loader against already-staged manifests (useful for debugging)
python orchestrator.py --load-only
```

---

### 3.4 `manifest.q` — The Python/q Handoff

**What it does:** Reads the JSON manifest files written by Python, validates them, groups them by date and schema, then dispatches them to the loader in batches.

**Why batch by date and schema?** kdb+'s `.Q.dpft` writes a whole partition directory atomically. If you write each chunk separately, you'd be re-reading and re-merging the same partition file N times for N chunks with the same date. Batching by `(date, schema)` means each partition is written exactly once.

**Key functions:**

- `readManifest` — parses the JSON, normalises field names (e.g. Databento's `ohlcv-1m` → our `ohlcv_1m`), converts date strings to kdb+ date format
- `validateManifest` — checks the CSV file actually exists on disk, the schema is known, and the row count is positive
- `processManifests` — scans the manifest directory, skips anything already `verified` in the job store (archiving its manifest file to avoid re-processing on the next run), groups the rest, and dispatches to `loadChunkBatch`

---

### 3.5 `loader.q` — The q Side: Parsing, Writing, Merging

This is the most technically complex part of the codebase because it has to handle kdb+ internals directly.

#### 3.5.1 CSV Parsing (`readTradesCSV`, `readOhlcvCSV`)

Each function reads a Databento CSV using kdb+'s native `0:` operator with an explicit type string — one character per column:

```q
raw:(" P  J S FJI JS";enlist csv) 0: csvPath;
```

The space characters mean "skip this column" — Databento's CSV has 14 columns for trades but we only need 9. Reading only what we need is faster and means we don't have to worry about Databento adding columns in a future API version (it would just be skipped).

After reading, columns are renamed to match our schema using `xcol`, types are enforced explicitly (`update sym:`symbol$sym, time:`timestamp$time...`), and only the schema columns are kept.

#### 3.5.2 Writing to the HDB (`loadChunkBatch`)

This is where the data actually lands in kdb+. The key steps:

1. **Idempotency check** — before doing any work, check whether this exchange's data already exists in the target partition. If it does, skip. This is done by reading only the `exchange` column file on disk, not the whole table.

2. **Row count validation** — confirm the number of rows parsed from the CSV matches the manifest's declared row count. A mismatch means the file was corrupted in transit.

3. **Quality checks** — run `runQualityChecks` (duplicate keys, time ordering, null values in key columns). Fail the chunk rather than write bad data.

4. **Merge with existing partition** — if this is a new exchange being added to a date that already has data (e.g. XNYS data being added to a partition that already has XNAS data), the existing table is read from disk, unenumerated, and concatenated with the new data before writing.

5. **Write lock** — an atomic `mkdir`-based POSIX lock prevents two processes from writing the same partition simultaneously.

6. **`.Q.dpft`** — kdb+'s standard function for writing a splayed, partitioned table. It takes the HDB root, the partition date, the sort/attribute column (`sym`), and the table name. It creates the directory structure, writes each column as a separate binary file, and applies the `p#` (parted) attribute to `sym` for fast lookup.

7. **Post-write: `g#` on exchange** — after `.Q.dpft` completes, the `exchange` column gets a `g#` (grouped) attribute applied. This means filtering by exchange uses a dictionary lookup rather than a linear scan.

#### 3.5.3 `unenumAll` — A kdb+ Gotcha Worth Explaining

When `.Q.dpft` writes a partition, it enumerates all symbol columns — converting them from plain symbol lists to integer references into a shared `sym` file. This is how kdb+ achieves memory efficiency (one copy of each string, referenced everywhere). When merging a new exchange into an existing partition, you have to unenumerate (reverse this) before concatenating, otherwise kdb+ signals a type error. `unenumAll` handles this by detecting all type-20h (enumerated symbol) columns and converting them back to plain symbols before the merge.

---

### 3.6 `quality.q` — Data Quality Checks

Runs three checks on every batch before it is written to the HDB:

1. **Duplicate key check** — counts rows with the same natural key (`sym`, `time`, `exchange`, `sequence` for trades; `sym`, `time`, `exchange` for OHLCV). Legitimate tick data can have multiple trades at the same nanosecond for the same symbol (different market participants), which is why `sequence` is included — it's the exchange's own deduplication key.

2. **Time ordering check** — verifies the table is sorted by `sym`, `time`. If it isn't, `.Q.dpft` would write an unsorted partition and range queries would be slower or incorrect.

3. **Null check** — confirms that key columns (price, size, open/high/low/close) have no null values. Nulls in price data indicate a parsing failure.

If any check fails, the chunk is marked `failed` in the job store rather than written to the HDB. Bad data never touches the database.

---

### 3.7 `code/reference/ref_tables.q` — Reference Data

Loads static reference data CSVs into in-memory kdb+ tables:

- `ref_security_master` — maps symbols to instrument IDs, exchanges, currencies
- `ref_corp_actions` — stock splits, dividends, mergers (used for price adjustment); append-only with a `loaded_at` timestamp so the full revision history is preserved for point-in-time queries
- `ref_adj_factors` — cumulative adjustment factors per symbol per date, with a `loaded_at` timestamp on each row so multiple revisions can coexist (point-in-time queries)
- `ref_symbology_map` — auto-populated by the loader: maps Databento instrument IDs to normalised symbols

The `resolveSymbol` and `resolveInstrumentId` functions use kdb+'s `aj` (asof join) semantics — finding the correct mapping as of a given date — which handles the fact that ticker symbols and instrument IDs can change over time.

---

### 3.8 `code/adjlib/adjlib.q` — Price Adjustment Library

Applies corporate action adjustments to OHLCV data so that a stock split doesn't create an artificial price discontinuity in a time series.

**Convention:** A 2:1 split (share count doubles, price halves) is stored as `cumulative_factor = 0.5` for all pre-split dates. Multiplying the historical price by 0.5 brings it into post-split (current) terms, giving a continuous series.

**Two adjustment methods:**
- `backward` — all prices in current (post-split) terms. The standard for most quant use cases.
- `forward` — all prices in historical terms. Useful when you need to match raw historical records.

**Point-in-time factor selection:** `getAdjustedClose` takes an optional fifth `asOf` timestamp. When provided, only factor rows where `loaded_at <= asOf` are used, and the most recent revision within that window is selected per `(sym, date)`. Pass `0Np` (null) to use the latest available revision. This prevents look-ahead bias in backtesting — a factor revision ingested in February won't affect a simulation that was run in January.

**Key function:** `applyAdj` uses a left join (`lj`) to attach the adjustment factor to each bar by `(sym, date)`, fills missing factors with 1.0 (no adjustment), then applies the multiplication/division vectorially across the whole table in one pass.

---

### 3.9 Testing

The test suite runs with a single script:

```bash
bash scripts/run_tests.sh
```

**q tests** (8 files in `tests/`):
- `test_schema.q` — schema table types and column presence
- `test_manifest.q` — manifest parsing, validation, directory scanning
- `test_loader.q` — CSV parsing, merge logic, HDB write cycle
- `test_quality.q` — all three quality check functions
- `test_adj.q` — adjustment factor application, both methods
- `test_ref_tables.q` — reference CSV loading
- `test_symbology.q` — symbol resolution functions
- `test_integration.q` — end-to-end query against a populated HDB

**Python tests** (63 tests across 2 files):
- `test_orchestrator.py` — chunk generation, job store, manifest writing, cost guard, retry logic, checksum handling
- `test_metrics.py` — metrics recording and summary generation

---

## 4. How Claude Was Used

**This was explicitly encouraged by the company as part of the graduate project.** The goal was not just to build the pipeline, but to learn how to effectively direct an AI coding assistant to produce production-quality output.

### The Workflow

**Step 1 — Problem specification**

The project requirements were pasted into Claude (web interface) and a structured prompt was requested. Before generating anything, Claude was asked to raise clarifying questions to ensure the build would be correct. Questions and answers covered:
- Which Databento datasets and schemas were required (XNAS.ITCH, trades and ohlcv-1m)
- Which kdb+ version (kdb+5)
- That this should be a **separate TorQ package** in its own repo, not modifications to the TorQ core
- That the process was being built by graduates, so clarity and documentation were as important as functionality

**Step 2 — Directory structure**

The existing directory tree (TorQ install, MCP server, the empty grad-project folder) was shared with Claude so it understood the environment before writing any code.

**Step 3 — Milestone-based prompt**

Claude produced a structured prompt that split the build into four milestones, each independently executable and testable:

- **M0** — Package skeleton: directory structure, `setenv.sh`, `schema.q`, `config/process.csv`, `settings.q`
- **M1** — Full pipeline: `orchestrator.py` (submit → poll → download → manifest), `loader.q`, `manifest.q`, basic test suite
- **M2** — Hardening: chunking, job store persistence, idempotency, retry logic, cost safeguard
- **M3** — Reference data: `ref_ingest.py`, `ref_tables.q`, `adjlib.q`, adjustment tests
- **M4** — Multi-exchange support: `exchange` column, merge logic in `loadChunkBatch`, `unenumAll`

This milestone structure was important because it meant at the end of each milestone there was working, testable code. It prevented the common failure mode of generating everything at once and ending up with a system that is impossible to debug because nothing has ever run.

**Step 4 — SKILL.md**

A kdb+/q developer SKILL.md file was created to give Claude persistent context about kdb+5 idioms, TorQ conventions, and known gotchas (such as the `.z.e` / `getenv` distinction in kdb+5, the `multiline closing brace` parser issue, and the `.Q.dpft` global table requirement). This reduced repeated mistakes across milestones.

### How I Guided the Process

The Claude-assisted workflow was iterative, not passive. Specific examples of direction given:

- **Pushed back on the Python/q boundary** — an early draft had Python doing CSV parsing. I directed it to move all parsing into q so that type enforcement happened at the point of writing to the HDB, not before.
- **Required idempotency from the start** — the job store and checksum logic were explicitly requested as non-negotiable requirements, not optional enhancements.
- **Specified the exchange column** — the `exchange` column in the schema was my direction, anticipating the multi-exchange milestone from the beginning rather than retrofitting it later.
- **Reviewed every generated test** — tests were run after each milestone, failures were fed back to Claude, and the root cause was investigated before accepting the fix. Claude would sometimes propose workarounds; I pushed back where the underlying issue needed to be properly understood and fixed.
- **Controlled scope** — several times Claude would suggest adding features (e.g. a full monitoring dashboard, database-backed job store). These were redirected: the requirement was a clean, well-understood pipeline, not a maximally-featured one.

### What This Demonstrates

Using Claude effectively in this project required:
1. Clear upfront specification (garbage in, garbage out applies to prompts just as much as data)
2. Domain knowledge to evaluate the output — a generated kdb+5 script that doesn't account for enum column types after `.Q.dpft` looks correct but corrupts the HDB on a multi-exchange merge
3. Discipline to test incrementally and not accept code that "looks right"
4. Understanding when to override Claude's suggestions in favour of simpler or more correct approaches

The AI accelerated the build significantly, particularly for boilerplate, documentation, and test generation. The architectural decisions, the correctness of the kdb+ partition write logic, and the production-readiness requirements were all human-directed.
