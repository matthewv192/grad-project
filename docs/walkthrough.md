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
q -q
```
Then in q: `\l hdb` (loading the HDB changes the working directory, so any additional scripts such as `adjlib.q` must be loaded before this line).

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

Chunks run in a `ThreadPoolExecutor` (default: 12 workers). The download phase is the bottleneck — it's I/O-bound and network-bound — so parallelism here gives a real speedup. The q loader is invoked **once** after all chunks complete, because concurrent `.Q.dpft` calls on the same partition would corrupt the HDB. A `fcntl.flock` file lock enforces this even if multiple orchestrator processes are running simultaneously.

#### 3.3.5 Pre-Flight HDB Check

Before submitting any Databento API jobs, the orchestrator runs a compact q one-liner against the HDB to find which of the requested dates already have data for the target `(schema, exchange)` pair. Those dates are removed from the chunk list before any cost estimation or API submission — so re-running a command for data that already exists costs nothing and produces a clear `already in HDB — skipping` message rather than a silent no-op.

This check is exchange-aware: loading XNAS.ITCH data for a date that already has XNYS.PILLAR data (but not XNAS.ITCH) correctly identifies that date as still needing to be loaded.

#### 3.3.6 CLI Modes

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
- `processManifests` — scans the manifest directory, filters to only the current run's manifests using the `REQUEST_ID` env var (preventing cross-contamination between concurrent parallel runs that share the same staging directory), skips anything already `verified` in the job store (archiving its manifest file), groups the rest by `(date, schema)`, and dispatches to `loadChunkBatch`

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

---

### 4.1 The Initial Prompting Strategy

The project did not start with "write me a backfill pipeline." It started with a deliberately constrained prompt:

> *"I am going to give you a set of project requirements. Before you write a single line of code, ask me every clarifying question you need to answer in order to build this correctly."*

Claude raised questions covering:
- Which Databento datasets and schemas were required (`XNAS.ITCH`, `trades` and `ohlcv-1m`)
- Which kdb+ version (kdb+5, not kdb+4 — the difference matters for several language behaviours)
- Whether this should be a TorQ package in its own repo or modifications to TorQ core (answer: separate package — critical for separation of concerns)
- What "production-ready" meant in this context (idempotency, crash recovery, cost safeguards, and test coverage were all called out explicitly)
- That clarity and documentation were as important as functionality, since the project was being built by graduates

This approach front-loaded the specification work rather than discovering misalignments after code had been written. The existing directory tree — TorQ install location, empty `grad-project/` folder, Python environment — was also shared before any code was generated, so Claude understood the actual deployment environment.

---

### 4.2 Milestone-Based Build

Claude produced a structured build plan splitting the project into five milestones, each independently executable and testable before the next began:

| Milestone | Scope |
|---|---|
| **M0** | Package skeleton: directory structure, `setenv.sh`, `schema.q`, `config/process.csv`, `settings.q` |
| **M1** | Full pipeline: `orchestrator.py` (submit → poll → download → manifest), `loader.q`, `manifest.q`, basic test suite |
| **M2** | Hardening: chunking, kdb job store persistence, idempotency, retry with exponential backoff, cost safeguard |
| **M3** | Reference data: `ref_ingest.py`, `ref_tables.q`, `adjlib.q`, adjustment tests |
| **M4** | Multi-exchange: `exchange` column in schema, merge logic in `loadChunkBatch`, `unenumAll` |

The milestone structure was important because it meant working, testable code existed at every stage. The common failure mode with AI-assisted development is generating everything at once and ending up with a system where nothing has ever run and errors are impossible to isolate. Completing and testing M1 before starting M2 eliminated entire classes of debugging problem.

---

### 4.3 Persistent Context — SKILL.md and Session Memory

A major challenge with multi-session AI-assisted development is context loss. Two mechanisms were used to address this:

**SKILL.md** — a 426-line kdb+/q developer reference document created at the start of the project and loaded into every Claude session. It covers:
- q language fundamentals (evaluation order, type system, null/infinity handling)
- HDB partition patterns (`.Q.dpft`, `.Q.en`, `.Q.chk`, attribute application)
- TorQ-specific conventions (`.lg.o`/`.lg.e` logging, `config/process.csv`, timer)
- Error-prone areas specific to kdb+5 (e.g. `getenv` vs `.z.e`, symbol enumeration after partition writes, the multiline closing brace parser issue)
- Project-specific coding standards (always comment non-obvious logic, use protected evaluation in all I/O code)

Without SKILL.md, Claude would periodically revert to kdb+4 idioms, use `.z.e` to read environment variables (which doesn't work in kdb+5), or write `q hdb` to load the HDB (which changes the working directory in a way that breaks subsequent script loads). SKILL.md prevented these from being re-introduced across sessions.

**Session memory** — Claude Code maintains a persistent memory file (`MEMORY.md`) that accumulates project state, architectural decisions, and kdb+5 gotchas discovered during development. This meant each new session started with full context: known bugs, file layout changes, wiring decisions, and active gaps. Without this, every session would begin with re-explaining the architecture.

---

### 4.4 Iterative Debugging — Specific Examples

The most important part of the workflow was not prompting Claude to write code — it was verifying the output and feeding failures back precisely. Several bugs required real domain knowledge to diagnose:

**`.Q.dpft` requires a global table, not a local variable**

An early version of `loadChunkBatch` passed a local variable to `.Q.dpft`:
```q
data: readTradesCSV[...];
.Q.dpft[hdbDir; partDate; `sym; data]   / WRONG — writes empty schema table
```
This compiled and ran without error but wrote an empty table to the HDB. The bug only manifested when querying the HDB and finding all partitions empty. The fix — always set the global first:
```q
`trades set data;
.Q.dpft[hdbDir; partDate; `sym; `trades]
```
This is not obvious from the kdb+ documentation and would not have been caught by a code review that only read the source.

**Enumerated symbol columns after `.Q.dpft`**

When the multi-exchange merge feature was added, existing HDB partitions would come back with integer values (12, 13, ...) instead of symbol names after a second exchange was loaded. Root cause: `.Q.dpft` enumerates **all** symbol columns (not just `sym`), converting them to type-20h integer references into the shared `sym` file. When merging, the existing partition's columns were being concatenated as integer lists with the new data's plain symbol lists, corrupting the result.

Fix: the `unenumAll` function added to `loader.q` detects all type-20h columns before the merge and converts them back to plain symbols:
```q
unenumAll:{[t] @[t; where 20h = type each flip t; {`$string x}] }
```
This kind of bug — correct at the syntax level, silently wrong at the data level — is exactly what makes kdb+ partition writes risky without deep familiarity with the type system.

**`};` at column 0 causes silent parse failure in kdb+5**

A multi-line function's closing brace at the start of a line is misparsed in kdb+5 when loaded via `\l`. The function body ends early but no error is raised at load time — the function simply runs with wrong behaviour. This manifested as loader failures only on specific input patterns. Fix: always indent the closing brace by at least one space. This is undocumented behaviour specific to kdb+5 script loading.

**`get` on a splayed partition suppresses the date column**

When reading an existing HDB partition to merge with new data:
```q
existing: get hsym `$string[hdbDir],"/",string[partDate],"/trades"
```
`get` returns the table **without** the `date` column — it is the partition key, implied by the directory. When concatenating with new data that includes `date`, this caused a `'mismatch` error. Fix: `update date:partDate from existing` before merging.

**Parallel-run race condition**

During multi-exchange testing, running two `./bin/backfill` invocations simultaneously — one for XNAS.ITCH, one for XNYS.PILLAR — caused each q loader subprocess to scan the entire `staging/metadata/manifests/` directory and process all manifests, not just its own run's. This produced partial writes: some symbols ended up in the HDB from the wrong run's loader, some were skipped entirely. The bug was invisible until the HDB was queried and symbol counts didn't match expectations.

Fix: Python now passes `REQUEST_ID` as an environment variable to the q loader subprocess, and `manifest.q`'s `processManifests` filters to only the manifests matching that ID before doing any work:
```q
reqId: getenv `REQUEST_ID;
if[count reqId;
    matchIdx: where (`$reqId) = {x`request_id} each parsed;
    parsed: parsed matchIdx;
    validPaths: validPaths matchIdx
];
```

---

### 4.5 Requirements Audit

Midway through the project, a structured audit was conducted: the original requirements document was reviewed line by line against the actual implementation to find gaps between what was specified and what was built.

Two gaps were found:

1. **`ref_corp_actions` not append-only** — the requirements specified point-in-time correctness for reference data (so a backtest run in January should use only factor data available in January, not revisions ingested in February). The `ref_corp_actions` table was being overwritten on each ingest rather than appended with a `loaded_at` timestamp. Fix: changed `_upsert_csv` to `_append_csv` and added a `loaded_at` column to both the schema and the ingest script.

2. **Job store was still JSON files** — the requirements specified a kdb binary table for job tracking. The implementation had a `backfill_jobs` schema defined in `schema.q` but the actual job store was writing individual JSON files per chunk to `staging/metadata/jobs/`. Fix: `jobstore.q` was written to manage a kdb binary table at `staging/metadata/backfill_jobs`, and Python's `JobStore` class was replaced with `KdbJobStore` which communicates with it via a q subprocess.

The audit approach — treating it as a formal gap analysis rather than "does it seem to work" — surfaced issues that would have been invisible in normal testing.

---

### 4.6 Architectural Decisions That Were Human-Directed

Claude consistently deferred to explicit direction on architectural choices:

- **Python/q boundary** — an early draft had Python doing CSV parsing. This was redirected: all parsing and type enforcement happens in q, at the point of writing to the HDB. This ensures that a malformed value causes a controlled failure in the loader, not a silent type mismatch in the database.

- **Exchange column from the start** — the `exchange` column was specified in `schema.q` at M0, anticipating the multi-exchange milestone. Claude's initial schema draft omitted it. Adding it retroactively would have required a migration script for all existing partitions.

- **Idempotency as a requirement, not a feature** — Claude's M1 draft assumed a clean run. The job store, checksum verification, and HDB pre-flight check were specified as non-negotiable from the beginning, not added later.

- **Scope control** — Claude periodically suggested additions: a monitoring dashboard, a REST API for job status, a database-backed configuration system. Each was declined. The requirement was a well-understood, maintainable pipeline — not a maximally-featured one.

- **Chunk size tradeoff** — the initial implementation used 1 symbol per chunk (finest retry granularity). After reviewing the Databento API overhead per job, this was revised to a default batch size of 10 symbols per chunk — reducing API round-trips by 10x for typical runs while keeping retry granularity acceptable.

---

### 4.7 What This Demonstrates

Using Claude effectively in this project required:

1. **Clear upfront specification** — the clarifying-questions prompt prevented misalignments from being baked into early code
2. **Domain knowledge to evaluate output** — a generated kdb+5 script that ignores enum column types after `.Q.dpft` looks syntactically correct but corrupts the HDB silently on a multi-exchange merge
3. **Incremental testing discipline** — running tests after each milestone before proceeding to the next, rather than testing the whole system at the end
4. **Knowing when to override** — Claude's default behaviour is to add safety checks, fallbacks, and generality. Several times the simpler, more direct approach was the correct one
5. **Persistent context management** — SKILL.md and session memory meant the AI's behaviour was consistent across many sessions rather than drifting back to defaults

The AI accelerated the build significantly: boilerplate, test generation, documentation, and iterative bug-fixing were all substantially faster. The architectural decisions, the correctness of the kdb+ partition write logic, and the production-readiness requirements were human-directed throughout.
