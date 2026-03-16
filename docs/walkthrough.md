# TorQ Databento Backfill Package — Technical Walkthrough

**Audience:** CTO & Head of Software Engineering
**Format:** 15 minute verbal walkthrough with code reference
**Repo:** `github.com/matthewv192/grad-project` (branch: `dev`)

---

## 1. What the Project Does and Why

A production-grade historical data backfill pipeline built as a standalone TorQ package. It connects to Databento, downloads historical trade and OHLCV data, and writes it into a kdb+ HDB in TorQ's standard partitioned format.

Why this is non-trivial:

- **Scale:** Even a few hundred symbols over a few months generates thousands of API jobs, each of which can fail independently
- **Data integrity:** kdb+ HDB partitions are splayed tables on disk — writing them incorrectly or concurrently corrupts the database
- **Resilience:** A crash halfway through must be resumable without re-downloading data or duplicating rows

---

## 2. Architecture

The core design decision is the **Python/q split**:

```
Python (orchestrator.py)          q (loader.q / manifest.q)
--------------------------------  --------------------------------
Databento API calls               CSV parsing + type enforcement
Download management               kdb+ HDB partition writes
Job store (state machine)         Data quality checks
Chunking + retry logic            Symbology map maintenance
Cost safeguard + metrics
        |                                   ^
        |   CSV files + JSON manifests      |
        +------- staging/ directory --------+
```

Python handles HTTP, JSON, and orchestration. q handles columnar parsing and partition writes. They communicate entirely through files on disk — neither side needs to know the other's internals, and each side can be tested independently.

---

## 3. Component Walkthrough

### 3.1 `setenv.sh`

Sets all environment variables (`TORQHOME`, `KDBHDB`, `STAGING_DIR`, `DATABENTO_API_KEY`) for both Python and q processes. Uses `${BASH_SOURCE[0]}` so it resolves correctly whether sourced interactively or from a script. `bin/backfill` sources it automatically — manual sourcing is only needed for interactive q sessions.

### 3.2 `schema/schema.q`

Single source of truth for every table. All q scripts load this first — centralising schemas means a column change propagates everywhere and `.Q.dpft` always receives correctly-typed input. The `exchange` column in both `trades` and `ohlcv_1m` enables XNAS.ITCH, XNYS.PILLAR, and IEXG.TOPS to coexist in the same partition date.

### 3.3 `orchestrator.py`

**Chunking:** Split into chunks of `chunk_size` symbols × 1 day (default: 10). 50 symbols × 30 days = 150 chunks. Each maps to one Databento batch job — a failure only affects that day/batch.

**Job store:** Every chunk has a `JobRecord` in a kdb binary table at `staging/metadata/backfill_jobs`, tracking `pending → submitted → running → downloaded → loaded → verified`. State is written before each action so a crash leaves a recoverable record. Managed by `KdbJobStore` via a q subprocess running `jobstore.q`.

**Per-chunk pipeline:** cost estimate → submit → poll → download → SHA-256 checksum → write manifest → mark loaded. Chunks already in `loaded` or `verified` status are skipped without re-querying Databento.

**Parallelism:** 12 workers run the download phase concurrently (`ThreadPoolExecutor`). The q loader is invoked once after all downloads complete — concurrent `.Q.dpft` calls on the same partition would corrupt the HDB. `fcntl.flock` enforces this across processes.

**Pre-flight HDB check:** Before any API submission, a q one-liner checks which requested dates already have data for the target `(schema, exchange)` pair. Those dates are dropped from the chunk list — no cost, no API calls.

### 3.4 `manifest.q`

Reads and validates JSON manifests written by Python, then groups them by `(date, schema)` before dispatching to the loader. Batching by date means each partition is written exactly once. Filters to the current run's manifests via a `REQUEST_ID` env var to prevent parallel runs from processing each other's data.

### 3.5 `loader.q`

Key steps in `loadChunkBatch`:

1. **Idempotency** — check if this exchange already exists in the partition by reading only the `exchange` column file on disk
2. **Row count validation** — parsed rows must match the manifest's declared count
3. **Quality checks** — duplicates, time ordering, nulls. Bad data rejected before touching the HDB
4. **Partition merge** — when adding a second exchange to an existing date, read the existing partition, reverse `.Q.dpft`'s symbol enumeration via `unenumAll`, concatenate, re-write
5. **Write lock** — atomic `mkdir`-based POSIX lock prevents concurrent writes
6. **`.Q.dpft`** — writes the splayed partition with `p#` on `sym`, then `g#` on `exchange`

### 3.6 `quality.q`

Three checks before every write: duplicate natural keys, time ordering, nulls in price/size columns. A failure marks the chunk `failed` — bad data never reaches the HDB.

### 3.7 Reference Data & Adjustments

`ref_ingest.py` fetches corporate actions and dividends via yfinance and computes cumulative adjustment factors. Reference tables are append-only with a `loaded_at` timestamp on every row. `adjlib.q` exposes `getAdjustedClose` with `backward`/`forward` methods and an `asOf` parameter for point-in-time factor selection — factors ingested after a given date are excluded, preventing look-ahead bias in backtesting.

### 3.8 Tests

```bash
./scripts/run_tests.sh
```

8 q tests (schema, manifest, loader, quality, symbology, ref tables, adj, integration) and 56 Python tests across `test_orchestrator.py` and `test_metrics.py`. The integration test auto-discovers the most recent HDB partition and runs live queries against it.

---

## 4. How Claude Was Used

**This was explicitly encouraged as part of the graduate project.** The goal was to learn how to direct an AI assistant to produce production-quality output, not just to build the pipeline.

---

### 4.1 The Initial Prompting Strategy

The project did not start with "write me a backfill pipeline." The first prompt was:

> *"I am going to give you a set of project requirements. Before you write a single line of code, ask me every clarifying question you need to answer in order to build this correctly."*

Claude raised questions covering:
- Which kdb+ version — kdb+5, not kdb+4 (several language behaviours differ, and code written for kdb+4 idioms fails silently in kdb+5)
- Whether this was a separate TorQ package or modifications to TorQ core (answer: separate package — critical for keeping the framework read-only and the project portable)
- What "production-ready" meant — idempotency, crash recovery, cost safeguards, and test coverage all called out explicitly
- That documentation clarity was as important as functionality since the project was being built by graduates

This front-loaded the specification work. The common failure mode with AI-assisted development is discovering requirement misalignments after code exists, at which point fixes cascade across many files rather than adjusting a prompt. The existing directory tree was also shared before any code was generated.

---

### 4.2 Milestone Structure

The build was split into five independently testable milestones:

| Milestone | Scope |
|---|---|
| **M0** | Package skeleton: `setenv.sh`, `schema.q`, `process.csv`, `settings.q` |
| **M1** | Full pipeline: `orchestrator.py`, `loader.q`, `manifest.q`, basic tests |
| **M2** | Hardening: job store, idempotency, retry with exponential backoff, cost safeguard |
| **M3** | Reference data: `ref_ingest.py`, `ref_tables.q`, `adjlib.q` |
| **M4** | Multi-exchange: `exchange` column, merge logic, `unenumAll` |

Completing and testing each milestone before starting the next prevented the common failure mode of generating everything at once and having no way to isolate errors. At the end of M1 there was a fully working single-exchange pipeline. M2 made it resilient. M3 and M4 extended it without breaking anything already in place.

---

### 4.3 Persistent Context — SKILL.md and Session Memory

A major challenge with multi-session AI development is context loss. Two mechanisms addressed this:

**SKILL.md** — a 426-line kdb+/q developer reference document created at the start and loaded into every Claude session. It covers q language fundamentals, HDB partition patterns, TorQ conventions, and kdb+5-specific gotchas. Without it, Claude periodically reverted to kdb+4 idioms — for example, using `.z.e` to read environment variables (which returns nothing in kdb+5; `getenv` is the correct approach), or writing `q hdb` to load the HDB (which changes the working directory and breaks subsequent script loads). SKILL.md prevented these from being re-introduced across sessions.

**Session memory** — Claude Code maintains a persistent `MEMORY.md` file that accumulates project state, architectural decisions, file layout changes, and kdb+5 bugs discovered during development. Each new session started with full context. Without this, every session would begin with re-explaining the architecture and revisiting resolved issues.

---

### 4.4 Iterative Debugging — Specific Examples

The most important part of the workflow was not prompting Claude to write code — it was verifying the output and feeding failures back precisely. Several bugs required real domain knowledge to identify:

**`.Q.dpft` requires a global table, not a local variable.** An early version of `loadChunkBatch` passed a local variable directly:
```q
data: readTradesCSV[...];
.Q.dpft[hdbDir; partDate; `sym; data]   / WRONG — writes empty schema table
```
This compiled and ran without error but wrote an empty table to the HDB. The bug was only caught by querying the database and finding all partitions empty. Fix: set the global first, then pass its name:
```q
`trades set data;
.Q.dpft[hdbDir; partDate; `sym; `trades]
```
This is not documented clearly in the kdb+ reference and would not be caught by a code review.

**Enum corruption on multi-exchange merge.** When multi-exchange support was added, existing partitions came back with integers (`12`, `13`, ...) instead of symbol names after a second exchange was loaded. Root cause: `.Q.dpft` enumerates *all* symbol columns (type 20h) on write, not just `sym`. When merging, the existing partition's enumerated columns were being concatenated with the new data's plain symbol lists, silently corrupting the result. Fix: the `unenumAll` function detects all type-20h columns before the merge and reverses the enumeration:
```q
unenumAll:{[t] @[t; where 20h = type each flip t; {`$string x}] }
```

**`};` at column 0 silently misparsed in kdb+5.** A multi-line function's closing brace placed at the start of a line caused the function body to end early when the script was loaded via `\l`. No error was raised at load time — the function simply ran with wrong behaviour on specific inputs. Fix: always indent the closing brace by at least one space. This is undocumented behaviour specific to kdb+5 script loading.

**`get` suppresses the partition key column.** When reading an existing partition to merge with new data, `get` returns the table without the `date` column — it is implied by the directory name. Concatenating with new data that includes `date` caused a `'mismatch` error. Fix: `update date:partDate from existing` before the merge.

**Parallel-run race condition.** During multi-exchange testing, running two `./bin/backfill` invocations simultaneously — one for XNAS.ITCH, one for XNYS.PILLAR — caused each q loader to scan the entire `staging/metadata/manifests/` directory and process all manifests, not just its own run's. This produced partial writes: some symbols appeared in the HDB from the wrong loader, some were skipped entirely. The bug was invisible until the HDB was queried and row counts didn't match expectations. Fix: Python passes `REQUEST_ID` as an environment variable to the q subprocess; `manifest.q` filters `processManifests` to only the manifests matching that ID:
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

Midway through the project a formal gap analysis was run — the original requirements were reviewed line by line against the actual implementation. Two gaps found and fixed:

1. **`ref_corp_actions` not append-only.** The requirements specified point-in-time correctness for reference data (a backtest in January should only use factors available in January). The table was being overwritten on each ingest. Fix: switched to append-only with a `loaded_at` timestamp on every row, matching the approach already used for `ref_adj_factors`.

2. **Job store still using JSON files.** The requirements specified a kdb binary table for job tracking. The implementation had `backfill_jobs` defined in `schema.q` but the actual store was writing one JSON file per chunk to `staging/metadata/jobs/`. Fix: `jobstore.q` was written to manage the kdb binary table; Python's `JobStore` was replaced with `KdbJobStore`.

The audit approach — treating it as a formal gap analysis rather than "does it seem to work" — surfaced issues that would have been invisible in normal testing.

---

### 4.6 Architectural Decisions That Were Human-Directed

Claude consistently deferred to explicit direction on architectural choices:

- **Python/q boundary** — an early draft had Python doing CSV parsing. Redirected: all type enforcement happens in q at the point of the HDB write, so a malformed value causes a controlled failure in the loader rather than a silent type mismatch in the database.
- **`exchange` column from M0** — Claude's initial schema draft omitted it. Adding it retroactively would have required migrating all existing partitions.
- **Idempotency as a requirement** — Claude's M1 draft assumed a clean run. The job store, checksum verification, and HDB pre-flight check were specified as non-negotiable from the start.
- **Scope control** — Claude periodically suggested additions: a monitoring dashboard, a REST API for job status, a database-backed configuration system. Each was declined in favour of a well-understood, maintainable pipeline.
- **Chunk size** — the initial implementation used 1 symbol per chunk for maximum retry granularity. After reviewing the Databento API overhead per job, this was revised to a default of 10 symbols per chunk, reducing API round-trips by ~10x for typical runs while keeping retry granularity acceptable.

---

### 4.7 What This Demonstrates

Using Claude effectively in this project required:

1. **Clear upfront specification** — the clarifying-questions prompt prevented requirement misalignments from being baked into early code
2. **Domain knowledge to evaluate output** — the enum corruption bug is syntactically correct and only manifests as wrong data in the HDB; it cannot be caught without understanding kdb+5's type system
3. **Incremental testing discipline** — completing and testing each milestone before starting the next eliminated entire classes of debugging problem
4. **Knowing when to override** — Claude's default behaviour is to add safety checks, fallbacks, and generality; several times the simpler, more direct approach was correct
5. **Persistent context management** — SKILL.md and session memory kept behaviour consistent across many sessions rather than drifting back to defaults

The AI substantially accelerated boilerplate, test generation, documentation, and iterative bug-fixing. Architectural decisions and correctness of the kdb+ partition logic were human-directed throughout.
