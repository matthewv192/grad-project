# Grad Project Presentation — 30 Minutes

**Presenter:** Matthew Vincent
**Date:** 2026-03-20
**Repo:** `github.com/matthewv192/grad-project` (branch: `dev`)

---

## Timing Guide

| Section | Topic | Time |
|---------|-------|------|
| 1 | Intro — what I was asked to do | 3 min |
| 2 | Architecture overview | 3 min |
| 3 | Code walkthrough (with Patrick/Jonny) | 10 min |
| 4 | Demo | 5 min |
| 5 | My experience on the project | 4 min |
| 6 | How this changes my approach going forward | 5 min |

---

## 1. Intro — What I Was Asked To Do (3 min)

**The brief:** Build a historical market data backfill package on top of the TorQ kdb+ framework using AI-assisted development with Claude.

**What it does:** Pulls trade and OHLCV data from the Databento API, converts it into kdb+ partitioned HDB format, and handles the full job lifecycle — chunking symbols into manageable batches, idempotent loading, retries, and post-write verification. It also includes a reference data layer (symbology, corporate actions, and a security master via OpenFIGI) and a Flask monitoring dashboard where you can submit backfill requests, track job progress, and inspect HDB coverage.

**Why it's not straightforward:**
- Even a small run (50 symbols, 1 month) creates ~50 API jobs that can each fail independently
- kdb+ partitions are files on disk — writing them wrong or concurrently corrupts the database
- A crash halfway through needs to be resumable without re-downloading or duplicating data

**Two goals in one project:**
1. Build a working pipeline
2. Learn how to use AI effectively and understand where it helps and where it falls short

---

## 2. Architecture Overview (3 min)

**Talk through this diagram:**

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

**Key point:** Python and q never talk directly. They communicate through files on disk. This means each side can be tested on its own and neither needs to know the other's internals.

**Other components:**
- `schema/schema.q` — single source of truth for every table definition
- `ref_ingest.py` — reference data (corporate actions, symbology, security master via OpenFIGI)
- `adjlib.q` — adjustment factor library with point-in-time lookups
- `code/monitor/` — Flask dashboard for job tracking, backfill submission, and HDB inspection

---

## 3. Code Walkthrough (10 min)

> This is the section with Patrick or Jonny. Walk through the files below in order — each one has talking points.

### 3.1 `setenv.sh`
- Sets all environment variables: `PACKAGEHOME`, `KDBHDB`, `STAGING_DIR`, `DATABENTO_API_KEY`
- Auto-detects TorQ if present, but the package runs standalone without it
- Every entrypoint (`bin/backfill`, `bin/monitor`) sources this first

### 3.2 `schema/schema.q`
- Every table defined here — trades, ohlcv_1m, backfill_jobs, ref tables
- All q scripts load this first so a column change only needs to happen in one place
- **Point out:** the `exchange` column in trades/ohlcv_1m — this lets multiple exchanges (XNAS, XNYS, IEXG) coexist in the same date partition

### 3.3 `orchestrator.py` — the big one
Walk through these concepts (no need to read every line):

- **Chunking** (line ~112): default 20 symbols per chunk, 1 trading day each. Weekends and NYSE holidays are skipped automatically using a trading calendar (2015–2030)
- **Job store** (line ~254): `JobStore` class tracks every chunk through `pending → submitted → running → downloaded → loaded → verified`. Written to a kdb binary table via subprocess
- **Cost safeguard**: `BACKFILL_MAX_COST_USD` default $50 — won't submit if estimated cost exceeds this
- **Pre-flight HDB check**: before any API call, queries the HDB to see what data already exists and skips those dates
- **Retry logic**: `--retry-failed` flag, exponential backoff capped at 60s
- **API rate limiting**: `threading.Semaphore(10)` on all Databento calls

### 3.4 `loader.q`
- **Idempotency check** — reads the exchange column file on disk to see if this data already exists
- **Quality checks** — duplicates, time ordering, nulls. Bad data gets rejected before touching the HDB
- **Symbology validation** — checks every (sym, instrument_id, exchange) tuple against ref_symbology_map
- **Multi-exchange merge** — when adding a second exchange to an existing partition, reads existing data, reverses enum via `unenumAll`, concatenates, re-writes
- **Post-write verification** — reads the partition back after `.Q.dpft` and verifies row count

### 3.5 Tests
- **212 Python tests** across 3 files (orchestrator, metrics, ref_ingest)
- **8 q test suites** covering schema, manifest, loader, quality, symbology, ref tables, adjustments
- **Integration test** that auto-discovers the most recent HDB partition and runs live queries
- Run with: `./scripts/run_tests.sh`

### 3.6 Monitoring Dashboard (`code/monitor/`)
- Flask app on port 8080
- Tabs: Jobs, Submit, Charts, Metrics, Failures, HDB Coverage, qSQL Query, Disk
- Can submit new backfill requests, cancel running jobs, retry failed ones
- Live progress updates via `.progress.json`
- OHLCV charting and log streaming
- qSQL Query tab: run arbitrary queries against the HDB (VWAP, TWAP, spread, etc.)

---

## 4. Demo (5 min)

> Pick one or two of these depending on what's easiest to show live on Teams.

### Option A: Run a dry-run backfill
```bash
source setenv.sh
./bin/backfill --dataset XNAS.ITCH --schema trades --symbols AAPL,MSFT --start 2024-06-01 --end 2024-06-05 --dry-run
```
**What to show:** cost estimate comes back without spending any money. Point out the trading calendar skipping weekends.

### Option B: Show the monitoring dashboard
```bash
./bin/monitor
```
**What to show:** open in browser at `localhost:8080`. Walk through the Jobs tab (status bars, verified/failed counts), HDB Coverage tab (which dates have data), and the submit form.

### Option C: Show the HDB
```bash
q -p 5000
\l hdb
select count i by date from trades
select distinct sym from trades where date = 2024.01.15
```
**What to show:** data is actually there, partitioned by date, queryable.

### Option D: Run the test suite
```bash
./scripts/run_tests.sh
```
**What to show:** all 8 q tests and 212 Python tests passing. Takes about 30 seconds.

### Things I struggled with (mention during or after demo)
- **kdb+5 gotchas** — `.z.e` being empty, `};` at column 0 being misparsed, `.Q.dpft` needing a global not a local. These are undocumented or poorly documented behaviours that caused silent failures
- **Enum corruption on multi-exchange merge** — data looked fine at write time but came back as integers instead of symbols. Took real kdb+ understanding to diagnose
- **Parallel-run race condition** — two simultaneous backfills would process each other's manifests. Only showed up when querying the HDB and row counts were wrong

---

## 5. My Experience on the Project (4 min)

### How AI was used
- ~95% of code was AI-generated. My input was focused on schema design, defining what the output should look like, and quality assurance
- About 75% of what Claude produced was kept without major rewrite. The other 25% needed fixes before it was production-ready
- Features took about 4 prompts on average to get right, but this improved as I got better at prompting
- First working version took about a day. Getting to presentation-ready took about 3 days

### What worked well
- **Fast prototyping** — could get a working pipeline, test suite, or dashboard built in a single session
- **Multi-language** — Claude worked across Python, q, bash, HTML, and markdown without needing separate tools
- **Custom skills made a big difference** — I created two skill documents that Claude loaded at the start of each session:
  - A **q/kdb+ Developer Skill** (426 lines) covering language fundamentals, HDB patterns, and kdb+5 gotchas. This stopped Claude from repeatedly making the same kdb+ mistakes
  - A **Code Review Skill** that gave Claude a structured framework for reviewing code across all three languages. This meant every review came back with concrete fixes ranked by severity, not vague suggestions
- **Good at bulk work** — generating 212 tests, updating docs across multiple files, running structured code reviews

### What didn't work well
- **Didn't self-verify** — Claude would produce code that looked correct but hadn't been tested. I had to run it, find the errors, and feed them back
- **kdb+ is too niche** — even with the Developer Skill, Claude got tripped up by version-specific behaviours. The skill reduced the problem but didn't eliminate it
- **Context loss** — in long sessions Claude would forget earlier decisions. Using the persistent memory system fixed this, but I had to learn to refresh it regularly
- **Couldn't define domain output** — Claude could build the mechanics but needed me to say what the schema and expected output should actually look like

---

## 6. How This Changes My Approach Going Forward (5 min)

### On client engagements
- I'd use AI to **get up to speed faster** — generate skeleton code, understand unfamiliar codebases, draft documentation. The first 80% of a task can be done much quicker
- But I'd **always verify the output myself** before it goes anywhere. The 25% that needed rework in this project would be unacceptable in a client setting without review
- For niche technologies (kdb+, specialist APIs), **create a skill document upfront**. It took me an hour to write the q Developer Skill and it saved days of correcting the same mistakes

### On approaching issues / Jiras / specs
- **Start with clarifying questions before writing any code.** The best thing I did on this project was asking Claude to raise every question it had about the requirements before generating anything. I'd do the same myself — understand the spec fully before starting
- **Break work into small, testable milestones.** Each milestone in this project was independently testable. If something broke, I knew exactly which change caused it
- **Write tests alongside the code, not after.** Claude generated tests as part of each milestone, which caught bugs early. I'd keep this habit even without AI

### On code quality
- **Use structured code reviews.** The Code Review Skill gave a repeatable framework — correctness, efficiency, style, security, testability. I'd apply the same checklist to my own reviews and PRs
- **Don't trust code that hasn't been run.** The biggest lesson from this project. Code that compiles and looks right can still produce wrong results silently, especially in kdb+
- **Keep a record of gotchas.** The kdb+5 gotchas list in this project saved me from hitting the same issue twice. On any new technology I'd maintain a similar list

### Anything else
- AI is a tool, not a replacement for understanding. The bugs that mattered most on this project — enum corruption, race conditions, `.Q.dpft` semantics — all required actual knowledge of kdb+ to diagnose. Claude couldn't find them on its own
- The skill documents and memory system are transferable. On any future project with a niche technology, I'd invest time upfront creating reference material for Claude rather than correcting the same mistakes session after session

---

## Quick Reference — If Asked

| Question | Answer |
|----------|--------|
| How many tests? | 212 Python + 8 q suites |
| What exchanges? | XNAS.ITCH, XNYS.PILLAR, IEXG.TOPS, EQUS.MINI |
| How much data? | 17+ date partitions, ~10 symbols |
| Cost safeguard? | $50 default cap per run |
| How long to build? | ~3 days to presentation-ready |
| What % AI-generated? | ~95% |
| What % kept as-is? | ~75% |
