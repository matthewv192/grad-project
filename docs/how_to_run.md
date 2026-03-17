# How To Run

A complete guide to running the TorQ Databento backfill pipeline from scratch.

---

## Prerequisites

- **kdb-x 5.0** — installed and on `PATH` (check: `q -q <<< "exit 0"`)
- **Python 3.10+** with `pip`
- **A Databento API key** with Historical API access
- **TorQ** cloned alongside this project (see layout below)

---

## Directory Layout

```
~/                          ← (or any common parent)
├── TorQ/                   ← read-only AquaQ TorQ framework
├── grad-project/           ← this package
└── venv/                   ← Python virtual environment (created below)
```

---

## Initial Setup (one-time)

### 1. Clone TorQ

```bash
git clone https://github.com/AquaQAnalytics/TorQ.git ~/TorQ
```

### 2. Install Python dependencies

```bash
cd ~/grad-project
python3 -m venv ../venv
source ../venv/bin/activate
pip install -e .
```

### 3. Verify the environment

```bash
source setenv.sh
```

Expected output:

```
TORQHOME    = /home/<user>/TorQ
PACKAGEHOME = /home/<user>/grad-project
KDBHDB      = /home/<user>/grad-project/hdb
```

`setenv.sh` sets `TORQHOME`, `PACKAGEHOME`, `KDBHDB`, `STAGING_DIR`, and the
Databento API key. The `bin/backfill` entrypoint sources it automatically on
every run, so you only need to source it manually for interactive q sessions.

---

## Running a Backfill

All commands use `bin/backfill`, which handles environment setup automatically.

### Request data

```bash
./bin/backfill \
    --symbols "AAPL,MSFT,GOOG" \
    --start 2024-06-03 \
    --end 2024-06-07 \
    --schema trades
```

**Arguments:**

| Flag | Default | Description |
|------|---------|-------------|
| `--symbols` | _(required)_ | Comma-separated ticker list |
| `--start` | _(required)_ | Start date, inclusive (YYYY-MM-DD) |
| `--end` | _(required)_ | End date, inclusive (YYYY-MM-DD) |
| `--schema` | `trades` | `trades` or `ohlcv-1m` |
| `--dataset` | `XNAS.ITCH` | Databento dataset identifier |
| `--chunk-size` | `10` | Symbols per Databento batch job |
| `--workers` | `12` | Parallel chunk workers |
| `--dry-run` | off | Print cost estimate only; no API calls |
| `--download-only` | off | Download and stage CSVs; skip the q loader |
| `--load-only` | off | Skip API calls; run the q loader on existing staged manifests |
| `--metrics` | off | Print per-chunk timing metrics and exit |

`bin/backfill` sources `setenv.sh` and activates the venv automatically. It
submits one Databento job per symbol per day, running up to 12 in parallel.
A live progress line updates in the terminal as chunks complete, followed by
a summary table of rows loaded per exchange on success.

### Pre-flight HDB check

Before submitting any Databento jobs, the orchestrator checks the HDB for dates that already have data for the requested `(schema, dataset)` combination. Those dates are skipped automatically — no API calls are made and no cost is incurred. The output shows which dates were skipped:

```
NOTE: 3 chunk(s) already in HDB — skipping (saves API cost):
  XNAS.ITCH  ohlcv-1m  2024-06-03  — already loaded
  XNAS.ITCH  ohlcv-1m  2024-06-04  — already loaded
  XNAS.ITCH  ohlcv-1m  2024-06-05  — already loaded
```

If all requested dates are already loaded, the run exits cleanly without submitting anything.

### Dry run (cost estimate, no API calls)

```bash
./bin/backfill \
    --symbols "AAPL,MSFT" \
    --start 2024-06-03 \
    --end 2024-06-07 \
    --dry-run
```

### Check progress

```bash
./bin/backfill --status
```

Filter to a specific request:

```bash
./bin/backfill --status --request-id req_20240603_120000_abc123
```

### Retry failures

If any chunks show `failed` in the status output:

```bash
./bin/backfill --retry-failed
```

---

## Requesting OHLCV Data

```bash
./bin/backfill \
    --symbols "AAPL" \
    --start 2024-06-03 \
    --end 2024-06-05 \
    --schema ohlcv-1m
```

OHLCV bars are written to `hdb/YYYY.MM.DD/ohlcv_1m/`.

---

## Loading Multiple Exchanges

The loader merges data from different exchanges into the same HDB partition.
Run a separate backfill per dataset; each load appends its rows to the existing partition:

```bash
# Load NASDAQ data
./bin/backfill \
    --symbols "AAPL,MSFT" --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dataset XNAS.ITCH

# Add NYSE data to the same partition
./bin/backfill \
    --symbols "AAPL,MSFT" --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dataset XNYS.PILLAR

# Add IEX data
./bin/backfill \
    --symbols "AAPL,MSFT" --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dataset IEXG.TOPS
```

Supported Databento datasets include `XNAS.ITCH`, `XNYS.PILLAR`, `IEXG.TOPS`, `EQUS.MINI`, and any other dataset that provides `trades` or `ohlcv-1m` schema. The `exchange` column in each HDB table identifies the source.

Idempotency is exchange-aware: re-running a load for an already-loaded `(date, exchange)` pair is detected and skipped without re-writing any data.

---

## Querying the HDB

### Interactive q session

```bash
cd ~/grad-project
q -q
```

Then in q:

```q
\l hdb
```

> **Note on loading order:** `\l hdb` changes q's working directory to `hdb/`. Load any additional scripts (e.g. `adjlib.q`) **before** `\l hdb`, or use `q -q` and load the HDB explicitly rather than `q hdb` (which has the same effect).


```q
/ Row counts by exchange for a partition date
select count i by exchange from ohlcv_1m where date=2024.01.17

/ A specific day and symbol
select from trades where date=2024.06.03, sym=`AAPL

/ Filter by exchange
select from ohlcv_1m where date=2024.01.17, sym=`AAPL, exchange=`XNAS.ITCH

/ Time range with VWAP (trades schema)
select vwap: size wavg price, total_size: sum size
    by sym, 0D00:30 xbar time
    from trades
    where date=2024.06.03, sym in `AAPL`MSFT

/ Compare OHLCV across exchanges for the same symbol/date
select open, high, low, close, volume by exchange
    from ohlcv_1m
    where date=2024.01.17, sym=`AAPL
```

### Adjusted close prices

Start q from the project root, load adjlib **before** the HDB (loading the HDB changes the working directory), then load reference data:

```bash
cd ~/grad-project
q -q
```

```q
\l code/adjlib/adjlib.q
\l hdb
loadAllRefData[]

/ Raw (unadjusted) bars — accepts a single sym or a list
getUnadjusted[`ohlcv_1m; `AAPL; 2024.06.03; 2024.06.05]
getUnadjusted[`ohlcv_1m; `AAPL`MSFT; 2024.06.03; 2024.06.05]

/ Backward-adjusted — prices scaled to post-split (current) terms (latest factors)
getAdjustedClose[`AAPL; 2024.06.03; 2024.06.05; `backward; 0Np]

/ Forward-adjusted — prices scaled to pre-split (historical) terms
getAdjustedClose[`AAPL; 2024.06.03; 2024.06.05; `forward; 0Np]

/ Point-in-time: use only factors loaded on or before a given timestamp
/ (avoids look-ahead bias from factor revisions made after a backtest start date)
getAdjustedClose[`AAPL; 2024.06.03; 2024.06.05; `backward; 2024.07.01D00:00:00.000000000]
```

> **Note:** Adjustment factors are fetched automatically from yfinance after each backfill and stored in `staging/reference/adj_factors.csv`. `loadAllRefData[]` loads them into memory. Pass `0Np` as the fifth argument to `getAdjustedClose` to use the latest available revision, or a specific timestamp for point-in-time factor selection.

---

## HDB Layout

After loading data from multiple exchanges the HDB looks like:

```
hdb/
├── 2024.01.17/
│   └── ohlcv_1m/           ← splayed table with rows from all exchanges
│       ├── sym              ← enumerated symbol (parted column, p# attribute)
│       ├── time
│       ├── exchange         ← identifies source (XNAS.ITCH, XNYS.PILLAR, etc.; g# attribute)
│       ├── open
│       ├── high
│       ├── low
│       ├── close
│       ├── volume
│       ├── instrument_id
│       └── date
├── 2024.06.03/
│   └── trades/             ← splayed table
│       ├── sym              ← p# attribute
│       ├── time
│       ├── exchange         ← g# attribute
│       ├── price
│       ├── size
│       ├── side
│       ├── conditions
│       ├── sequence
│       ├── instrument_id
│       └── date
└── sym                     ← symbol enumeration file (shared across all partitions)
```

---

## Running Tests

```bash
# Full suite — 8 q tests + 3 Python tests (integration skipped if no trades data)
./scripts/run_tests.sh
```

The integration test is skipped if the auto-discovered partition has no trades data. When data is present, it auto-discovers the most recent partition date and first available sym — no further configuration is needed:

```bash
KDBHDB=/path/to/hdb ./scripts/run_tests.sh
```

To pin to a specific date or sym:

```bash
KDBHDB=/path/to/hdb \
INTEGRATION_TEST_DATE=2024-01-17 \
INTEGRATION_TEST_SYM=AAPL \
./scripts/run_tests.sh
```

To run an individual test in isolation:

```bash
# q unit tests (no external dependencies)
q tests/test_schema.q
q tests/test_manifest.q
q tests/test_loader.q
q tests/test_quality.q
q tests/test_symbology.q
q tests/test_ref_tables.q
q tests/test_adj.q

# Integration test (requires a populated HDB)
KDBHDB=/path/to/hdb q tests/test_integration.q

# Python tests
python3 -m pytest tests/test_orchestrator.py -v
python3 -m pytest tests/test_metrics.py -v
python3 -m pytest tests/test_ref_ingest.py -v
```

---

## Logs & Diagnostics

### Log files

Every run writes a log file under `logs/`:

| Mode | Log file |
|------|----------|
| Normal backfill | `logs/<request_id>.log` |
| `--retry-failed` | `logs/retry_<timestamp>.log` |
| `--load-only` | `logs/load_only_<timestamp>.log` |

The request ID is printed at the start of every run, so you can match terminal output to the log file.

### Log format

Every line is a JSON object:

```json
{"time":"2024-06-03T12:04:13","level":"INFO","logger":"orchestrator","msg":"..."}
```

The q loader runs as a subprocess — its output is captured and re-emitted wrapped in the orchestrator's JSON, with a `[q]` prefix in `msg`:

```json
{"time":"...","level":"INFO","logger":"orchestrator","msg":"[q] 2024.06.03D12:06:16 [loader] loading batch: schema=trades date=2024.06.03 chunks=1"}
```

**Important:** q-layer errors appear with `[ERROR]` inside the `msg` string, not in the top-level `level` field (which stays `INFO`). This means grepping for `"level":"ERROR"` alone will miss q errors.

### Finding errors quickly

```bash
# Catches both Python-layer errors and q-layer errors
grep -i 'error\|failed\|failure' logs/<request_id>.log

# Python-layer errors only (level field)
grep '"level":"ERROR"' logs/<request_id>.log

# q-layer errors only ([ERROR] prefix in msg)
grep '\[ERROR\]' logs/<request_id>.log

# See just the meaningful events (skip polling noise)
grep -v 'Polling job_id' logs/<request_id>.log
```

### Reading a failure

When a chunk fails the log will show the sequence:

1. `quality checks failed` or similar error message in `msg`
2. `job record updated: <chunk_id> → loaded` — data was parsed
3. `job record updated: <chunk_id> → failed` — write was aborted due to the error

The specific cause is always on the line just before the status transitions. For example:

```
[quality] QUALITY FAILURES trades date=2024.06.03: dups=218 ordering_errors=0 null_cells=0
[loader] job record updated: req_... → loaded
[loader] job record updated: req_... → failed
```

For a detailed look at all job records, query the kdb job store directly:

```bash
echo '{"op":"loadAll"}' | JOBS_FILE=$(pwd)/staging/metadata/backfill_jobs q -q code/backfill/jobstore.q
```

The `error_msg` and `failure_type` fields contain the exact failure reason.

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `LD_LIBRARY_PATH: unbound variable` | `set -u` with unset var | Already fixed — `setenv.sh` uses `${LD_LIBRARY_PATH:-}` |
| `TORQHOME = ` (empty) | `setenv.sh` sourced before `cd grad-project` | Always source from inside `grad-project/` |
| `q loader exited 1` | q can't find `schema/schema.q` | Ensure you're running from inside `grad-project/` |
| `write lock held for ...` | Stale lock from a crashed run | Run `find hdb -name ".*.lock" -type d -exec rmdir {} +` then resubmit with `--retry-failed` |
| Some syms missing from HDB after parallel runs | Multiple `./bin/backfill` invocations ran simultaneously | The `REQUEST_ID` manifest filter prevents this in current code. If you see missing syms from an old run, use `--retry-failed` to reload the affected chunks. |
| `ValueError: Cannot infer date from filename` | Databento changed filename format | Check the downloaded CSV filename; report the new pattern to update the regex |
| `request_id already exists with different parameters` | `--request-id` collision | Omit `--request-id` to auto-generate a new one, or use `--retry-failed` to resume the original run |
| `Job ... failed at Databento` | Databento rejected or failed the batch job | Check the error detail in the log; run `./bin/backfill --retry-failed` after the cause is resolved |
| Chunk shows `failed` in status | API error or cost limit hit | Run `./bin/backfill --retry-failed`; inspect error via `--status` or query `staging/metadata/backfill_jobs` directly |
| Old stale manifest causes validation error | CSV file deleted but manifest remains | Safe to ignore — logged as a warning, does not block other chunks |
| `ModuleNotFoundError: No module named 'databento'` | venv not found | Run `scripts/setup_python.sh` to create the venv, then retry |

---

## Recovery Procedures

When something goes wrong in production, use the table below to recover.
The pipeline is designed for idempotent re-runs — in most cases, simply
re-running the same command (or `--retry-failed`) is sufficient.

| Failure | What happened | Recovery |
|---------|---------------|----------|
| Orchestrator crashes mid-run | Some chunks completed, others didn't. Job store has partial state. | Re-run with the same `--request-id` (or omit it to resume the auto-generated one). Idempotency skips already-verified chunks and resumes from the last checkpoint. |
| q loader crashes mid-`.Q.dpft` | Partition may be partially written. Lock file may be stale. | Re-run — stale lock detection will clean up the old lock automatically. The loader overwrites the incomplete partition on retry. |
| Databento API is down | Chunks fail with `api_error` status. | Wait for the API to recover, then run `./bin/backfill --retry-failed`. |
| HDB partition is corrupt | Bad data in `hdb/YYYY.MM.DD/<schema>/` (wrong row count, missing columns, etc.) | Delete the entire partition directory (`rm -rf hdb/YYYY.MM.DD/<schema>/`) and re-run the backfill for that date. The pipeline will treat it as a fresh load. |
| Job store is corrupt | `staging/metadata/backfill_jobs` is unreadable or has inconsistent data. | Delete the file (`rm staging/metadata/backfill_jobs`) and re-run. The CSVs and manifests in `staging/` are the real source of truth; the job store is just a status tracker. |
| Cost limit exceeded | Chunk fails immediately with `cost exceeds limit` in error_msg. | Either increase the limit (`export BACKFILL_MAX_COST_USD=100`) or reduce the date range / symbol count, then `--retry-failed`. |
| Disk full during download | Download fails, partial CSVs left in `staging/<chunk_id>/`. | Free disk space (run `./scripts/cleanup_staging.sh`), then `--retry-failed`. The checksum mismatch on the partial CSV triggers a fresh re-download. |

---

## Maintenance

### Staging cleanup

Downloaded CSVs are cleaned up automatically after successful verification,
but failed or abandoned runs leave data behind. The cleanup script removes
staging chunk directories and old log files:

```bash
# Default: remove staging dirs older than 7 days, logs older than 30 days
./scripts/cleanup_staging.sh

# Custom retention
STAGING_DAYS=3 LOG_DAYS=14 ./scripts/cleanup_staging.sh
```

For automated cleanup, add a cron entry:

```bash
# Run daily at 02:00 — adjust paths to your installation
0 2 * * * cd /path/to/grad-project && ./scripts/cleanup_staging.sh >> logs/cleanup.log 2>&1
```

### Log rotation

Each backfill run creates a per-request log file in `logs/`. The Python
`RotatingFileHandler` caps each file at 10 MB with 5 backups (50 MB max per
request). Across many requests, the `logs/` directory grows — the cleanup
script handles this by removing files older than the `LOG_DAYS` retention
period (default: 30 days).

### Monitoring disk usage

For production deployments, monitor the `staging/` and `hdb/` directories:

```bash
du -sh staging/ hdb/ logs/
```

Typical sizes:
- `staging/`: near-zero after successful runs (CSVs cleaned up); can grow to
  10-50 GB during a large backfill before cleanup runs
- `hdb/`: grows proportionally to data loaded (roughly 50-200 MB per
  symbol-day for trades, 1-5 MB for ohlcv-1m)
- `logs/`: bounded by cleanup script retention
