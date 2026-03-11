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

### 3. Set your API key

```bash
export DATABENTO_API_KEY="db-XXXXXXXXXXXXXXXXXXXX"
```

> The key must be exported **before** sourcing `setenv.sh`. It can also be set
> permanently in your shell profile (`~/.bashrc` / `~/.zshrc`).

### 4. Source the environment

```bash
source setenv.sh
```

Expected output:

```
TORQHOME    = /home/<user>/TorQ
PACKAGEHOME = /home/<user>/grad-project
KDBHDB      = /home/<user>/grad-project/hdb
```

This sets `TORQHOME`, `PACKAGEHOME`, `KDBHDB`, `STAGING_DIR`, and activates
the venv if it exists at `../venv`.

---

## Running a Backfill

All commands are run from inside `grad-project/`.

### Request data

```bash
./scripts/request_backfill.sh \
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
| `--chunk-size` | `10` | Symbols per batch job |
| `--dry-run` | off | Print cost estimate only; no API calls |
| `--download-only` | off | Download and stage CSVs; skip the q loader |
| `--load-only` | off | Skip API calls; run the q loader on existing staged manifests |
| `--metrics` | off | Print per-chunk timing metrics and exit |

The script:
1. Sources `setenv.sh` and activates the venv
2. Calls `orchestrator.py` which submits one Databento job per (day × symbol-batch)
3. Polls until each job is done, downloads the CSV, writes a JSON manifest
4. Invokes the q loader to write data into `hdb/`

Progress is logged to stdout in JSON format.

### Dry run (cost estimate, no API calls)

```bash
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" \
    --start 2024-06-03 \
    --end 2024-06-07 \
    --dry-run
```

### Check progress

```bash
./scripts/backfill_status.sh
```

Filter to a specific request:

```bash
./scripts/backfill_status.sh --request-id req_20240603_120000_abc123
```

### Retry failures

If any chunks show `failed` in the status output:

```bash
./scripts/retry_failed.sh
```

---

## Requesting OHLCV Data

```bash
./scripts/request_backfill.sh \
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
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dataset XNAS.ITCH

# Add NYSE data to the same partition
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dataset XNYS.PILLAR

# Add IEX data
./scripts/request_backfill.sh \
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
q hdb
```

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

```q
\l code/adjlib/adjlib.q

/ Raw (unadjusted) bars — accepts a single sym or a list
getUnadjusted[`ohlcv_1m; `AAPL; 2024.06.03; 2024.06.05]
getUnadjusted[`ohlcv_1m; `AAPL`MSFT; 2024.06.03; 2024.06.05]

/ Backward-adjusted — prices scaled to post-split (current) terms
getAdjustedClose[`AAPL; 2024.06.03; 2024.06.05; `backward]

/ Forward-adjusted — prices scaled to pre-split (historical) terms
getAdjustedClose[`AAPL; 2024.06.03; 2024.06.05; `forward]
```

> **Note:** These functions query the HDB, so run them from a session that has already loaded it (`q hdb` or after `system "l hdb"`). The adjustment factors come from synthetic reference data generated by `ref_ingest.py`. Replace with real corporate action data for production use (see [limitations](limitations.md)).

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
# Full suite — 8 q tests + 2 Python tests (integration skipped without KDBHDB)
./scripts/run_tests.sh
```

The integration test is skipped unless `KDBHDB` points to a populated HDB. When enabled, it auto-discovers the most recent partition date and first available sym — no further configuration is needed:

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
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `LD_LIBRARY_PATH: unbound variable` | `set -u` with unset var | Already fixed — `setenv.sh` uses `${LD_LIBRARY_PATH:-}` |
| `TORQHOME = ` (empty) | `setenv.sh` sourced before `cd grad-project` | Always source from inside `grad-project/` |
| `DATABENTO_API_KEY is not set` | Key not exported | `export DATABENTO_API_KEY="db-..."` before sourcing |
| `q loader exited 1` | q can't find `schema/schema.q` | Ensure you're running from inside `grad-project/` |
| `write lock held for ...` | Stale lock from a crashed run | Run `find hdb -name ".*.lock" -type d -exec rmdir {} +` then reset the chunk status to `downloaded` in `staging/metadata/jobs/` |
| `ValueError: Cannot infer date from filename` | Databento changed filename format | Check the downloaded CSV filename; report the new pattern to update the regex |
| `request_id already exists with different parameters` | `--request-id` collision | Omit `--request-id` to auto-generate a new one, or use `--retry-failed` to resume the original run |
| `Job ... failed at Databento` | Databento rejected or failed the batch job | Check the error detail in the log; run `retry_failed.sh` after the cause is resolved |
| Chunk shows `failed` in status | API error or cost limit hit | Run `retry_failed.sh`; inspect error in `staging/metadata/jobs/` |
| Old stale manifest causes validation error | CSV file deleted but manifest remains | Safe to ignore — logged as a warning, does not block other chunks |
| `ModuleNotFoundError: No module named 'databento'` | venv not activated | Run `source ../venv/bin/activate` or `source setenv.sh` first |
