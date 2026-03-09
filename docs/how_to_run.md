# How To Run

A complete guide to running the TorQ Databento backfill pipeline from scratch.

---

## Prerequisites

- **kdb-x 5.0** — installed and on `PATH` (check: `q -q <<< "exit 0"`)
- **Python 3.10+** with `pip`
- **A Databento API key** with Historical API access
- **TorQ** cloned in the directory _alongside_ this project (see layout below)

---

## Directory Layout

The pipeline expects TorQ to sit one level up from `grad-project/`:

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
python -m venv ../venv
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
| `--dataset` | `XNAS.ITCH` | Databento dataset |
| `--chunk-size` | `10` | Symbols per batch job |
| `--dry-run` | off | Print cost estimate only; no API calls |
| `--skip-load` | off | Download only; skip the q loader |

The script:
1. Sources `setenv.sh` and activates the venv
2. Calls `orchestrator.py` which submits one Databento job per (day × symbol-batch)
3. Polls until each job is done, downloads the CSV, writes a manifest
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

## Querying the HDB

### Interactive q session

```bash
cd ~/grad-project
q -q
```

```q
\l hdb

/ Row counts by date and symbol
select count i by date, sym from trades

/ A specific day
select from trades where date = 2024.06.03, sym = `AAPL

/ Time range with aggregation
select vwap: size wavg price, total_size: sum size
    by sym, 0D00:30 xbar time
    from trades
    where date = 2024.06.03, sym in `AAPL`MSFT

/ OHLCV bars (if loaded)
select from ohlcv_1m where date = 2024.06.03, sym = `AAPL
```

### Adjusted close prices

```q
\l hdb
\l code/adjlib/adjlib.q

/ Raw (unadjusted) bars
getUnadjusted[`ohlcv_1m; `AAPL; 2024.06.03; 2024.06.05]

/ Bars with adj_close column appended
getAdjustedClose[`AAPL; 2024.06.03; 2024.06.05; `split]
```

---

## HDB Layout

After loading data the HDB looks like:

```
hdb/
├── 2024.06.03/
│   ├── trades/         ← splayed table (one file per column)
│   │   ├── sym
│   │   ├── time
│   │   ├── price
│   │   └── ...
│   └── ohlcv_1m/       ← present if ohlcv-1m was requested
├── 2024.06.04/
│   └── ...
└── sym                 ← symbol enumeration file (shared)
```

---

## Running Tests

```bash
# All unit tests
for f in tests/test_schema.q tests/test_manifest.q tests/test_loader.q \
          tests/test_ref_tables.q tests/test_adj.q; do
    q $f -q
done

# Integration test (requires a loaded HDB partition)
INTEGRATION_TEST_DATE=2024-06-03 \
INTEGRATION_TEST_SYM=AAPL \
q tests/test_integration.q -q
```

---

## Troubleshooting

| Symptom | Cause | Fix |
|---------|-------|-----|
| `LD_LIBRARY_PATH: unbound variable` | `set -u` with unset var | Already fixed — `setenv.sh` now uses `${LD_LIBRARY_PATH:-}` |
| `TORQHOME = ` (empty) | `setenv.sh` sourced before `cd grad-project` | Always source from inside `grad-project/` |
| `DATABENTO_API_KEY is not set` | Key not exported | `export DATABENTO_API_KEY="db-..."` before sourcing |
| `q loader exited 1` | q can't find `schema/schema.q` | Ensure you're running from inside `grad-project/` |
| Empty HDB partitions | Date not extracted from filename | Fixed — `infer_date_from_filename` now uses regex |
| Chunk shows `failed` in status | API error or cost limit hit | Run `retry_failed.sh`; check error in `staging/metadata/jobs/` |
| Old stale manifest causes validation error | CSV file deleted but manifest remains | Safe to ignore — logged as a warning, does not block other chunks |
