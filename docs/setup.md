# Setup Guide

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| kdb+ / kdb-x | 5.0 | Must be on `PATH` as `q` |
| Python | ≥ 3.10 | |
| TorQ | latest main | Cloned alongside this package (see layout below) |
| Databento account | — | API key required for real data |

---

## 1. Clone and position the repos

Both repos must sit side-by-side under the same parent directory:

```bash
git clone https://github.com/AquaQAnalytics/TorQ.git ~/TorQ
git clone <this-repo> ~/grad-project
```

Expected layout:
```
~/
├── TorQ/
├── grad-project/
└── venv/          ← created in step 2
```

---

## 2. Set up Python environment

Run the setup script, which creates the virtual environment and installs all dependencies in one step:

```bash
cd ~/grad-project
./scripts/setup_python.sh
```

Or manually:

```bash
python3 -m venv ../venv
source ../venv/bin/activate
pip install -e .
```

---

## 3. Configure environment

```bash
cd ~/grad-project
source setenv.sh
```

Expected output:
```
TORQHOME    = /home/<user>/TorQ
PACKAGEHOME = /home/<user>/grad-project
KDBHDB      = /home/<user>/grad-project/hdb
```

`setenv.sh` sets `TORQHOME`, `PACKAGEHOME`, `KDBHDB`, `STAGING_DIR`, and the Databento API key. The `bin/backfill` entrypoint sources it automatically — manual sourcing is only needed for interactive q sessions.

---

## 4. Run a dry run (no API calls)

```bash
./bin/backfill \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 \
    --end 2024-01-17 \
    --schema ohlcv-1m \
    --dry-run
```

This prints a cost estimate and chunk plan without submitting any Databento jobs.

---

## 5. Run a real backfill

```bash
./bin/backfill \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 \
    --end 2024-01-17 \
    --schema ohlcv-1m
```

Downloaded CSVs are staged in `staging/<chunk_id>/` (one directory per chunk, named by symbol and date). Manifests are written to `staging/metadata/manifests/`. The q loader is invoked automatically to write data into the HDB.

---

## 6. Load from a second exchange into the same partition

```bash
./bin/backfill \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 \
    --end 2024-01-17 \
    --schema ohlcv-1m \
    --dataset XNYS.PILLAR
```

The loader merges XNYS.PILLAR rows into the existing `hdb/2024.01.17/ohlcv_1m/` partition alongside XNAS.ITCH rows. Each row carries an `exchange` column.

Supported datasets include: `XNAS.ITCH`, `XNYS.PILLAR`, `IEXG.TOPS`, `EQUS.MINI`, and any other Databento dataset that provides `trades` or `ohlcv-1m` schema.

---

## 7. Query the HDB

```bash
cd ~/grad-project
q hdb
```

```q
/ Row counts by exchange for a specific date
select count i by exchange from ohlcv_1m where date=2024.01.17

/ All AAPL trades on a day
select from trades where date=2024.01.17, sym=`AAPL

/ OHLCV bars for MSFT on NASDAQ
select from ohlcv_1m where date=2024.01.17, sym=`MSFT, exchange=`XNAS.ITCH
```

---

## 8. Run unit tests

```bash
cd ~/grad-project
./scripts/run_tests.sh
```

This runs 8 q tests and 2 Python tests. The integration test is skipped by default (no KDBHDB set). All other tests exit 0 on success and require no API key or loaded HDB.

To run an individual test in isolation:

```bash
q tests/test_loader.q
python3 -m pytest tests/test_orchestrator.py -v
```

The integration test is skipped unless `KDBHDB` points to a populated HDB. When enabled it auto-discovers the most recent partition date and first available sym:

```bash
KDBHDB=/path/to/hdb ./scripts/run_tests.sh
```

---

## 9. Check backfill status

```bash
./scripts/backfill_status.sh
```

---

## 10. Retry failed chunks

```bash
./scripts/retry_failed.sh
```
