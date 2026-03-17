# Setup Guide

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| kdb+ / kdb-x | 5.0 | Must be on `PATH` as `q` |
| Python | ≥ 3.10 | |
| Databento account | — | API key required for real data |

> **TorQ is optional.** This package follows TorQ directory conventions but runs standalone. If you want TorQ integration, clone it alongside: `git clone https://github.com/AquaQAnalytics/TorQ.git ~/TorQ`

---

## 1. Clone the repo

```bash
git clone <this-repo> ~/grad-project
```

Expected layout:
```
~/
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

## 3. Add your Databento API key

Open `setenv.sh` and replace the placeholder value on the marked line with your own key. Your key starts with `db-` and can be found at <https://app.databento.com/portal/keys>.

```bash
nano setenv.sh    # look for the line marked "EDIT ME"
```

The line to change looks like this:

```bash
# Databento API key — EDIT ME: replace with your own key
export DATABENTO_API_KEY="db-..."
```

> **Do not** set `DATABENTO_API_KEY` as a shell export before sourcing `setenv.sh` — `setenv.sh` overwrites the variable, so pre-setting it has no effect.

---

## 4. Configure environment

```bash
cd ~/grad-project
source setenv.sh
```

Expected output:
```
PACKAGEHOME = /home/<user>/grad-project
KDBHDB      = /home/<user>/grad-project/hdb
TORQHOME    = (not set — TorQ not found, running standalone)
```

`setenv.sh` sets `PACKAGEHOME`, `KDBHDB`, `STAGING_DIR`, and the Databento API key. If TorQ is cloned alongside, `TORQHOME` is set automatically. The `bin/backfill` entrypoint sources it on every run — manual sourcing is only needed for interactive q sessions.

---

## 5. Run a dry run (no API calls)

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

## 6. Run a real backfill

```bash
./bin/backfill \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 \
    --end 2024-01-17 \
    --schema ohlcv-1m
```

Downloaded CSVs are staged in `staging/<chunk_id>/` (one directory per chunk, named by symbol and date). Manifests are written to `staging/metadata/manifests/`. The q loader is invoked automatically to write data into the HDB.

---

## 7. Load from a second exchange into the same partition

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

## 8. Query the HDB

```bash
cd ~/grad-project
q -q
```

Then in q:

```q
\l hdb
```

> **Note:** `\l hdb` (or `q hdb`) changes q's working directory to `hdb/`. Load any additional scripts (e.g. `adjlib.q`) **before** `\l hdb`, or start with `q -q` and load the HDB explicitly.

```q
/ Row counts by exchange for a specific date
select count i by exchange from ohlcv_1m where date=2024.01.17

/ All AAPL trades on a day
select from trades where date=2024.01.17, sym=`AAPL

/ OHLCV bars for MSFT on NASDAQ
select from ohlcv_1m where date=2024.01.17, sym=`MSFT, exchange=`XNAS.ITCH
```

---

## 9. Run unit tests

```bash
cd ~/grad-project
./scripts/run_tests.sh
```

This runs 8 q tests and 3 Python tests (orchestrator, metrics, ref_ingest). The integration test is skipped by default if the auto-discovered partition has no trades data. All other tests exit 0 on success and require no API key or loaded HDB.

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

## 10. Check backfill status

```bash
./bin/backfill --status
```

Filter to a specific request:

```bash
./bin/backfill --status --request-id req_20240603_120000_abc123
```


---

## 11. Retry failed chunks

```bash
./bin/backfill --retry-failed
```

