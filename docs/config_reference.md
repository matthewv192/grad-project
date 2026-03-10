# Configuration Reference

All settings live in `config/settings.q` and can be overridden by environment variables set before `setenv.sh` is sourced.

---

## Databento / Dataset

| Setting (q var) | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_DATASET` | — | `XNAS.ITCH` | Default Databento dataset identifier (overridden per-run by `--dataset`) |
| `BACKFILL_DEFAULT_SCHEMA` | — | `trades` | Schema used when `--schema` not specified |
| `BACKFILL_SCHEMAS` | — | `` `trades`ohlcv_1m `` | Schemas considered valid (validation only) |

Supported dataset/schema combinations (tested):

| Dataset | trades | ohlcv-1m | Notes |
|---|---|---|---|
| `XNAS.ITCH` | ✅ | ✅ | NASDAQ |
| `XNYS.PILLAR` | ✅ | ✅ | NYSE |
| `IEXG.TOPS` | — | ✅ | IEX |
| `EQUS.MINI` | — | ✅ | Equity US Mini |

Any Databento dataset providing `trades` or `ohlcv-1m` schema should work. The dataset name is written to the `exchange` column in each HDB table.

The `--dataset` CLI flag sets the dataset for a single orchestrator run. Multiple exchanges can be loaded into the same HDB partition by running the orchestrator once per dataset.

---

## Chunking

| Setting | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_CHUNK_SIZE` | `BACKFILL_CHUNK_SIZE` | `10` | Symbols per Databento batch job. Smaller = finer-grained retry granularity, more API jobs. |

---

## Cost Safeguard

| Setting | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_MAX_COST_USD` | `BACKFILL_MAX_COST_USD` | `50.0` | Max spend per orchestrator run. Set to `0` to disable (not recommended). |

The orchestrator calls the Databento metadata API before submitting each batch job. If the estimated cost would push the total over this limit, it logs an error and aborts without submitting.

If the metadata API returns a Databento error (e.g. unknown dataset), estimation returns `0.0` and the safeguard is disabled for that chunk with a warning. Unexpected errors (network fault, SDK bug) abort the run so the safeguard is never silently bypassed.

---

## Retry Policy

| Setting | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_MAX_RETRIES` | `BACKFILL_MAX_RETRIES` | `3` | Max retry attempts before a chunk is abandoned. |

Backoff: `min(2^retries, 60)` seconds between attempts (1s, 2s, 4s, …, capped at 60s).

---

## Paths

| Setting | Env var | Default | Description |
|---|---|---|---|
| `STAGING_DIR` | `STAGING_DIR` | `<package>/staging` | Root for downloaded CSVs, manifests, and job store |
| `HDB_DIR` | `KDBHDB` | `<package>/hdb` | kdb+ HDB root directory |
| `JOB_METADATA_DIR` | — | `<staging>/metadata` | Job store and manifest location |

---

## Logging

| Setting | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_LOG_LEVEL` | — | `1` (INFO) | TorQ log level: 0=DEBUG 1=INFO 2=WARN 3=ERROR |

Python logging uses JSON format on stdout. Redirect to `logs/` if needed:
```bash
./scripts/request_backfill.sh ... >> logs/backfill.log 2>&1
```

---

## CLI Flags (orchestrator.py / request_backfill.sh)

| Flag | Required | Description |
|---|---|---|
| `--symbols` | Yes | Comma-separated symbol list, e.g. `AAPL,MSFT` |
| `--start` | Yes | Start date `YYYY-MM-DD` (inclusive) |
| `--end` | Yes | End date `YYYY-MM-DD` (inclusive) |
| `--schema` | No | `trades` or `ohlcv-1m` (default: `trades`) |
| `--dataset` | No | Databento dataset identifier (default: `XNAS.ITCH`). The dataset name is stored as the `exchange` column in the HDB. |
| `--chunk-size` | No | Symbols per chunk (default: `10`) |
| `--workers` | No | Parallel chunk workers (default: `4`). Each worker runs the full submit→poll→download pipeline for one chunk concurrently. |
| `--request-id` | No | Override auto-generated ID. If a record already exists with different parameters the run aborts; matching parameters are treated as an idempotent resume. |
| `--retry-failed` | No | Only retry `failed` chunks from the job store |
| `--dry-run` | No | Estimate cost and print chunk plan; do not submit or download |
| `--download-only` | No | Download and stage only; do not invoke the q loader |
| `--load-only` | No | Skip API calls; run the q loader on existing staged manifests |
| `--metrics` | No | Print per-chunk timing metrics and exit |

---

## Manifest JSON Schema

Each downloaded chunk produces a manifest file in `staging/metadata/manifests/`. The q loader reads these to know what to load.

| Key | Type | Description |
|---|---|---|
| `request_id` | string | Orchestrator run identifier |
| `chunk_id` | string | Unique per (request, date, batch) |
| `databento_job_id` | string | Databento job reference |
| `exchange` | string | Databento dataset (e.g. `XNAS.ITCH`) — becomes the `exchange` column in the HDB |
| `schema` | string | `trades` or `ohlcv-1m` |
| `date` | string | Partition date `YYYY-MM-DD` |
| `symbols` | array | Tickers in this chunk |
| `file_path` | string | Absolute path to the staged CSV |
| `row_count` | integer | Expected row count (used for verification) |
| `checksum` | string | SHA-256 of the CSV for integrity checking |
| `min_ts` / `max_ts` | string | Timestamp range (empty if not populated) |
| `created_at` | string | ISO 8601 creation timestamp |
