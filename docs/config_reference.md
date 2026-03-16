# Configuration Reference

Pipeline parameters (dataset, schema, chunk size, cost limit, retries) are controlled by environment variables read directly by `orchestrator.py`. The only q-side configuration is `STAGING_DIR` and `KDBHDB`, set in `setenv.sh`.

---

## Databento / Dataset

Dataset and schema are specified per-run via CLI flags (see [CLI Flags](#cli-flags-orchestratorpy--request_backfillsh) below). Defaults apply when the flags are omitted.

| Parameter | CLI flag | Default | Description |
|---|---|---|---|
| Dataset | `--dataset` | `XNAS.ITCH` | Databento dataset identifier |
| Schema | `--schema` | `trades` | `trades` or `ohlcv-1m` |
| Symbol type | `--stype-in` | `raw_symbol` | Databento symbol type (`raw_symbol`, `continuous`, `instrument_id`) |

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

Each chunk is one symbol × one exchange × one day — the finest granularity
Databento supports. This means a single symbol/day failure never blocks any
other symbol or day.

| Setting | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_CHUNK_SIZE` | `BACKFILL_CHUNK_SIZE` | `10` | Number of symbols per Databento batch job. Larger values reduce API round-trips; smaller values give finer retry granularity. |

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

Python logging uses JSON format on stdout and is also written to a file in `logs/` for every run:

| Mode | Log file |
|---|---|
| Normal backfill | `logs/<request_id>.log` |
| `--retry-failed` | `logs/retry_<timestamp>.log` |
| `--load-only` | `logs/load_only_<timestamp>.log` |

Redirect stdout to `logs/` if you also want console output captured:
```bash
./bin/backfill ... >> logs/backfill.log 2>&1
```

---

## CLI Flags (bin/backfill / orchestrator.py)

| Flag | Required | Description |
|---|---|---|
| `--symbols` | Yes | Comma-separated symbol list, e.g. `AAPL,MSFT` |
| `--start` | Yes | Start date `YYYY-MM-DD` (inclusive) |
| `--end` | Yes | End date `YYYY-MM-DD` (inclusive) |
| `--schema` | No | `trades` or `ohlcv-1m` (default: `trades`) |
| `--dataset` | No | Databento dataset identifier (default: `XNAS.ITCH`). The dataset name is stored as the `exchange` column in the HDB. |
| `--chunk-size` | No | Symbols per Databento batch job (default: 10). Larger values reduce API round-trips; smaller values give finer retry granularity. |
| `--workers` | No | Parallel chunk workers (default: `12`). Each worker runs the full submit→poll→download pipeline for one chunk concurrently. |
| `--request-id` | No | Override auto-generated ID. If a record already exists with different parameters the run aborts; matching parameters are treated as an idempotent resume. |
| `--retry-failed` | No | Only retry `failed` chunks from the job store |
| `--status` | No | Print a summary of all job statuses and exit; combine with `--request-id` to filter to one request |
| `--dry-run` | No | Estimate cost and print chunk plan; do not submit or download |
| `--download-only` | No | Download and stage only; do not invoke the q loader |
| `--load-only` | No | Skip API calls; run the q loader on existing staged manifests. Does not filter by request ID — processes all pending manifests in the staging directory. |
| `--metrics` | No | Print per-chunk timing metrics and exit |
| `--stype-in` | No | Databento symbol type passed to submit/cost API calls (default: `raw_symbol`). Use `continuous` for continuous futures contracts or `instrument_id` for numeric IDs. |

---

## Manifest JSON Schema

Each downloaded chunk produces a manifest file in `staging/metadata/manifests/`. The q loader reads these to know what to load.

| Key | Type | Description |
|---|---|---|
| `request_id` | string | Orchestrator run identifier |
| `chunk_id` | string | Unique per (request, date, symbol) — format: `<request_id>_<YYYY.MM.DD>_<SYM>` |
| `databento_job_id` | string | Databento job reference |
| `exchange` | string | Databento dataset (e.g. `XNAS.ITCH`) — becomes the `exchange` column in the HDB |
| `schema` | string | `trades` or `ohlcv-1m` |
| `date` | string | Partition date `YYYY-MM-DD` |
| `symbols` | array | Tickers in this chunk |
| `file_path` | string | Absolute path to the staged CSV |
| `row_count` | integer | Expected row count (used for verification) |
| `checksum` | string | SHA-256 of the CSV for integrity checking |
| `min_ts` / `max_ts` | string | Actual timestamp range written to the HDB partition (populated by q loader after `.Q.dpft`; empty until then) |
| `created_at` | string | ISO 8601 creation timestamp |

---

## Job Store Schema

Each chunk has a persistent record in the kdb binary table at `staging/metadata/backfill_jobs`. Updated at every status transition. The table schema is defined in `schema/schema.q` as `backfill_jobs`; reads and writes are handled by `code/backfill/jobstore.q`.

| Column | kdb type | Description |
|---|---|---|
| `chunk_id` | symbol | Unique per (request, date, symbol) — format: `<request_id>_<YYYY.MM.DD>_<SYM>` |
| `request_id` | symbol | Parent request identifier |
| `databento_job_id` | symbol | Databento batch job ID (set after submit) |
| `dataset` | symbol | Databento dataset, e.g. `XNAS.ITCH` |
| `schema` | symbol | `` `trades `` or `` `ohlcv_1m `` |
| `date` | date | Partition date |
| `symbols` | generic list | Ticker list for this chunk |
| `status` | symbol | `pending` → `submitted` → `running` → `downloaded` → `loaded` → `verified` (or `failed`) |
| `retries` | int | Number of failed attempts so far |
| `error_msg` | generic list | Last failure message (empty on success) |
| `failure_type` | symbol | Failure category: `api_error` · `download_error` · `parse_error` · `load_error` · `quality_error` |
| `file_path` | symbol | Primary CSV path |
| `file_paths` | generic list | All CSV paths (multi-file jobs) |
| `checksum` | symbol | SHA-256 of primary CSV |
| `row_count` | long | Data row count from CSV |
| `min_ts` / `max_ts` | timestamp | Actual timestamp range written to HDB (set by q loader) |
| `created_at` / `updated_at` | timestamp | Write timestamps |

### Querying the job store from the CLI

```bash
# Summary of all requests (uses Python status display)
./bin/backfill --status

# Filter to a specific request
./bin/backfill --status --request-id req_20240603_120000_abc123

# Query the raw kdb table directly
echo '{"op":"loadAll"}' | JOBS_FILE=$(pwd)/staging/metadata/backfill_jobs q -q code/backfill/jobstore.q
```
