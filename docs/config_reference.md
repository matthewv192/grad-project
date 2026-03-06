# Configuration Reference

All settings live in `config/settings.q` and can be overridden by environment variables set before `setenv.sh` is sourced.

---

## Databento / Dataset

| Setting (q var) | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_DATASET` | — | `XNAS.ITCH` | Databento dataset identifier |
| `BACKFILL_DEFAULT_SCHEMA` | — | `trades` | Schema used when `--schema` not specified |
| `BACKFILL_SCHEMAS` | — | `` `trades`ohlcv_1m `` | Schemas considered valid (validation only) |

Supported dataset/schema combinations:
- `XNAS.ITCH` + `trades` — individual trade prints
- `XNAS.ITCH` + `ohlcv-1m` — one-minute OHLCV bars

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

---

## Retry Policy

| Setting | Env var | Default | Description |
|---|---|---|---|
| `BACKFILL_MAX_RETRIES` | `BACKFILL_MAX_RETRIES` | `3` | Max retry attempts before a chunk is abandoned. |

Backoff: `2^retries` seconds between attempts (1s, 2s, 4s).

---

## Paths

| Setting | Env var | Default | Description |
|---|---|---|---|
| `STAGING_DIR` | `STAGING_DIR` | `<package>/staging` | Root for downloaded files and manifests |
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

## CLI Flags (orchestrator.py)

| Flag | Required | Description |
|---|---|---|
| `--symbols` | Yes | Comma-separated symbol list, e.g. `AAPL,MSFT` |
| `--start` | Yes | Start date `YYYY-MM-DD` (inclusive) |
| `--end` | Yes | End date `YYYY-MM-DD` (inclusive) |
| `--schema` | No | `trades` or `ohlcv-1m` (default: `trades`) |
| `--dataset` | No | Databento dataset (default: `XNAS.ITCH`) |
| `--chunk-size` | No | Symbols per chunk (default: `10`) |
| `--request-id` | No | Override auto-generated ID (useful for reruns) |
| `--retry-failed` | No | Only retry `failed` chunks from job store |
| `--dry-run` | No | Estimate cost and print plan; do not submit |
