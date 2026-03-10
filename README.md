# grad-project — TorQ Databento Batch Backfill Package

A TorQ-compatible package that downloads historical market data from the
[Databento Historical API](https://databento.com) in batch and loads it into
a kdb+ HDB. Supports multiple exchanges in the same HDB partition.

**Status:** All milestones complete — production-ready for multi-exchange backfill.

---

## Quick Start

```bash
export DATABENTO_API_KEY="your-key-here"
source setenv.sh

# Dry run (estimate cost, no API calls)
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dry-run

# Real backfill — NASDAQ
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m

# Add a second exchange to the same partition
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" \
    --start 2024-01-17 --end 2024-01-17 \
    --schema ohlcv-1m --dataset XNYS.PILLAR

# Query across exchanges
q hdb <<< 'select count i by exchange from ohlcv_1m where date=2024.01.17'

# Unit tests
q tests/test_schema.q && q tests/test_manifest.q && \
q tests/test_loader.q && q tests/test_symbology.q
```

See [docs/how_to_run.md](docs/how_to_run.md) for the full guide.

---

## Repository Layout

```
grad-project/
├── setenv.sh               # Set TORQHOME, PACKAGEHOME, KDBHDB, STAGING_DIR
├── pyproject.toml          # Python deps: databento, pandas, pyarrow
├── config/
│   ├── settings.q          # All configurable parameters
│   └── process.csv         # TorQ process definitions (hdb:6010, loader:6011)
├── code/
│   ├── backfill/
│   │   ├── orchestrator.py # Python: submit/poll/download Databento batch jobs
│   │   ├── manifest.q      # q: read manifests, validate, dispatch to loader
│   │   ├── loader.q        # q: parse CSVs, merge partitions, write HDB
│   │   └── quality.q       # q: data quality checks (dups, ordering, nulls)
│   ├── reference/
│   │   ├── ref_ingest.py   # Generate synthetic reference data CSVs
│   │   └── ref_tables.q    # Load reference CSVs into q tables
│   └── adjlib/
│       └── adjlib.q        # Price/volume adjustment library
├── schema/
│   └── schema.q            # All table schemas (trades, ohlcv_1m, ref_*, jobs)
├── scripts/
│   ├── request_backfill.sh # Submit a new backfill request
│   ├── backfill_status.sh  # Show job progress
│   └── retry_failed.sh     # Re-queue failed chunks
├── tests/
│   ├── test_schema.q
│   ├── test_manifest.q
│   ├── test_loader.q
│   ├── test_symbology.q
│   ├── test_adj.q
│   └── test_integration.q
└── docs/
    ├── how_to_run.md
    ├── setup.md
    ├── config_reference.md
    └── limitations.md
```

---

## Milestones

| # | Name | Status |
|---|---|---|
| 0 | Package Skeleton | ✅ Complete |
| 1 | Batch Backfill Pipeline | ✅ Complete |
| 2 | Chunking, Idempotency & Resilience | ✅ Complete |
| 3 | Reference Data Ingestion | ✅ Complete |
| 4 | Corporate Action Adjustment Library | ✅ Complete |
| — | Multi-Exchange HDB | ✅ Complete |

---

## Key Capabilities

- **Multi-exchange partitions** — XNAS.ITCH, XNYS.PILLAR, IEXG.TOPS, EQUS.MINI (and others) can coexist in the same HDB partition date. Each row carries an `exchange` column.
- **Idempotent loads** — Re-running the same request skips already-verified chunks. Supplying a duplicate `--request-id` with different parameters aborts with a clear error.
- **Job store** — Every chunk tracks status (`pending` → `downloaded` → `loaded` → `verified`) in `staging/metadata/jobs/`.
- **Cost safeguard** — `BACKFILL_MAX_COST_USD` (default $50) blocks over-budget requests before API submission. Unexpected cost-estimation failures abort the run rather than silently disabling the guard.
- **Quality checks** — Duplicate detection (exchange-aware), ordering validation, and null checking on every loaded chunk. Missing schema columns are logged as errors.
- **Symbology map** — `staging/reference/symbology_map.csv` accumulates `(sym, instrument_id, exchange)` pairs across all loads, written once per run.
- **UTC enforcement** — The q loader subprocess always runs with `TZ=UTC` set, preventing timestamp corruption on non-UTC hosts.
- **Metrics continuity** — A stub metrics record is written at chunk start, so a mid-run crash never leaves a gap in `staging/metrics/`.

---

## Docs

- [Architecture diagram](docs/architecture.md)
- [How to run](docs/how_to_run.md)
- [Setup guide](docs/setup.md)
- [Config reference](docs/config_reference.md)
- [Known limitations](docs/limitations.md)
