# grad-project — TorQ Databento Batch Backfill Package

A TorQ-compatible package that downloads historical market data from the
[Databento Historical API](https://databento.com) in batch and loads it into
a kdb+ HDB.

**Status:** Milestone 0 complete — package skeleton in place.

---

## Quick Start

```bash
export DATABENTO_API_KEY="your-key-here"
source setenv.sh

# Dry run (no API calls)
./scripts/request_backfill.sh --symbols "AAPL" --start 2024-01-15 --end 2024-01-15 --dry-run

# Unit tests
q tests/test_schema.q
q tests/test_manifest.q
```

See [docs/setup.md](docs/setup.md) for full setup instructions.

---

## Repository Layout

```
grad-project/
├── setenv.sh               # Set TORQHOME, PACKAGEHOME, DATABENTO_API_KEY
├── pyproject.toml          # Python deps: databento, pandas, pyarrow
├── config/
│   ├── settings.q          # All configurable parameters
│   └── process.csv         # TorQ process definitions
├── code/
│   ├── backfill/
│   │   ├── orchestrator.py # Python: submit/poll/download Databento batch jobs
│   │   ├── manifest.q      # q: read manifests, validate, trigger loader
│   │   └── loader.q        # q: load staged CSV into HDB partitions
│   ├── reference/
│   │   ├── ref_ingest.py   # STUB: generate synthetic reference data CSVs
│   │   └── ref_tables.q    # STUB: load reference CSVs into q tables
│   └── adjlib/
│       └── adjlib.q        # STUB: price/volume adjustment library
├── schema/
│   └── schema.q            # All table schemas (trades, ohlcv_1m, ref, jobs)
├── scripts/
│   ├── request_backfill.sh # Submit a new backfill request
│   ├── backfill_status.sh  # Show job progress
│   └── retry_failed.sh     # Re-queue failed chunks
├── tests/
│   ├── test_schema.q
│   ├── test_manifest.q
│   ├── test_loader.q
│   ├── test_adj.q
│   ├── test_integration.q
│   └── run_integration.sh
└── docs/
    ├── setup.md
    ├── config_reference.md
    └── limitations.md
```

---

## Milestones

| # | Name | Status |
|---|---|---|
| 0 | Package Skeleton | ✅ Complete |
| 1 | Batch Backfill Pipeline | 🔲 Next |
| 2 | Chunking, Idempotency & Resilience | 🔲 Pending |
| 3 | Reference Data Ingestion (Stubbed) | 🔲 Pending |
| 4 | Corporate Action Adjustment Library | 🔲 Pending |

---

## Docs

- [Setup guide](docs/setup.md)
- [Config reference](docs/config_reference.md)
- [Known limitations](docs/limitations.md)
