# Architecture

End-to-end data flow from user request to kdb+ HDB partition.

---

## Pipeline Overview

### High-Level Flow
```mermaid
flowchart LR
    User[👤 User/Cron] --> Shell[Shell Scripts]
    Shell --> Python[Python Orchestrator]
    Python <--> API[☁ Databento API]
    Python --> Stage[(Staging)]
    Stage --> qLoad[q Loader]
    qLoad --> HDB[(HDB)]
```

### Detailed Component View

**1. Request Orchestration (Python)**
```mermaid
flowchart TD
    A[orchestrator.py] --> B[Chunk by day × symbol]
    B --> C[Cost guard < $50]
    C --> D[Fetch from Databento]
    D --> E[Write manifest + job]
```

**2. Data Loading (q)**
```mermaid
flowchart TD
    A[loader.q] --> B[Scan manifests]
    B --> C[Parse CSV + quality checks]
    C --> D[.Q.dpft write partition]
```

**3. Storage Layout**
```
staging/
├── <chunk_id>/          # Raw CSV data (one dir per chunk)
├── metadata/
│   ├── manifests/       # Load instructions (one JSON per chunk)
│   ├── jobs/            # Job status tracking (one JSON per chunk)
│   └── archive/         # Verified manifests (moved after load)
├── metrics/             # Per-chunk timing metrics
└── reference/           # Symbology maps

hdb/
└── YYYY.MM.DD/          # Partitioned by date
    ├── trades/
    └── ohlcv_1m/
```

**4. Key Scripts**
- **Shell**: `bin/backfill` (entrypoint — sources env, activates venv, calls orchestrator)
- **Python**: `orchestrator.py` (API client, cost control)
- **q**: `loader.q`, `manifest.q`, `quality.q`, `adjlib.q`
- **Reference**: `ref_ingest.py`, `ref_tables.q`

---

## Component Responsibilities

### Python Layer

| Component | Responsibility |
|---|---|
| `orchestrator.py` | Chunk generation, cost guard, Databento API calls (submit/poll/download), manifest writing, job store management, q loader invocation |
| `metrics.py` | Per-chunk timing across pipeline stages; aggregated `summary.json` per request |

### Staging Directory (Python ↔ q bridge)

All communication between Python and q goes through files on disk. Python never calls q directly except to pipe `runLoader[]` over stdin.

| Path | Written by | Read by |
|---|---|---|
| `chunks/<id>/*.csv` | `download_csv()` | `loadChunkBatch()` |
| `metadata/manifests/*.json` | `write_manifest()` | `processManifests()` |
| `metadata/jobs/*.json` | `orchestrator.py` | `updateJobRecord()`, status scripts |
| `metrics/<req>/<chunk>.json` | `metrics.py` (stub) | `updateMetrics()` (q fills timing) |
| `reference/symbology_map.csv` | `flushSymbologyMap()` | `ref_tables.q` → `resolveSymbol()` |

### q Loader

| Component | Responsibility |
|---|---|
| `manifest.q` | Scan manifest dir, validate each manifest, check job store for skip/retry |
| `loader.q` | Parse CSVs, merge multi-exchange partitions, write HDB via `.Q.dpft`, update job records and metrics |
| `quality.q` | Duplicate detection (exchange-aware), time ordering, null checking |

### kdb+ HDB

Partitioned by `date`, splayed tables sorted by `` `sym`time `` within each partition. The `exchange` column identifies the data source. Multiple exchanges coexist in the same partition date; idempotency is exchange-aware.

### Reference Data & Adjustments

| Component | Responsibility |
|---|---|
| `ref_ingest.py` | Generate synthetic security master, corp actions, and adj factor CSVs |
| `ref_tables.q` | Load reference CSVs into in-memory tables; `resolveSymbol()` / `resolveInstrumentId()` for point-in-time lookups |
| `adjlib.q` | `applyAdj()` applies split/dividend factors; `getAdjustedClose()` returns OHLCV + `adj_close` column in `backward` (post-split) or `forward` (pre-split) terms |

---

## Job Status Lifecycle

```mermaid
stateDiagram-v2
    [*] --> pending
    pending --> submitted : estimate_cost() OK
    submitted --> running : submit_job() returns job_id
    running --> downloaded : download_csv() complete
    downloaded --> loaded : write_manifest() complete
    loaded --> verified : quality checks pass + row_count match
    loaded --> failed : quality check failure
    running --> failed : poll timeout / API error
    submitted --> failed : submit error / cost limit
    failed --> pending : --retry-failed resets status
```

---

## Idempotency & Fault Tolerance

- **Chunk-level idempotency**: each `(date, symbol-batch, exchange)` chunk is independent. A crash in chunk N does not affect chunk N+1.
- **Exchange-aware idempotency**: re-running a load for an already-loaded `(date, exchange)` pair is detected and skipped without re-writing data.
- **Write locking**: `acquireWriteLock()` uses atomic POSIX `mkdir` to prevent concurrent `.Q.dpft` calls on the same partition.
- **Atomic writes**: all JSON updates (job records, metrics, manifests) write to a `.tmp` file and then `mv` to the final path.
- **Checksum verification**: on resume after a crash-during-download, the stored SHA-256 is re-verified before skipping the re-download.
