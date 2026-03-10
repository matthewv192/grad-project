# Architecture

End-to-end data flow from user request to kdb+ HDB partition.

---

## Pipeline Overview

```mermaid
flowchart TD
    User(["👤 User / Cron"])

    subgraph Shell["Shell Layer"]
        direction LR
        RBS["request_backfill.sh"]
        BSS["backfill_status.sh\nretry_failed.sh"]
    end

    subgraph Python["Python — orchestrator.py + metrics.py"]
        direction TB
        CHUNK["① Chunk generation<br/>1 day × 1 symbol-batch per chunk"]
        COST["② Cost guard<br/>estimate_cost() — aborts if &gt; $50"]
        SUBMIT["③ Submit batch job<br/>submit_job()"]
        POLL["④ Poll until done<br/>poll_until_done()"]
        DOWNLOAD["⑤ Download CSV<br/>download_csv()"]
        MANIFEST["⑥ Write manifest + job record<br/>write_manifest()"]
        CHUNK --> COST --> SUBMIT --> POLL --> DOWNLOAD --> MANIFEST
    end

    DAPI(["☁ Databento<br/>Historical API"])

    subgraph Staging["Staging — staging/"]
        direction TB
        CSV[("chunks/‹chunk_id›/*.csv<br/>downloaded trade / OHLCV data")]
        MAN[("metadata/manifests/*.json<br/>load instructions for q")]
        JOBS[("metadata/jobs/*.json<br/>job store — status lifecycle")]
        MET[("metrics/‹req›/‹chunk›.json<br/>per-stage timing")]
        SYM[("reference/symbology_map.csv<br/>sym ↔ instrument_id ↔ exchange")]
    end

    subgraph qLoader["q Loader — loader.q  manifest.q  quality.q"]
        direction TB
        PM["processManifests()<br/>manifest.q — scan, validate, dispatch"]
        LC["loadChunk()<br/>loader.q — route by schema"]
        LT["loadTrades()"]
        LO["loadOhlcv()"]
        QC["runQualityChecks()<br/>quality.q — dups · ordering · nulls"]
        DPFT[".Q.dpft()<br/>write splayed HDB partition"]
        UPD["updateJobRecord()<br/>updateJobRecordTimestamps()"]
        PM --> LC
        LC --> LT & LO
        LT & LO --> DPFT
        LC --> QC
        LC --> UPD
    end

    subgraph HDB["kdb+ HDB — hdb/"]
        direction LR
        PART["YYYY.MM.DD/"]
        TRADES["trades/<br/>splayed · sorted sym·time"]
        OHLCV["ohlcv_1m/<br/>splayed · sorted sym·time"]
        SYMFILE[("sym<br/>enum file")]
        PART --> TRADES & OHLCV
    end

    subgraph AdjRef["Reference Data & Adjustments"]
        direction TB
        REFINGEST["ref_ingest.py<br/>generate synthetic reference CSVs"]
        REFTABLES["ref_tables.q<br/>security master · corp actions<br/>adj factors · symbology map"]
        ADJLIB["adjlib.q<br/>applyAdj()  getAdjustedClose()<br/>backward / forward methods"]
        REFINGEST --> REFTABLES --> ADJLIB
    end

    User --> Shell
    RBS --> Python
    BSS -.->|"reads job records"| JOBS
    Python <-->|"submit · poll · download"| DAPI
    DOWNLOAD -->|"staged CSV"| CSV
    MANIFEST -->|"manifest"| MAN
    Python -->|"job records"| JOBS
    Python -->|"timing stub"| MET
    Python -->|"stdin: runLoader[]"| PM
    PM -.->|"reads"| MAN
    PM -.->|"CSV path via manifest"| CSV
    UPD -->|"status · timestamps"| JOBS
    LC -->|"load timing"| MET
    LC -->|"flush sym·instrument_id pairs"| SYM
    DPFT -->|"splayed partition"| PART
    DPFT -->|"enum update"| SYMFILE
    REFTABLES -.->|"reads"| SYM
    ADJLIB -.->|"queries ohlcv_1m / trades"| HDB
```

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
| `chunks/‹id›/*.csv` | `download_csv()` | `loadTrades()` / `loadOhlcv()` |
| `metadata/manifests/*.json` | `write_manifest()` | `processManifests()` |
| `metadata/jobs/*.json` | `orchestrator.py` | `updateJobRecord()`, status scripts |
| `metrics/‹req›/‹chunk›.json` | `metrics.py` (stub) | `updateMetrics()` (q fills timing) |
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
