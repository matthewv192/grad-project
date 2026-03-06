# Known Limitations

## What is Stubbed

| Component | Status | Milestone |
|---|---|---|
| `orchestrator.py` | Skeleton only — no API calls | Milestone 1 |
| `manifest.q` — `loadChunk` dispatch | Stub call commented out | Milestone 1 |
| `loader.q` — `runLoader` trigger | Not wired to TorQ process | Milestone 1 |
| `backfill_status.sh` | Prints placeholder | Milestone 2 |
| Job store persistence | Not implemented | Milestone 2 |
| Chunking logic | Not implemented | Milestone 2 |
| Idempotency checks | Not implemented | Milestone 2 |
| `ref_ingest.py` | Generates synthetic data only | Milestone 3 |
| `ref_tables.q` — live queries | Reads from CSVs, not a live provider | Milestone 3 |
| `adjlib.q` — `applyAdj` / `getAdjustedClose` / `getUnadjusted` | Return stubs | Milestone 4 |
| `test_adj.q` — full assertions | Commented out pending Milestone 4 | Milestone 4 |
| `test_integration.q` — all assertions | Commented out pending Milestone 1 | Milestone 1 |

---

## Cost Considerations

- Databento charges per gigabyte of data returned. XNAS.ITCH `trades` data can be large for liquid symbols over long date ranges.
- The `BACKFILL_MAX_COST_USD` safeguard (default $50) will block requests that exceed the threshold **before** submitting.
- Always use `--dry-run` first when testing with a new date range or symbol universe.
- OHLCV data is significantly cheaper than tick-level trades data for the same period.

---

## Data Scope

- Only `XNAS.ITCH` (NASDAQ) is configured out of the box. Adding other datasets requires updating `config/settings.q` and the column maps in `loader.q`.
- Extended hours trades are included in Databento's `trades` schema. Filter on `time` if you only want regular session data.

---

## TorQ Integration

- The package uses TorQ environment variable conventions but does not currently start via `torq.sh`. Processes are run manually or via the shell scripts in `scripts/`.
- `config/process.csv` defines the `hdb` and `loader` process entries but they are not wired into a full TorQ stack yet.

---

## Not Supported

- Real-time / live data ingestion — batch only
- Non-equity asset classes (futures, options, FX) — untested
- Databento streaming API — not used
- Windows — shell scripts are bash only; use WSL
