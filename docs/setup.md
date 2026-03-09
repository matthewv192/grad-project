# Setup Guide

## Prerequisites

| Requirement | Version | Notes |
|---|---|---|
| kdb+ / kdb-x | 5.0 | Must be on `PATH` as `q` |
| Python | ≥ 3.10 | |
| TorQ | latest main | Cloned to `../TorQ` relative to this package |
| Databento account | — | API key required for real data |

---

## 1. Clone and position the repos

```bash
# Both repos must sit side-by-side
git clone https://github.com/AquaQAnalytics/TorQ.git
git clone <this-repo> grad-project
```

Directory layout expected:
```
parent/
├── TorQ/
└── grad-project/
```

---

## 2. Set up Python environment

```bash
cd ..   # parent directory
python3 -m venv venv
source venv/bin/activate
pip install -e grad-project/
```

---

## 3. Configure environment

```bash
cd grad-project

# Set your Databento API key before sourcing
export DATABENTO_API_KEY="your-key-here"

# Load all environment variables
source setenv.sh
```

---

## 4. Run your first backfill (dry run)

```bash
# Estimate cost and print the chunk plan — no API calls are submitted
./scripts/request_backfill.sh \
    --symbols "AAPL,MSFT" \
    --start 2024-01-15 \
    --end 2024-01-15 \
    --schema trades \
    --dry-run
```

---

## 5. Run a real backfill

```bash
./scripts/request_backfill.sh \
    --symbols "AAPL" \
    --start 2024-01-15 \
    --end 2024-01-15 \
    --schema trades
```

Downloaded files land in `staging/<request_id>/`.

---

## 6. Load into the HDB

```bash
q code/backfill/loader.q -e "runLoader[];exit 0"
```

HDB partitions are written to `hdb/`.

---

## 7. Query the HDB

```bash
q hdb
```

```q
select from trades where date=2024.01.15, sym=`AAPL
```

---

## 8. Run unit tests

```bash
q tests/test_schema.q
q tests/test_manifest.q
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
