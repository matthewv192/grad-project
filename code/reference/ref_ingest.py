"""
ref_ingest.py — STUB: reference data ingestion for security master,
                corporate actions, and adjustment factors.

Milestone 3: This stub generates synthetic reference data for a set of test
symbols and writes CSVs that the q loader (ref_tables.q) can read.

In a production setup, replace the generate_* functions below with calls to
your actual reference data provider (Bloomberg, Refinitiv, ICE, etc.).

Output files (written to staging/reference/):
  security_master.csv
  corp_actions.csv
  adj_factors.csv
"""

import csv
import logging
import os
import sys
from datetime import date, timedelta

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}',
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
log = logging.getLogger(__name__)

STAGING_DIR = os.environ.get("STAGING_DIR", os.path.join(os.path.dirname(__file__), "../../staging"))
REF_DIR = os.path.join(STAGING_DIR, "reference")

# Test symbols — must match whatever is used in the integration tests
TEST_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]


def generate_security_master(symbols: list[str]) -> list[dict]:
    """Return a list of dummy security master rows."""
    rows = []
    for i, sym in enumerate(symbols):
        rows.append({
            "sym": sym,
            "instrument_id": 1000 + i,
            "name": f"{sym} Inc.",
            "exchange": "XNAS",
            "currency": "USD",
            "valid_from": "2000-01-01",
            "valid_to": "9999-12-31",
        })
    return rows


def generate_corp_actions(symbols: list[str]) -> list[dict]:
    """Return synthetic corporate action events.

    For testing purposes, AAPL gets a 2:1 split on 2024-06-10 (factor=0.5
    means pre-split prices are halved when looking forward).
    """
    rows = []
    # One stock split for AAPL — used in test_adj.q to verify the adjustment logic
    rows.append({
        "sym": "AAPL",
        "action_type": "split",
        "ex_date": "2024-06-10",
        "record_date": "2024-06-09",
        "effective_date": "2024-06-10",
        "factor": 0.5,          # post/pre ratio — 2:1 split means factor = 0.5
        "description": "2-for-1 stock split (synthetic test data)",
    })
    # One dividend for MSFT
    rows.append({
        "sym": "MSFT",
        "action_type": "dividend",
        "ex_date": "2024-05-15",
        "record_date": "2024-05-14",
        "effective_date": "2024-05-15",
        "factor": 0.998,        # small dividend adjustment
        "description": "Quarterly dividend (synthetic test data)",
    })
    return rows


def generate_adj_factors(symbols: list[str], start: date, end: date) -> list[dict]:
    """Generate daily cumulative adjustment factors for each symbol.

    For days before the AAPL split ex_date, cumulative_factor = 0.5.
    For days on/after the split, cumulative_factor = 1.0.
    All other symbols: cumulative_factor = 1.0 throughout.
    """
    rows = []
    aapl_split_date = date(2024, 6, 10)
    d = start
    while d <= end:
        for sym in symbols:
            if sym == "AAPL" and d < aapl_split_date:
                cum = 0.5
                split_f = 0.5
            else:
                cum = 1.0
                split_f = 1.0
            rows.append({
                "sym": sym,
                "date": d.isoformat(),
                "cumulative_factor": cum,
                "split_factor": split_f,
                "dividend_factor": 1.0,
            })
        d += timedelta(days=1)
    return rows


def generate_symbology_map(symbols: list[str]) -> list[dict]:
    """Return stub symbology mapping rows (instrument_id → sym, per dataset).

    In production, replace with real Databento instrument definition data.
    The loader auto-populates staging/reference/symbology_map.csv during backfill;
    this stub is only needed for tests or pre-seeding the map.
    """
    rows = []
    for i, sym in enumerate(symbols):
        rows.append({
            "sym": sym,
            "instrument_id": 1000 + i,
            "dataset": "XNAS.ITCH",
            "valid_from": "2000-01-01",
            "valid_to": "9999-12-31",
        })
    return rows


def write_csv(path: str, rows: list[dict]) -> None:
    if not rows:
        log.warning(f"No rows to write to {path}")
        return
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=rows[0].keys())
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Wrote {len(rows)} rows to {path}")


def main():
    """Generate all synthetic reference CSVs."""
    log.info("Generating synthetic reference data (STUB)")

    sm = generate_security_master(TEST_SYMBOLS)
    write_csv(os.path.join(REF_DIR, "security_master.csv"), sm)

    ca = generate_corp_actions(TEST_SYMBOLS)
    write_csv(os.path.join(REF_DIR, "corp_actions.csv"), ca)

    # Generate factors for a range spanning the synthetic split date
    af = generate_adj_factors(
        TEST_SYMBOLS,
        start=date(2024, 1, 1),
        end=date(2024, 12, 31),
    )
    write_csv(os.path.join(REF_DIR, "adj_factors.csv"), af)

    sm_map = generate_symbology_map(TEST_SYMBOLS)
    write_csv(os.path.join(REF_DIR, "symbology_map.csv"), sm_map)

    log.info("Reference data generation complete")


if __name__ == "__main__":
    main()
