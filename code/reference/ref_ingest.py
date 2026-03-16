"""
ref_ingest.py — corporate action and adjustment factor ingestion.

Fetches real event history from yfinance and computes daily cumulative
backward-adjustment factors for any set of symbols over any date range.

Called automatically by orchestrator.py after a successful backfill so that
ref_adj_factors is always aligned to whatever was just loaded into the HDB.
Can also be run standalone via CLI.

Output files (written to staging/reference/):
  corp_actions.csv  — raw event history (splits and dividends)
  adj_factors.csv   — daily cumulative backward-adjustment factors
  security_master.csv — stub (no free provider; populated with placeholders)

Adjustment convention (matching adjlib.q):
  adjusted_price = raw_price * cumulative_factor
  For backward adjustment: cumulative_factor < 1.0 on pre-event dates.
  e.g. a 4:1 split on 2020-08-31 → cumulative_factor = 0.25 for dates before it.

Factor computation uses ALL historical events for each symbol, not just those
in the requested date range.  A split in 2024 affects the factor for 2022 data,
so the full event history must be considered when computing any historical factor.
"""

import argparse
import csv
import logging
import os
import sys
from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

log = logging.getLogger(__name__)

_LOG_FMT = '{"time":"%(asctime)s","level":"%(levelname)s","msg":"%(message)s"}'

try:
    import yfinance as yf
    _YF_AVAILABLE = True
except ImportError:
    _YF_AVAILABLE = False


# ---------------------------------------------------------------------------
# Corp action fetching
# ---------------------------------------------------------------------------

def fetch_corp_actions(symbols: list[str]) -> list[dict]:
    """Fetch ALL historical corp action events for the given symbols via yfinance.

    Intentionally fetches the full history (not just the requested date range)
    because cumulative backward-adjustment factors for historical dates depend
    on all future events.  e.g. a 2024 4:1 split changes the factor for 2022.

    Returns a list of dicts matching the ref_corp_actions CSV schema.
    Failures per-symbol are logged as warnings and skipped.
    """
    if not _YF_AVAILABLE:
        log.warning("yfinance is not installed — corp action fetching unavailable. "
                    "Install with: pip install yfinance")
        return []

    loaded_at = _now_kdb()
    rows = []
    for sym in symbols:
        try:
            ticker = yf.Ticker(sym)

            # ---- Splits ----
            # yfinance returns ratio as new/old (e.g. 4.0 for a 4:1 split).
            # Our factor convention: factor = 1/ratio so pre-split prices
            # are multiplied down to post-split (current) terms.
            splits = ticker.splits
            for dt, ratio in splits.items():
                ex_d = dt.date() if hasattr(dt, "date") else dt
                ratio_f = float(ratio)
                if ratio_f <= 0:
                    continue
                rows.append({
                    "sym": sym,
                    "action_type": "split",
                    "ex_date": str(ex_d),
                    "record_date": str(ex_d),
                    "effective_date": str(ex_d),
                    "factor": round(1.0 / ratio_f, 8),
                    "description": f"{ratio_f:.4g}:1 stock split",
                    "loaded_at": loaded_at,
                })

            # ---- Dividends ----
            # Dividend adjustment factor = (close_before_ex - dividend) / close_before_ex
            # We need the unadjusted close on the last trading day before ex_date.
            divs = ticker.dividends
            if len(divs) > 0:
                hist_start = divs.index.min().date() - timedelta(days=10)
                hist_end = divs.index.max().date() + timedelta(days=2)
                hist = ticker.history(
                    start=str(hist_start), end=str(hist_end),
                    auto_adjust=False,
                )
                if len(hist) > 0:
                    # Normalise DatetimeIndex to plain date objects for comparison
                    hist_dates = [
                        idx.date() if hasattr(idx, "date") else idx
                        for idx in hist.index
                    ]
                    hist_closes = list(hist["Close"])

                    for dt, amount in divs.items():
                        ex_d = dt.date() if hasattr(dt, "date") else dt
                        amount_f = float(amount)
                        if amount_f <= 0:
                            continue
                        # Last trading close before ex_date
                        before_closes = [
                            c for d, c in zip(hist_dates, hist_closes) if d < ex_d
                        ]
                        if not before_closes:
                            continue
                        close_before = float(before_closes[-1])
                        if close_before <= 0:
                            continue
                        factor = (close_before - amount_f) / close_before
                        if not (0 < factor < 1):
                            continue  # sanity: dividends should produce factor in (0,1)
                        rows.append({
                            "sym": sym,
                            "action_type": "dividend",
                            "ex_date": str(ex_d),
                            "record_date": str(ex_d),
                            "effective_date": str(ex_d),
                            "factor": round(factor, 8),
                            "description": f"Cash dividend ${amount_f:.4f}",
                            "loaded_at": loaded_at,
                        })

            n_sym = sum(1 for r in rows if r["sym"] == sym)
            log.info(f"Fetched {n_sym} corp action event(s) for {sym}")

        except Exception as exc:
            log.warning(f"Failed to fetch corp actions for {sym}: {exc}")

    return rows


# ---------------------------------------------------------------------------
# Factor computation
# ---------------------------------------------------------------------------

def _now_kdb() -> str:
    """Return current UTC time as a kdb+ timestamp string: YYYY.MM.DDTHH:MM:SS.nnnnnnnnn."""
    now = datetime.now(timezone.utc)
    return now.strftime("%Y.%m.%dT%H:%M:%S.") + f"{now.microsecond:06d}000"


def compute_adj_factors(symbols: list[str], corp_actions: list[dict],
                        start: date, end: date) -> list[dict]:
    """Compute daily cumulative backward-adjustment factors.

    For each calendar day D in [start, end] and each symbol:
      cumulative_factor = product of factor for every event where ex_date > D

    So for a 4:1 split (factor=0.25) on 2020-08-31:
      dates before 2020-08-31  → cumulative_factor = 0.25
      dates on/after 2020-08-31 → cumulative_factor = 1.0

    Multiple events are multiplied together in chronological order.
    """
    # Group events by symbol, sorted ascending by ex_date
    events_by_sym: dict[str, list[tuple]] = defaultdict(list)
    for ca in corp_actions:
        events_by_sym[ca["sym"]].append((
            date.fromisoformat(ca["ex_date"]),
            float(ca["factor"]),
            ca["action_type"],
        ))
    for sym in events_by_sym:
        events_by_sym[sym].sort(key=lambda x: x[0])

    loaded_at = _now_kdb()
    rows = []
    current = start
    while current <= end:
        for sym in symbols:
            cum = 1.0
            split_f = 1.0
            div_f = 1.0
            for ex_d, factor, action_type in events_by_sym.get(sym, []):
                if ex_d > current:
                    cum *= factor
                    if action_type == "split":
                        split_f *= factor
                    else:
                        div_f *= factor
            rows.append({
                "sym": sym,
                "date": str(current),
                "cumulative_factor": round(cum, 8),
                "split_factor": round(split_f, 8),
                "dividend_factor": round(div_f, 8),
                "loaded_at": loaded_at,
            })
        current += timedelta(days=1)

    return rows


# ---------------------------------------------------------------------------
# CSV helpers
# ---------------------------------------------------------------------------

def _read_csv(path: Path) -> list[dict]:
    if not path.exists():
        return []
    with open(path, newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    # Union of all field names (preserving first-seen order) so that rows with
    # different schemas (e.g. after adding loaded_at to an existing CSV) can be
    # written together.  Missing values are written as empty string.
    all_fields: list[str] = list(dict.fromkeys(k for row in rows for k in row.keys()))
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=all_fields, restval="")
        writer.writeheader()
        writer.writerows(rows)
    log.info(f"Wrote {len(rows)} rows to {path}")


def _append_csv(path: Path, rows: list[dict]) -> None:
    """Append rows to an existing CSV, preserving all prior rows.

    Used for adj_factors.csv so that each ingestion batch adds a new revision
    (with its own loaded_at timestamp) rather than overwriting the prior one.
    This preserves the full factor history needed for point-in-time queries.
    """
    if not rows:
        return
    existing = _read_csv(path)
    _write_csv(path, existing + rows)
    log.info(f"Appended {len(rows)} rows to {path} (total: {len(existing) + len(rows)})")


def _upsert_csv(path: Path, new_rows: list[dict], key_cols: list[str]) -> None:
    """Merge new_rows into the CSV at path, upserting by key_cols.

    Existing rows whose key matches a new row are replaced.
    New rows with no existing key are appended.
    Rows for other symbols/keys are preserved unchanged.
    """
    if not new_rows:
        return
    existing = _read_csv(path)
    index: dict[tuple, int] = {}
    for i, row in enumerate(existing):
        key = tuple(row[k] for k in key_cols)
        index[key] = i
    for row in new_rows:
        key = tuple(row[k] for k in key_cols)
        if key in index:
            existing[index[key]] = row
        else:
            existing.append(row)
            index[key] = len(existing) - 1
    _write_csv(path, existing)


# ---------------------------------------------------------------------------
# Security master stub
# ---------------------------------------------------------------------------

def generate_security_master(symbols: list[str]) -> list[dict]:
    """Stub security master — no free provider available.

    Populated with placeholder values so the CSV exists and ref_tables.q
    can load it without errors.  Replace with real data for production use.
    """
    return [
        {
            "sym": sym,
            "instrument_id": 1000 + i,
            "name": f"{sym} Inc.",
            "exchange": "XNAS",
            "currency": "USD",
            "valid_from": "2000-01-01",
            "valid_to": "9999-12-31",
        }
        for i, sym in enumerate(symbols)
    ]


# ---------------------------------------------------------------------------
# Main ingestion entry point (callable from orchestrator)
# ---------------------------------------------------------------------------

def ingest_ref_data(symbols: list[str], start: date, end: date,
                    staging_dir: Path) -> None:
    """Fetch and persist reference data for the given symbols and date range.

    Designed to be called after a successful backfill run.  Safe to call
    repeatedly — all writes are upserts, so existing data for other symbols
    or other date ranges is never overwritten.

    Factor recomputation covers both the requested range AND any dates already
    present in adj_factors.csv for these symbols.  This ensures that a newly
    discovered split (e.g. fetched today for a 2020 event) updates all
    historical factor rows, not just the newly backfilled window.
    """
    ref_dir = staging_dir / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Ingesting reference data: symbols={symbols} range={start}..{end}")

    # ---- Corp actions: fetch full history, append (PIT — never overwrite) ----
    corp_actions = fetch_corp_actions(symbols)
    if corp_actions:
        _append_csv(ref_dir / "corp_actions.csv", corp_actions)
        log.info(f"Corp actions: {len(corp_actions)} event(s) appended")
    else:
        log.info(f"No corp actions fetched — adj_factors will use factor=1.0")

    # ---- Adj factors: compute for requested range + all existing dates ----
    # We recompute existing dates for these symbols so that any newly fetched
    # event (e.g. a just-announced split) is reflected in historical factor rows.
    existing_factors = _read_csv(ref_dir / "adj_factors.csv")
    existing_dates: set[date] = {
        date.fromisoformat(r["date"])
        for r in existing_factors
        if r["sym"] in symbols
    }

    # Union of existing dates and the newly requested range
    all_dates: set[date] = set(existing_dates)
    d = start
    while d <= end:
        all_dates.add(d)
        d += timedelta(days=1)

    factor_start = min(all_dates)
    factor_end = max(all_dates)
    factor_rows = compute_adj_factors(symbols, corp_actions, factor_start, factor_end)

    # Only keep rows for dates we actually need (sparse — skip in-between calendar days
    # that were never in the HDB or the new range)
    factor_rows = [
        r for r in factor_rows
        if date.fromisoformat(r["date"]) in all_dates
    ]
    _append_csv(ref_dir / "adj_factors.csv", factor_rows)
    log.info(f"Adj factors: {len(factor_rows)} row(s) appended (PIT history preserved)")

    # ---- Security master: stub ----
    _upsert_csv(
        ref_dir / "security_master.csv",
        generate_security_master(symbols),
        key_cols=["sym"],
    )

    log.info("Reference data ingestion complete")


# ---------------------------------------------------------------------------
# Synthetic data generators (kept for --synthetic mode used in tests)
# ---------------------------------------------------------------------------

def _synthetic_corp_actions() -> list[dict]:
    return [
        {
            "sym": "AAPL", "action_type": "split",
            "ex_date": "2024-06-10", "record_date": "2024-06-09",
            "effective_date": "2024-06-10", "factor": "0.5",
            "description": "2-for-1 stock split (synthetic test data)",
            "loaded_at": "2024.01.01T00:00:00.000000000",
        },
        {
            "sym": "MSFT", "action_type": "dividend",
            "ex_date": "2024-05-15", "record_date": "2024-05-14",
            "effective_date": "2024-05-15", "factor": "0.998",
            "description": "Quarterly dividend (synthetic test data)",
            "loaded_at": "2024.01.01T00:00:00.000000000",
        },
    ]


def _synthetic_adj_factors(symbols: list[str],
                            start: date, end: date) -> list[dict]:
    aapl_split = date(2024, 6, 10)
    loaded_at = "2024.01.01T00:00:00.000000000"
    rows = []
    d = start
    while d <= end:
        for sym in symbols:
            cum = 0.5 if sym == "AAPL" and d < aapl_split else 1.0
            rows.append({
                "sym": sym, "date": str(d),
                "cumulative_factor": cum,
                "split_factor": cum,
                "dividend_factor": 1.0,
                "loaded_at": loaded_at,
            })
        d += timedelta(days=1)
    return rows


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main(argv=None):
    logging.basicConfig(level=logging.INFO, format=_LOG_FMT,
                        datefmt="%Y-%m-%dT%H:%M:%S", stream=sys.stdout)

    staging_dir = Path(os.environ.get("STAGING_DIR", "staging"))
    parser = argparse.ArgumentParser(
        description="Ingest corporate actions and adjustment factors."
    )
    parser.add_argument("--symbols", default=None,
                        help="Comma-separated symbols, e.g. AAPL,MSFT")
    parser.add_argument("--start", default="2024-01-01",
                        help="Start date YYYY-MM-DD (inclusive, default 2024-01-01)")
    parser.add_argument("--end", default="2024-12-31",
                        help="End date YYYY-MM-DD (inclusive, default 2024-12-31)")
    parser.add_argument("--synthetic", action="store_true",
                        help="Write hardcoded synthetic data (for tests/CI)")
    args = parser.parse_args(argv)

    symbols = (
        [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        if args.symbols
        else ["AAPL", "MSFT", "GOOGL", "AMZN", "TSLA"]
    )
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    ref_dir = staging_dir / "reference"
    ref_dir.mkdir(parents=True, exist_ok=True)

    if args.synthetic:
        log.info("Generating synthetic reference data (--synthetic mode)")
        _write_csv(ref_dir / "corp_actions.csv", _synthetic_corp_actions())
        _write_csv(ref_dir / "adj_factors.csv",
                   _synthetic_adj_factors(symbols, start, end))
        _write_csv(ref_dir / "security_master.csv",
                   generate_security_master(symbols))
        log.info("Synthetic reference data written")
        return

    ingest_ref_data(symbols, start, end, staging_dir)


if __name__ == "__main__":
    main()
