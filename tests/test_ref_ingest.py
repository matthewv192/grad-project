#!/usr/bin/env python3
"""
test_ref_ingest.py — unit tests for code/reference/ref_ingest.py

Tests pure functions and CSV helpers that do not require a live yfinance
connection. Covers: factor computation, CSV read/write/append/upsert,
security master generation, and edge cases.
"""

import csv
import os
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code" / "reference"))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code" / "backfill"))

from ref_ingest import (
    _append_csv,
    _read_csv,
    _upsert_csv,
    _write_csv,
    compute_adj_factors,
    fetch_corp_actions,
    generate_security_master,
    ingest_ref_data,
)


# ---------------------------------------------------------------------------
# compute_adj_factors
# ---------------------------------------------------------------------------

class TestComputeAdjFactors(unittest.TestCase):

    def test_no_events_gives_factor_one(self):
        """Symbols with no corp actions should have cumulative_factor=1.0 everywhere."""
        rows = compute_adj_factors(
            ["AAPL"], [], date(2024, 1, 15), date(2024, 1, 17),
        )
        self.assertEqual(len(rows), 3)
        for r in rows:
            self.assertEqual(r["sym"], "AAPL")
            self.assertEqual(float(r["cumulative_factor"]), 1.0)
            self.assertEqual(float(r["split_factor"]), 1.0)
            self.assertEqual(float(r["dividend_factor"]), 1.0)

    def test_split_before_ex_date(self):
        """Dates before a 2:1 split (factor=0.5) should have factor=0.5."""
        actions = [
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "0.5", "action_type": "split"},
        ]
        rows = compute_adj_factors(
            ["AAPL"], actions, date(2024, 1, 15), date(2024, 1, 18),
        )
        by_date = {r["date"]: r for r in rows}
        # Before split: factor = 0.5
        self.assertAlmostEqual(float(by_date["2024-01-15"]["cumulative_factor"]), 0.5)
        self.assertAlmostEqual(float(by_date["2024-01-16"]["cumulative_factor"]), 0.5)
        # On and after split: factor = 1.0 (ex_date is NOT > current)
        self.assertAlmostEqual(float(by_date["2024-01-17"]["cumulative_factor"]), 1.0)
        self.assertAlmostEqual(float(by_date["2024-01-18"]["cumulative_factor"]), 1.0)

    def test_split_factor_vs_dividend_factor(self):
        """Split and dividend events should populate their respective factor columns."""
        actions = [
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "0.5", "action_type": "split"},
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "0.99", "action_type": "dividend"},
        ]
        rows = compute_adj_factors(
            ["AAPL"], actions, date(2024, 1, 15), date(2024, 1, 15),
        )
        r = rows[0]
        self.assertAlmostEqual(float(r["split_factor"]), 0.5)
        self.assertAlmostEqual(float(r["dividend_factor"]), 0.99)
        self.assertAlmostEqual(float(r["cumulative_factor"]), 0.5 * 0.99)

    def test_multiple_events_multiply(self):
        """Two splits should multiply their factors together."""
        actions = [
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "0.5", "action_type": "split"},
            {"sym": "AAPL", "ex_date": "2024-01-18",
             "factor": "0.25", "action_type": "split"},
        ]
        rows = compute_adj_factors(
            ["AAPL"], actions, date(2024, 1, 15), date(2024, 1, 15),
        )
        # Both events are after 2024-01-15, so both factors apply
        self.assertAlmostEqual(float(rows[0]["cumulative_factor"]), 0.5 * 0.25)

    def test_multiple_symbols(self):
        """Each symbol gets its own factor rows."""
        actions = [
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "0.5", "action_type": "split"},
        ]
        rows = compute_adj_factors(
            ["AAPL", "MSFT"], actions, date(2024, 1, 15), date(2024, 1, 15),
        )
        self.assertEqual(len(rows), 2)
        aapl = [r for r in rows if r["sym"] == "AAPL"][0]
        msft = [r for r in rows if r["sym"] == "MSFT"][0]
        self.assertAlmostEqual(float(aapl["cumulative_factor"]), 0.5)
        self.assertAlmostEqual(float(msft["cumulative_factor"]), 1.0)

    def test_single_day_range(self):
        """start == end should produce exactly one row per symbol."""
        rows = compute_adj_factors(
            ["AAPL"], [], date(2024, 1, 15), date(2024, 1, 15),
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["date"], "2024-01-15")

    def test_loaded_at_present(self):
        """Every row should have a loaded_at timestamp."""
        rows = compute_adj_factors(
            ["AAPL"], [], date(2024, 1, 15), date(2024, 1, 15),
        )
        self.assertTrue(rows[0]["loaded_at"])
        self.assertIn("T", rows[0]["loaded_at"])

    def test_events_for_other_symbols_ignored(self):
        """Events for symbols not in the requested list are ignored."""
        actions = [
            {"sym": "TSLA", "ex_date": "2024-01-17",
             "factor": "0.5", "action_type": "split"},
        ]
        rows = compute_adj_factors(
            ["AAPL"], actions, date(2024, 1, 15), date(2024, 1, 15),
        )
        self.assertAlmostEqual(float(rows[0]["cumulative_factor"]), 1.0)


# ---------------------------------------------------------------------------
# _read_csv / _write_csv
# ---------------------------------------------------------------------------

class TestCsvHelpers(unittest.TestCase):

    def test_read_nonexistent_returns_empty(self):
        self.assertEqual(_read_csv(Path("/nonexistent/file.csv")), [])

    def test_write_then_read_round_trip(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            rows = [{"a": "1", "b": "2"}, {"a": "3", "b": "4"}]
            _write_csv(p, rows)
            result = _read_csv(p)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["a"], "1")
            self.assertEqual(result[1]["b"], "4")

    def test_write_empty_rows_does_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [])
            self.assertFalse(p.exists())

    def test_write_creates_parent_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "deep" / "nested" / "test.csv"
            _write_csv(p, [{"a": "1"}])
            self.assertTrue(p.exists())

    def test_write_handles_mixed_schemas(self):
        """Rows with different keys should union all field names."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            rows = [{"a": "1", "b": "2"}, {"a": "3", "c": "4"}]
            _write_csv(p, rows)
            result = _read_csv(p)
            # Both rows should have all three columns
            self.assertIn("a", result[0])
            self.assertIn("b", result[0])
            self.assertIn("c", result[1])
            # Missing values should be empty
            self.assertEqual(result[0].get("c", ""), "")
            self.assertEqual(result[1].get("b", ""), "")


# ---------------------------------------------------------------------------
# _append_csv
# ---------------------------------------------------------------------------

class TestAppendCsv(unittest.TestCase):

    def test_append_to_new_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _append_csv(p, [{"x": "1"}])
            result = _read_csv(p)
            self.assertEqual(len(result), 1)

    def test_append_preserves_existing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [{"x": "1"}])
            _append_csv(p, [{"x": "2"}])
            result = _read_csv(p)
            self.assertEqual(len(result), 2)
            self.assertEqual(result[0]["x"], "1")
            self.assertEqual(result[1]["x"], "2")

    def test_append_empty_rows_does_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [{"x": "1"}])
            _append_csv(p, [])
            result = _read_csv(p)
            self.assertEqual(len(result), 1)


# ---------------------------------------------------------------------------
# _upsert_csv
# ---------------------------------------------------------------------------

class TestUpsertCsv(unittest.TestCase):

    def test_insert_new_rows(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [{"sym": "AAPL", "val": "1"}])
            _upsert_csv(p, [{"sym": "MSFT", "val": "2"}], key_cols=["sym"])
            result = _read_csv(p)
            self.assertEqual(len(result), 2)

    def test_update_existing_row(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [{"sym": "AAPL", "val": "old"}])
            _upsert_csv(p, [{"sym": "AAPL", "val": "new"}], key_cols=["sym"])
            result = _read_csv(p)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["val"], "new")

    def test_empty_new_rows_does_nothing(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [{"sym": "AAPL", "val": "1"}])
            _upsert_csv(p, [], key_cols=["sym"])
            result = _read_csv(p)
            self.assertEqual(len(result), 1)

    def test_multi_key_upsert(self):
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [
                {"sym": "AAPL", "date": "2024-01-15", "val": "1"},
                {"sym": "AAPL", "date": "2024-01-16", "val": "2"},
            ])
            _upsert_csv(p, [{"sym": "AAPL", "date": "2024-01-15", "val": "X"}],
                         key_cols=["sym", "date"])
            result = _read_csv(p)
            self.assertEqual(len(result), 2)
            match = [r for r in result if r["date"] == "2024-01-15"][0]
            self.assertEqual(match["val"], "X")

    def test_missing_key_column_uses_empty_string(self):
        """Rows missing a key column should not raise KeyError."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            # Existing row lacks the "extra" key column
            _write_csv(p, [{"sym": "AAPL", "val": "1"}])
            _upsert_csv(p, [{"sym": "MSFT", "extra": "X", "val": "2"}],
                         key_cols=["sym", "extra"])
            result = _read_csv(p)
            self.assertEqual(len(result), 2)


# ---------------------------------------------------------------------------
# generate_security_master
# ---------------------------------------------------------------------------

class TestGenerateSecurityMaster(unittest.TestCase):

    def test_returns_one_row_per_symbol(self):
        result = generate_security_master(["AAPL", "MSFT"])
        self.assertEqual(len(result), 2)
        self.assertEqual(result[0]["sym"], "AAPL")
        self.assertEqual(result[1]["sym"], "MSFT")

    def test_has_required_columns(self):
        result = generate_security_master(["AAPL"])
        r = result[0]
        for col in ["sym", "instrument_id", "name", "exchange", "currency",
                     "valid_from", "valid_to"]:
            self.assertIn(col, r)

    def test_empty_symbols(self):
        self.assertEqual(generate_security_master([]), [])


# ---------------------------------------------------------------------------
# ingest_ref_data — empty date guard
# ---------------------------------------------------------------------------

class TestIngestRefDataEdgeCases(unittest.TestCase):

    def test_start_after_end_does_not_crash(self):
        """ingest_ref_data with start > end should return gracefully."""
        with tempfile.TemporaryDirectory() as tmp:
            # start > end, no existing factors → all_dates is empty
            ingest_ref_data(["AAPL"], date(2024, 1, 15), date(2024, 1, 10),
                            Path(tmp))
            # Should not raise ValueError — security master still written
            sm = _read_csv(Path(tmp) / "reference" / "security_master.csv")
            self.assertEqual(len(sm), 1)


# ---------------------------------------------------------------------------
# Sad paths — ref_ingest
# ---------------------------------------------------------------------------

class TestComputeAdjFactorsSadPaths(unittest.TestCase):

    def test_zero_factor_included(self):
        """A factor of 0.0 is mathematically valid (total loss) — should not crash."""
        actions = [
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "0.0", "action_type": "split"},
        ]
        rows = compute_adj_factors(
            ["AAPL"], actions, date(2024, 1, 15), date(2024, 1, 15),
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0]["cumulative_factor"]), 0.0)

    def test_negative_factor_included(self):
        """Negative factor from bad data — compute_adj_factors doesn't filter,
        that's fetch_corp_actions' job. Should not crash."""
        actions = [
            {"sym": "AAPL", "ex_date": "2024-01-17",
             "factor": "-0.5", "action_type": "split"},
        ]
        rows = compute_adj_factors(
            ["AAPL"], actions, date(2024, 1, 15), date(2024, 1, 15),
        )
        self.assertEqual(len(rows), 1)
        self.assertAlmostEqual(float(rows[0]["cumulative_factor"]), -0.5)

    def test_empty_symbols_returns_empty(self):
        rows = compute_adj_factors([], [], date(2024, 1, 15), date(2024, 1, 15))
        self.assertEqual(rows, [])

    def test_malformed_date_in_action_raises(self):
        """Bad ex_date format should raise ValueError from fromisoformat."""
        actions = [
            {"sym": "AAPL", "ex_date": "not-a-date",
             "factor": "0.5", "action_type": "split"},
        ]
        with self.assertRaises(ValueError):
            compute_adj_factors(["AAPL"], actions,
                                date(2024, 1, 15), date(2024, 1, 15))


class TestUpsertCsvSadPaths(unittest.TestCase):

    def test_upsert_on_nonexistent_file_creates_it(self):
        """Upserting into a file that doesn't exist should create it."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "new.csv"
            self.assertFalse(p.exists())
            _upsert_csv(p, [{"sym": "AAPL", "val": "1"}], key_cols=["sym"])
            result = _read_csv(p)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["sym"], "AAPL")

    def test_append_with_schema_evolution(self):
        """Appending rows with a new column should preserve old rows and add the column."""
        with tempfile.TemporaryDirectory() as tmp:
            p = Path(tmp) / "test.csv"
            _write_csv(p, [{"sym": "AAPL", "val": "1"}])
            _append_csv(p, [{"sym": "MSFT", "val": "2", "new_col": "X"}])
            result = _read_csv(p)
            self.assertEqual(len(result), 2)
            # Old row should have new_col as empty
            self.assertEqual(result[0].get("new_col", ""), "")
            # New row should have it
            self.assertEqual(result[1]["new_col"], "X")


class TestFetchCorpActionsSadPaths(unittest.TestCase):

    def test_yfinance_not_installed_returns_empty(self):
        """When yfinance import flag is False, fetch returns empty list."""
        import ref_ingest
        original = ref_ingest._YF_AVAILABLE
        try:
            ref_ingest._YF_AVAILABLE = False
            result = fetch_corp_actions(["AAPL"])
            self.assertEqual(result, [])
        finally:
            ref_ingest._YF_AVAILABLE = original


# ---------------------------------------------------------------------------
# yfinance integration smoke tests (skipped if yfinance not installed)
# ---------------------------------------------------------------------------

try:
    import yfinance as yf
    _YF_INSTALLED = True
except ImportError:
    _YF_INSTALLED = False


@unittest.skipUnless(_YF_INSTALLED, "yfinance not installed")
class TestFetchCorpActionsLive(unittest.TestCase):
    """Smoke tests that call the real yfinance API.

    Skipped only if yfinance is not installed.  These tests verify that:
      - yfinance still returns data in the expected format
      - our parsing logic handles real-world data without crashing
    """

    def test_aapl_has_splits(self):
        """AAPL has had multiple stock splits — we should get at least one."""
        rows = fetch_corp_actions(["AAPL"])
        splits = [r for r in rows if r["action_type"] == "split"]
        self.assertGreater(len(splits), 0, "AAPL should have at least one split")
        # Verify structure
        r = splits[0]
        self.assertIn("sym", r)
        self.assertIn("ex_date", r)
        self.assertIn("factor", r)
        self.assertEqual(r["sym"], "AAPL")
        # Factor should be a positive number < 1 (backward adjustment)
        self.assertGreater(float(r["factor"]), 0)
        self.assertLess(float(r["factor"]), 1)

    def test_aapl_has_dividends(self):
        """AAPL pays quarterly dividends — we should get at least one."""
        rows = fetch_corp_actions(["AAPL"])
        divs = [r for r in rows if r["action_type"] == "dividend"]
        self.assertGreater(len(divs), 0, "AAPL should have at least one dividend")
        r = divs[0]
        self.assertIn("factor", r)
        f = float(r["factor"])
        self.assertGreater(f, 0)
        self.assertLess(f, 1)

    def test_invalid_symbol_returns_empty(self):
        """A nonsense ticker should not crash — just return empty or skip."""
        rows = fetch_corp_actions(["ZZZZXNOTREAL999"])
        # Should not raise; may return 0 rows or some garbage — we only care it didn't crash
        self.assertIsInstance(rows, list)

    def test_multiple_symbols(self):
        """Fetching multiple symbols should return rows tagged with correct syms."""
        rows = fetch_corp_actions(["AAPL", "MSFT"])
        syms = {r["sym"] for r in rows}
        # Both should have corp actions
        self.assertIn("AAPL", syms)
        self.assertIn("MSFT", syms)

    def test_full_pipeline_with_real_data(self):
        """End-to-end: fetch → compute factors → write CSVs."""
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            ingest_ref_data(
                ["AAPL"], date(2024, 6, 1), date(2024, 6, 5), staging,
            )
            ref_dir = staging / "reference"
            # Corp actions CSV should exist and have rows
            ca = _read_csv(ref_dir / "corp_actions.csv")
            self.assertGreater(len(ca), 0)
            # Adj factors CSV should exist and have rows
            af = _read_csv(ref_dir / "adj_factors.csv")
            self.assertGreater(len(af), 0)
            # Every factor row should have the expected columns
            for r in af[:5]:
                self.assertIn("cumulative_factor", r)
                self.assertIn("split_factor", r)
                self.assertIn("loaded_at", r)
                self.assertGreater(float(r["cumulative_factor"]), 0)
            # Security master should exist
            sm = _read_csv(ref_dir / "security_master.csv")
            self.assertEqual(len(sm), 1)
            self.assertEqual(sm[0]["sym"], "AAPL")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
