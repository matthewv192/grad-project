#!/usr/bin/env python3
"""
test_orchestrator.py — unit tests for code/backfill/orchestrator.py

Tests pure functions and stateful components that do not require a live
Databento API connection.  External calls (API, subprocess) are mocked
where necessary.

Run from PACKAGEHOME:
    python tests/test_orchestrator.py
Exits 0 on pass, 1 on any failure.
"""

import json
import os
import shutil
import sys
import tempfile
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

# Ensure code/backfill is on the import path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code" / "backfill"))

import databento as db
from orchestrator import (
    Chunk,
    JobRecord,
    JobStore,
    MAX_RETRIES,
    count_csv_rows,
    estimate_cost,
    generate_chunks,
    infer_date_from_filename,
    sha256_of_file,
)


# ---------------------------------------------------------------------------
# generate_chunks
# ---------------------------------------------------------------------------

class TestGenerateChunks(unittest.TestCase):
    REQ = "req_test"
    SCHEMA = "trades"
    DATASET = "XNAS.ITCH"

    def _chunks(self, syms, start, end, chunk_size=10):
        return generate_chunks(self.REQ, syms, start, end,
                               chunk_size, self.SCHEMA, self.DATASET)

    def test_single_day_single_batch(self):
        chunks = self._chunks(["AAPL", "MSFT"],
                               date(2024, 1, 15), date(2024, 1, 15))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].symbols, ["AAPL", "MSFT"])
        self.assertEqual(chunks[0].date, date(2024, 1, 15))

    def test_symbols_split_across_batches(self):
        # 7 symbols with chunk_size=5 → batches of 5 and 2
        syms = [f"SYM{i:02d}" for i in range(7)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 15),
                              chunk_size=5)
        self.assertEqual(len(chunks), 2)
        self.assertEqual(len(chunks[0].symbols), 5)
        self.assertEqual(len(chunks[1].symbols), 2)

    def test_multiple_days_multiplies_chunks(self):
        # 3 days × 1 batch = 3 chunks
        chunks = self._chunks(["AAPL"],
                               date(2024, 1, 15), date(2024, 1, 17))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0].date, date(2024, 1, 15))
        self.assertEqual(chunks[2].date, date(2024, 1, 17))

    def test_multi_day_multi_batch(self):
        # 3 days × 3 batches (7 syms, chunk_size=3) = 9 chunks
        syms = [f"S{i}" for i in range(7)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 17),
                              chunk_size=3)
        self.assertEqual(len(chunks), 9)

    def test_end_date_is_inclusive(self):
        chunks = self._chunks(["AAPL"],
                               date(2024, 1, 15), date(2024, 1, 15))
        self.assertEqual(chunks[-1].date, date(2024, 1, 15))

    def test_chunk_ids_are_all_unique(self):
        syms = [f"S{i}" for i in range(5)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 17),
                              chunk_size=2)
        ids = [c.chunk_id for c in chunks]
        self.assertEqual(len(ids), len(set(ids)))

    def test_chunk_carries_correct_metadata(self):
        chunks = self._chunks(["AAPL"],
                               date(2024, 1, 15), date(2024, 1, 15))
        c = chunks[0]
        self.assertEqual(c.request_id, self.REQ)
        self.assertEqual(c.schema, self.SCHEMA)
        self.assertEqual(c.dataset, self.DATASET)


# ---------------------------------------------------------------------------
# infer_date_from_filename
# ---------------------------------------------------------------------------

class TestInferDateFromFilename(unittest.TestCase):

    def test_standard_trades_format(self):
        self.assertEqual(
            infer_date_from_filename("xnas-itch-20240603.trades.csv"),
            "2024-06-03",
        )

    def test_ohlcv_format(self):
        self.assertEqual(
            infer_date_from_filename("equs-mini-20240117.ohlcv-1m.csv"),
            "2024-01-17",
        )

    def test_underscore_separator_format(self):
        self.assertEqual(
            infer_date_from_filename("DBNJ-XXXXX_20240603_trades.csv"),
            "2024-06-03",
        )

    def test_xnys_dataset(self):
        self.assertEqual(
            infer_date_from_filename("xnys-pillar-20240117.trades.csv"),
            "2024-01-17",
        )

    def test_no_date_raises_value_error(self):
        with self.assertRaises(ValueError):
            infer_date_from_filename("no_date_here.csv")

    def test_date_without_preceding_separator_raises(self):
        # "20240603" not preceded by '-' or '_' must not match
        with self.assertRaises(ValueError):
            infer_date_from_filename("file20240603.csv")

    def test_error_message_contains_filename(self):
        try:
            infer_date_from_filename("badfile.csv")
            self.fail("Expected ValueError")
        except ValueError as exc:
            self.assertIn("badfile.csv", str(exc))


# ---------------------------------------------------------------------------
# JobRecord
# ---------------------------------------------------------------------------

class TestJobRecord(unittest.TestCase):

    def _make(self, **kwargs):
        defaults = dict(
            chunk_id="req_2024.01.15_b000",
            request_id="req_test",
            schema="trades",
            symbols=["AAPL", "MSFT"],
            date="2024-01-15",
            status="downloaded",
            retries=1,
            file_path="/tmp/test.csv",
            file_paths=["/tmp/test.csv"],
        )
        defaults.update(kwargs)
        return JobRecord(**defaults)

    def test_round_trip_preserves_all_fields(self):
        r = self._make()
        r2 = JobRecord.from_dict(r.to_dict())
        self.assertEqual(r.chunk_id, r2.chunk_id)
        self.assertEqual(r.request_id, r2.request_id)
        self.assertEqual(r.symbols, r2.symbols)
        self.assertEqual(r.status, r2.status)
        self.assertEqual(r.file_paths, r2.file_paths)
        self.assertEqual(r.retries, r2.retries)

    def test_from_dict_backward_compat_missing_file_paths(self):
        """Records serialised before file_paths was added must default to []."""
        d = self._make().to_dict()
        del d["file_paths"]
        r = JobRecord.from_dict(d)
        self.assertEqual(r.file_paths, [])

    def test_from_dict_ignores_unknown_fields(self):
        """Future fields in the JSON must not crash from_dict."""
        d = self._make().to_dict()
        d["future_field"] = "some_value"
        r = JobRecord.from_dict(d)   # must not raise
        self.assertEqual(r.chunk_id, "req_2024.01.15_b000")

    def test_touch_sets_updated_at(self):
        r = self._make()
        r.updated_at = ""
        r.touch()
        self.assertNotEqual(r.updated_at, "")

    def test_touch_updated_at_is_iso_string(self):
        r = self._make()
        r.touch()
        # Should parse without error as an ISO timestamp
        from datetime import datetime
        datetime.fromisoformat(r.updated_at.replace("Z", "+00:00"))


# ---------------------------------------------------------------------------
# JobStore
# ---------------------------------------------------------------------------

class TestJobStore(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.store = JobStore(Path(self.tmp))

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def _make(self, chunk_id="c001", status="downloaded", retries=0):
        return JobRecord(
            chunk_id=chunk_id,
            request_id="req_test",
            schema="trades",
            symbols=["AAPL"],
            date="2024-01-15",
            status=status,
            retries=retries,
        )

    def test_save_and_load_round_trip(self):
        r = self._make()
        self.store.save(r)
        r2 = self.store.load("c001")
        self.assertIsNotNone(r2)
        self.assertEqual(r2.chunk_id, "c001")
        self.assertEqual(r2.status, "downloaded")

    def test_load_returns_none_for_unknown_chunk(self):
        self.assertIsNone(self.store.load("does_not_exist"))

    def test_load_all_returns_every_saved_record(self):
        for i in range(3):
            self.store.save(self._make(chunk_id=f"c{i:03d}"))
        records = self.store.load_all()
        self.assertEqual(len(records), 3)
        self.assertEqual({r.chunk_id for r in records},
                         {"c000", "c001", "c002"})

    def test_load_all_empty_store_returns_empty_list(self):
        self.assertEqual(self.store.load_all(), [])

    def test_load_failed_returns_only_failed_status(self):
        self.store.save(self._make("c1", status="failed",     retries=0))
        self.store.save(self._make("c2", status="verified",   retries=0))
        self.store.save(self._make("c3", status="downloaded", retries=0))
        failed = self.store.load_failed()
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].chunk_id, "c1")

    def test_load_failed_excludes_records_at_max_retries(self):
        # At max retries → not retryable
        self.store.save(self._make("c1", status="failed", retries=MAX_RETRIES))
        # Below max → retryable
        self.store.save(self._make("c2", status="failed", retries=MAX_RETRIES - 1))
        failed = self.store.load_failed()
        self.assertEqual(len(failed), 1)
        self.assertEqual(failed[0].chunk_id, "c2")

    def test_save_overwrites_existing_record(self):
        r = self._make()
        self.store.save(r)
        r.status = "verified"
        self.store.save(r)
        r2 = self.store.load("c001")
        self.assertEqual(r2.status, "verified")

    def test_no_tmp_file_left_after_save(self):
        self.store.save(self._make())
        tmp_files = list(Path(self.tmp).rglob("*.tmp"))
        self.assertEqual(len(tmp_files), 0)


# ---------------------------------------------------------------------------
# estimate_cost
# ---------------------------------------------------------------------------

class TestEstimateCost(unittest.TestCase):

    def _client(self):
        return MagicMock()

    def test_success_returns_float(self):
        client = self._client()
        client.metadata.get_cost.return_value = 1.23
        result = estimate_cost(client, "XNAS.ITCH", ["AAPL"],
                               "trades", "2024-01-15", "2024-01-16")
        self.assertAlmostEqual(result, 1.23)

    def test_bento_error_returns_zero(self):
        """A Databento API error (unknown dataset etc.) disables the guard."""
        client = self._client()
        client.metadata.get_cost.side_effect = db.BentoError("unknown dataset")
        result = estimate_cost(client, "XNAS.ITCH", ["AAPL"],
                               "trades", "2024-01-15", "2024-01-16")
        self.assertEqual(result, 0.0)

    def test_unexpected_error_raises_runtime_error(self):
        """Network faults and SDK bugs must propagate so the guard is not silently lost."""
        client = self._client()
        client.metadata.get_cost.side_effect = ConnectionError("network timeout")
        with self.assertRaises(RuntimeError):
            estimate_cost(client, "XNAS.ITCH", ["AAPL"],
                          "trades", "2024-01-15", "2024-01-16")

    def test_zero_cost_returned_as_zero(self):
        """Legitimate $0.00 estimate (e.g. free tier) must not be treated as an error."""
        client = self._client()
        client.metadata.get_cost.return_value = 0.0
        result = estimate_cost(client, "XNAS.ITCH", ["AAPL"],
                               "trades", "2024-01-15", "2024-01-16")
        self.assertEqual(result, 0.0)


# ---------------------------------------------------------------------------
# count_csv_rows
# ---------------------------------------------------------------------------

class TestCountCsvRows(unittest.TestCase):

    def _write_csv(self, lines):
        f = tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False)
        f.write("\n".join(lines) + "\n")
        f.close()
        return Path(f.name)

    def test_three_data_rows(self):
        p = self._write_csv(["h1,h2", "a,1", "b,2", "c,3"])
        try:
            self.assertEqual(count_csv_rows(p), 3)
        finally:
            p.unlink()

    def test_header_only_returns_zero(self):
        p = self._write_csv(["h1,h2"])
        try:
            self.assertEqual(count_csv_rows(p), 0)
        finally:
            p.unlink()

    def test_one_data_row(self):
        p = self._write_csv(["h1,h2", "a,1"])
        try:
            self.assertEqual(count_csv_rows(p), 1)
        finally:
            p.unlink()


# ---------------------------------------------------------------------------
# sha256_of_file
# ---------------------------------------------------------------------------

class TestSha256OfFile(unittest.TestCase):

    def test_hash_prefixed_with_sha256(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"test content")
            name = f.name
        try:
            self.assertTrue(sha256_of_file(Path(name)).startswith("sha256:"))
        finally:
            os.unlink(name)

    def test_hash_is_deterministic(self):
        with tempfile.NamedTemporaryFile(delete=False) as f:
            f.write(b"hello world")
            name = f.name
        try:
            self.assertEqual(sha256_of_file(Path(name)),
                             sha256_of_file(Path(name)))
        finally:
            os.unlink(name)

    def test_different_content_gives_different_hash(self):
        with tempfile.NamedTemporaryFile(delete=False) as f1, \
             tempfile.NamedTemporaryFile(delete=False) as f2:
            f1.write(b"content A")
            f2.write(b"content B")
            n1, n2 = f1.name, f2.name
        try:
            self.assertNotEqual(sha256_of_file(Path(n1)),
                                sha256_of_file(Path(n2)))
        finally:
            os.unlink(n1)
            os.unlink(n2)


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
