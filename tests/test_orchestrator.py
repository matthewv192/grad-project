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
import threading
import time
import unittest
from datetime import date, datetime, timezone
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
    _API_SEMAPHORE,
    _JsonFormatter,
    _StaleLockRemoved,
    _acquire_lock_with_timeout,
    _classify_error,
    _classify_failure,
    _cleanup_staging_csvs,
    _from_kdb_ts,
    _is_pid_alive,
    _parse_env_float,
    _parse_env_int,
    _to_kdb_ts,
    _write_manifests_for_record,
    count_csv_rows,
    download_csv,
    estimate_cost,
    generate_chunks,
    infer_date_from_filename,
    poll_until_done,
    run_chunk,
    sha256_of_file,
    submit_job,
    write_manifest,
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

    def test_single_day_single_symbol(self):
        # 1 symbol × 1 day = 1 chunk
        chunks = self._chunks(["AAPL"], date(2024, 1, 15), date(2024, 1, 15))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].symbols, ["AAPL"])
        self.assertEqual(chunks[0].date, date(2024, 1, 15))

    def test_symbols_batched_into_chunks(self):
        # 7 symbols × 1 day, chunk_size=10 → 1 batch of 7 = 1 chunk
        syms = [f"SYM{i:02d}" for i in range(7)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 15))
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].symbols, syms)

    def test_chunk_size_one_gives_one_chunk_per_symbol(self):
        # chunk_size=1: 7 symbols × 1 day = 7 chunks (one per symbol)
        syms = [f"SYM{i:02d}" for i in range(7)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 15),
                              chunk_size=1)
        self.assertEqual(len(chunks), 7)
        self.assertTrue(all(len(c.symbols) == 1 for c in chunks))

    def test_multiple_days_multiplies_chunks(self):
        # 1 symbol × 3 days = 3 chunks
        chunks = self._chunks(["AAPL"],
                               date(2024, 1, 15), date(2024, 1, 17))
        self.assertEqual(len(chunks), 3)
        self.assertEqual(chunks[0].date, date(2024, 1, 15))
        self.assertEqual(chunks[2].date, date(2024, 1, 17))

    def test_multi_day_multi_symbol(self):
        # 7 symbols × 3 days, chunk_size=10 → 1 batch per day = 3 chunks
        syms = [f"S{i}" for i in range(7)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 17))
        self.assertEqual(len(chunks), 3)
        self.assertTrue(all(len(c.symbols) == 7 for c in chunks))

    def test_multi_day_multi_symbol_chunk_size_one(self):
        # chunk_size=1: 7 symbols × 3 days = 21 chunks
        syms = [f"S{i}" for i in range(7)]
        chunks = self._chunks(syms, date(2024, 1, 15), date(2024, 1, 17),
                              chunk_size=1)
        self.assertEqual(len(chunks), 21)

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

    def test_stype_in_threaded_to_chunk(self):
        """stype_in passed to generate_chunks must appear on every Chunk."""
        chunks = generate_chunks(
            self.REQ, ["AAPL"], date(2024, 1, 15), date(2024, 1, 15),
            10, self.SCHEMA, self.DATASET, stype_in="continuous",
        )
        self.assertEqual(len(chunks), 1)
        self.assertEqual(chunks[0].stype_in, "continuous")

    def test_stype_in_default_is_raw_symbol(self):
        """Omitting stype_in must default to 'raw_symbol' on each Chunk."""
        chunks = self._chunks(["AAPL"], date(2024, 1, 15), date(2024, 1, 15))
        self.assertEqual(chunks[0].stype_in, "raw_symbol")


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

    def test_save_creates_kdb_table_file(self):
        self.store.save(self._make())
        jobs_file = Path(self.tmp) / "metadata" / "backfill_jobs"
        self.assertTrue(jobs_file.exists())


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

    def test_bento_error_raises_runtime_error(self):
        """A Databento API error must raise so the cost guard is never silently disabled."""
        client = self._client()
        client.metadata.get_cost.side_effect = db.BentoError("unknown dataset")
        with self.assertRaises(RuntimeError) as ctx:
            estimate_cost(client, "XNAS.ITCH", ["AAPL"],
                          "trades", "2024-01-15", "2024-01-16")
        self.assertIn("Cost estimation failed", str(ctx.exception))

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
# _classify_error
# ---------------------------------------------------------------------------

class TestClassifyError(unittest.TestCase):

    def test_bento_error_is_api_error(self):
        self.assertEqual(_classify_error(db.BentoError("bad request")), "api_error")

    def test_timed_out_message_is_api_error(self):
        self.assertEqual(_classify_error(RuntimeError("timed out waiting")), "api_error")

    def test_expired_message_is_api_error(self):
        self.assertEqual(_classify_error(RuntimeError("job expired")), "api_error")

    def test_failed_at_databento_is_api_error(self):
        self.assertEqual(_classify_error(RuntimeError("failed at databento")), "api_error")

    def test_cost_message_is_api_error(self):
        self.assertEqual(_classify_error(RuntimeError("cost exceeds limit")), "api_error")

    def test_no_csv_files_is_download_error(self):
        self.assertEqual(_classify_error(RuntimeError("no csv files produced")), "download_error")

    def test_checksum_mismatch_is_download_error(self):
        self.assertEqual(_classify_error(ValueError("checksum mismatch")), "download_error")

    def test_cannot_infer_date_is_parse_error(self):
        self.assertEqual(_classify_error(ValueError("cannot infer date from filename")), "parse_error")

    def test_parsing_message_is_parse_error(self):
        self.assertEqual(_classify_error(ValueError("error while parsing csv")), "parse_error")

    def test_manifest_message_is_load_error(self):
        self.assertEqual(_classify_error(RuntimeError("failed to write manifest")), "load_error")

    def test_permission_denied_is_load_error(self):
        self.assertEqual(_classify_error(PermissionError("permission denied: /hdb")), "load_error")

    def test_unknown_error_falls_back_to_api_error(self):
        """Unrecognised exceptions fall back to api_error rather than raising."""
        self.assertEqual(_classify_error(RuntimeError("something completely unknown")), "api_error")


# ---------------------------------------------------------------------------
# run_chunk — cost guard
# ---------------------------------------------------------------------------

class TestCostGuard(unittest.TestCase):
    """
    Verify that run_chunk marks a chunk as failed (without calling submit_job)
    when the estimated cost exceeds MAX_COST_USD.
    """

    def _make_chunk(self):
        return Chunk(
            chunk_id="c_guard_001",
            request_id="req_guard",
            dataset="XNAS.ITCH",
            schema="trades",
            symbols=["AAPL"],
            date=date(2024, 1, 15),
        )

    def test_chunk_fails_when_cost_exceeds_limit(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            manifest_dir = staging / "manifests"
            manifest_dir.mkdir()
            store = JobStore(staging / "jobs")

            chunk = self._make_chunk()
            client = MagicMock()

            # estimate_cost returns a value well above the $50 default limit
            with patch("orchestrator.estimate_cost", return_value=999.0), \
                 patch("orchestrator.submit_job") as mock_submit:
                result = run_chunk(client, chunk, store, staging, manifest_dir)

            self.assertFalse(result)
            record = store.load("c_guard_001")
            self.assertIsNotNone(record)
            self.assertEqual(record.status, "failed")
            self.assertIn("exceeds limit", record.error_msg)
            mock_submit.assert_not_called()

    def test_submit_proceeds_when_cost_is_within_limit(self):
        """When cost is under the limit, submit_job should be called."""
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            manifest_dir = staging / "manifests"
            manifest_dir.mkdir()
            store = JobStore(staging / "jobs")

            chunk = self._make_chunk()
            client = MagicMock()

            # estimate_cost returns a value comfortably below the $50 default
            with patch("orchestrator.estimate_cost", return_value=1.0), \
                 patch("orchestrator.submit_job", side_effect=RuntimeError("stop here")):
                # submit_job raises to prevent the rest of run_chunk running,
                # but we only care that it was called at all
                run_chunk(client, chunk, store, staging, manifest_dir)

            record = store.load("c_guard_001")
            self.assertIsNotNone(record)
            # status will be 'failed' due to our injected error, but that means
            # submit was reached — the cost guard did not abort
            self.assertNotIn("exceeds limit", record.error_msg)


# ---------------------------------------------------------------------------
# poll_until_done
# ---------------------------------------------------------------------------

class TestPollUntilDone(unittest.TestCase):
    """Test the Databento job polling loop."""

    def _client(self, jobs_sequence):
        """Create a mock client that returns different job lists on each call.

        jobs_sequence: list of lists, one per list_jobs call.
        """
        client = MagicMock()
        client.batch.list_jobs.side_effect = jobs_sequence
        return client

    @patch("orchestrator.POLL_INTERVAL_S", 0)  # no sleep in tests
    def test_returns_immediately_when_done(self):
        client = self._client([[{"id": "j1", "state": "done", "cost": 1.5}]])
        result = poll_until_done(client, "j1")
        self.assertEqual(result["id"], "j1")
        self.assertEqual(result["state"], "done")
        client.batch.list_jobs.assert_called_once()

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    def test_polls_until_done(self):
        """Job transitions from processing → done over two polls."""
        client = self._client([
            [{"id": "j1", "state": "processing"}],
            [{"id": "j1", "state": "done", "cost": 2.0}],
        ])
        result = poll_until_done(client, "j1")
        self.assertEqual(result["state"], "done")
        self.assertEqual(client.batch.list_jobs.call_count, 2)

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    def test_raises_on_expired(self):
        client = self._client([[{"id": "j1", "state": "expired"}]])
        with self.assertRaises(RuntimeError) as ctx:
            poll_until_done(client, "j1")
        self.assertIn("expired", str(ctx.exception))

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    def test_raises_on_not_found(self):
        client = self._client([[{"id": "other_job", "state": "done"}]])
        with self.assertRaises(RuntimeError) as ctx:
            poll_until_done(client, "j1")
        self.assertIn("not found", str(ctx.exception))

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    @patch("orchestrator.POLL_TIMEOUT_S", 0)
    def test_raises_on_timeout(self):
        client = self._client([[{"id": "j1", "state": "processing"}]])
        with self.assertRaises(RuntimeError) as ctx:
            poll_until_done(client, "j1")
        self.assertIn("Timed out", str(ctx.exception))

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    def test_raises_on_sdk_failed_error(self):
        """SDK exception containing 'failed' is surfaced as RuntimeError."""
        client = MagicMock()
        client.batch.list_jobs.side_effect = Exception("Job failed at server")
        with self.assertRaises(RuntimeError) as ctx:
            poll_until_done(client, "j1")
        self.assertIn("failed at Databento", str(ctx.exception))

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    def test_reraises_unexpected_sdk_error(self):
        """SDK exceptions without 'failed' keyword propagate directly."""
        client = MagicMock()
        client.batch.list_jobs.side_effect = ConnectionError("network timeout")
        with self.assertRaises(ConnectionError):
            poll_until_done(client, "j1")

    @patch("orchestrator.POLL_INTERVAL_S", 0)
    def test_single_api_call_covers_expired_state(self):
        """Expired jobs are found in the same list_jobs call (no second round-trip)."""
        client = self._client([[
            {"id": "j1", "state": "expired"},
            {"id": "j2", "state": "done"},
        ]])
        with self.assertRaises(RuntimeError) as ctx:
            poll_until_done(client, "j1")
        self.assertIn("expired", str(ctx.exception))
        # Only one API call, not two
        client.batch.list_jobs.assert_called_once()
        call_args = client.batch.list_jobs.call_args
        self.assertIn("expired", call_args[1]["states"])


# ---------------------------------------------------------------------------
# submit_job
# ---------------------------------------------------------------------------

class TestSubmitJob(unittest.TestCase):

    def test_returns_job_dict(self):
        client = MagicMock()
        client.batch.submit_job.return_value = {"id": "j1", "state": "queued"}
        result = submit_job(client, "XNAS.ITCH", ["AAPL"], "trades",
                            "2024-01-15", "2024-01-16")
        self.assertEqual(result["id"], "j1")
        client.batch.submit_job.assert_called_once()

    def test_passes_csv_encoding(self):
        client = MagicMock()
        client.batch.submit_job.return_value = {"id": "j1", "state": "queued"}
        submit_job(client, "XNAS.ITCH", ["AAPL"], "trades",
                   "2024-01-15", "2024-01-16")
        call_kwargs = client.batch.submit_job.call_args[1]
        self.assertEqual(call_kwargs["encoding"], "csv")
        self.assertTrue(call_kwargs["pretty_px"])
        self.assertTrue(call_kwargs["pretty_ts"])

    def test_propagates_stype_in(self):
        client = MagicMock()
        client.batch.submit_job.return_value = {"id": "j1", "state": "queued"}
        submit_job(client, "XNAS.ITCH", ["AAPL"], "trades",
                   "2024-01-15", "2024-01-16", stype_in="continuous")
        call_kwargs = client.batch.submit_job.call_args[1]
        self.assertEqual(call_kwargs["stype_in"], "continuous")


# ---------------------------------------------------------------------------
# download_csv
# ---------------------------------------------------------------------------

class TestDownloadCsv(unittest.TestCase):

    def test_returns_csv_paths_only(self):
        with tempfile.TemporaryDirectory() as tmp:
            chunk_dir = Path(tmp) / "chunk1"
            # Create fake files that the mock will "download"
            chunk_dir.mkdir()
            csv1 = chunk_dir / "data.csv"
            csv1.write_text("h1\nrow1\n")
            meta = chunk_dir / "metadata.json"
            meta.write_text("{}")

            client = MagicMock()
            client.batch.download.return_value = [str(csv1), str(meta)]
            result = download_csv(client, "j1", chunk_dir)
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0].name, "data.csv")

    def test_returns_empty_for_no_csvs(self):
        with tempfile.TemporaryDirectory() as tmp:
            chunk_dir = Path(tmp) / "chunk1"
            client = MagicMock()
            client.batch.download.return_value = []
            result = download_csv(client, "j1", chunk_dir)
            self.assertEqual(result, [])

    def test_creates_chunk_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            chunk_dir = Path(tmp) / "new_dir" / "chunk1"
            client = MagicMock()
            client.batch.download.return_value = []
            download_csv(client, "j1", chunk_dir)
            self.assertTrue(chunk_dir.exists())


# ---------------------------------------------------------------------------
# write_manifest
# ---------------------------------------------------------------------------

class TestWriteManifest(unittest.TestCase):

    def test_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv_path = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv_path.write_text("h1,h2\nrow1,row2\n")
            result = write_manifest(
                "req1", "c1", "j1", "trades", ["AAPL"],
                csv_path, manifest_dir, dataset="XNAS.ITCH",
            )
            self.assertEqual(result["request_id"], "req1")
            self.assertEqual(result["chunk_id"], "c1")
            self.assertEqual(result["schema"], "trades")
            # Verify file on disk
            manifest_path = manifest_dir / "c1.json"
            self.assertTrue(manifest_path.exists())
            with open(manifest_path) as f:
                on_disk = json.load(f)
            self.assertEqual(on_disk["request_id"], "req1")

    def test_uses_provided_row_count_and_checksum(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv_path = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv_path.write_text("h1,h2\nrow1,row2\n")
            result = write_manifest(
                "req1", "c1", "j1", "trades", ["AAPL"],
                csv_path, manifest_dir, row_count=42, checksum="sha256:abc",
            )
            self.assertEqual(result["row_count"], 42)
            self.assertEqual(result["checksum"], "sha256:abc")

    def test_infers_date_from_filename(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv_path = Path(tmp) / "xnas-itch-20240603.trades.csv"
            csv_path.write_text("h1\nrow\n")
            result = write_manifest(
                "req1", "c1", "j1", "trades", ["AAPL"],
                csv_path, manifest_dir,
            )
            self.assertEqual(result["date"], "2024-06-03")

    def test_no_tmp_file_left_behind(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv_path = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv_path.write_text("h1\nrow\n")
            write_manifest("req1", "c1", "j1", "trades", ["AAPL"],
                           csv_path, manifest_dir)
            tmp_files = list(Path(tmp).rglob("*.tmp"))
            self.assertEqual(len(tmp_files), 0)


# ---------------------------------------------------------------------------
# _acquire_lock_with_timeout
# ---------------------------------------------------------------------------

class TestAcquireLockWithTimeout(unittest.TestCase):

    def test_acquires_lock_on_uncontested_file(self):
        import fcntl
        with tempfile.NamedTemporaryFile(mode="a") as f:
            _acquire_lock_with_timeout(f, timeout_s=5)
            # Lock held — should be able to unlock without error
            fcntl.flock(f, fcntl.LOCK_UN)

    def test_raises_on_timeout(self):
        import fcntl
        with tempfile.NamedTemporaryFile(mode="a+", delete=False) as f:
            path = f.name
        try:
            # Write the current PID so stale detection sees a live process
            with open(path, "w") as pf:
                pf.write(str(os.getpid()))
            # Hold the lock
            holder = open(path, "a+")
            fcntl.flock(holder, fcntl.LOCK_EX)
            try:
                contender = open(path, "a+")
                try:
                    with self.assertRaises(RuntimeError) as ctx:
                        _acquire_lock_with_timeout(contender, timeout_s=1)
                    self.assertIn("Could not acquire", str(ctx.exception))
                finally:
                    contender.close()
            finally:
                fcntl.flock(holder, fcntl.LOCK_UN)
                holder.close()
        finally:
            os.unlink(path)


# ---------------------------------------------------------------------------
# _JsonFormatter
# ---------------------------------------------------------------------------

class TestJsonFormatter(unittest.TestCase):

    def test_output_is_valid_json(self):
        import logging
        fmt = _JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="hello %s", args=("world",), exc_info=None,
        )
        output = fmt.format(record)
        parsed = json.loads(output)
        self.assertEqual(parsed["msg"], "hello world")
        self.assertEqual(parsed["level"], "INFO")
        self.assertEqual(parsed["logger"], "test")
        self.assertIn("time", parsed)

    def test_exception_included(self):
        import logging
        fmt = _JsonFormatter()
        try:
            raise ValueError("test error")
        except ValueError:
            import sys
            exc_info = sys.exc_info()
        record = logging.LogRecord(
            name="test", level=logging.ERROR, pathname="", lineno=0,
            msg="something broke", args=(), exc_info=exc_info,
        )
        output = fmt.format(record)
        parsed = json.loads(output)
        self.assertIn("exc", parsed)
        self.assertIn("ValueError", parsed["exc"])

    def test_no_exc_key_when_no_exception(self):
        import logging
        fmt = _JsonFormatter()
        record = logging.LogRecord(
            name="test", level=logging.INFO, pathname="", lineno=0,
            msg="all good", args=(), exc_info=None,
        )
        output = fmt.format(record)
        parsed = json.loads(output)
        self.assertNotIn("exc", parsed)


# ---------------------------------------------------------------------------
# JobStore._parse_json error handling
# ---------------------------------------------------------------------------

class TestJobStoreJsonParsing(unittest.TestCase):

    def test_load_returns_none_on_invalid_json(self):
        """If jobstore.q returns garbage, load() returns None instead of crashing."""
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            with patch.object(store, "_run_q", return_value="not valid json {{{"):
                result = store.load("c001")
            self.assertIsNone(result)

    def test_load_all_returns_empty_on_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            with patch.object(store, "_run_q", return_value="<<<garbage>>>"):
                result = store.load_all()
            self.assertEqual(result, [])

    def test_load_failed_returns_empty_on_invalid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            with patch.object(store, "_run_q", return_value="not json"):
                result = store.load_failed()
            self.assertEqual(result, [])


# ---------------------------------------------------------------------------
# API rate limiting (_API_SEMAPHORE)
# ---------------------------------------------------------------------------

class TestApiRateLimiting(unittest.TestCase):

    def test_semaphore_exists_and_is_bounded(self):
        """Verify the API semaphore exists and has a reasonable concurrency limit."""
        self.assertIsInstance(_API_SEMAPHORE, threading.Semaphore)
        # Semaphore should limit concurrency (internal _value check)
        self.assertGreater(_API_SEMAPHORE._value, 0)
        self.assertLessEqual(_API_SEMAPHORE._value, 10)

    def test_concurrent_api_calls_are_throttled(self):
        """Verify that the semaphore limits actual concurrency."""
        max_concurrent = 0
        current_concurrent = 0
        lock = threading.Lock()

        def tracking_get_cost(**kwargs):
            nonlocal max_concurrent, current_concurrent
            with lock:
                current_concurrent += 1
                max_concurrent = max(max_concurrent, current_concurrent)
            time.sleep(0.05)
            with lock:
                current_concurrent -= 1
            return 1.0

        client = MagicMock()
        client.metadata.get_cost.side_effect = tracking_get_cost

        threads = []
        for _ in range(8):
            t = threading.Thread(
                target=estimate_cost,
                args=(client, "XNAS.ITCH", ["AAPL"], "trades",
                      "2024-01-15", "2024-01-16"),
            )
            threads.append(t)

        for t in threads:
            t.start()
        for t in threads:
            t.join()

        # Semaphore is 4, so max concurrent should be <= 4
        self.assertLessEqual(max_concurrent, 4)


# ---------------------------------------------------------------------------
# _parse_env_float / _parse_env_int (Fix #9)
# ---------------------------------------------------------------------------

class TestParseEnvConfig(unittest.TestCase):

    def test_float_returns_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TEST_FLOAT_VAR", None)
            result = _parse_env_float("TEST_FLOAT_VAR", 42.0)
        self.assertEqual(result, 42.0)

    def test_float_parses_valid_value(self):
        with patch.dict(os.environ, {"TEST_FLOAT_VAR": "99.5"}):
            result = _parse_env_float("TEST_FLOAT_VAR", 42.0)
        self.assertEqual(result, 99.5)

    def test_float_returns_default_on_invalid_value(self):
        with patch.dict(os.environ, {"TEST_FLOAT_VAR": "not_a_number"}):
            result = _parse_env_float("TEST_FLOAT_VAR", 42.0)
        self.assertEqual(result, 42.0)

    def test_int_returns_default_when_unset(self):
        with patch.dict(os.environ, {}, clear=False):
            os.environ.pop("TEST_INT_VAR", None)
            result = _parse_env_int("TEST_INT_VAR", 7)
        self.assertEqual(result, 7)

    def test_int_parses_valid_value(self):
        with patch.dict(os.environ, {"TEST_INT_VAR": "15"}):
            result = _parse_env_int("TEST_INT_VAR", 7)
        self.assertEqual(result, 15)

    def test_int_returns_default_on_invalid_value(self):
        with patch.dict(os.environ, {"TEST_INT_VAR": "abc"}):
            result = _parse_env_int("TEST_INT_VAR", 7)
        self.assertEqual(result, 7)


# ---------------------------------------------------------------------------
# _cleanup_staging_csvs (Fix #10)
# ---------------------------------------------------------------------------

class TestCleanupStagingCsvs(unittest.TestCase):

    def test_removes_verified_chunk_dirs(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            store = JobStore(staging / "jobs")

            # Create a verified chunk with a staging dir
            r = JobRecord(
                chunk_id="c_verified", request_id="req1",
                schema="trades", symbols=["AAPL"], date="2024-01-15",
                status="verified",
            )
            store.save(r)
            chunk_dir = staging / "c_verified"
            chunk_dir.mkdir()
            (chunk_dir / "data.csv").write_text("test data")

            cleaned = _cleanup_staging_csvs(store, staging, request_id="req1")
            self.assertEqual(cleaned, 1)
            self.assertFalse(chunk_dir.exists())

    def test_does_not_remove_non_verified_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            store = JobStore(staging / "jobs")

            r = JobRecord(
                chunk_id="c_failed", request_id="req1",
                schema="trades", symbols=["AAPL"], date="2024-01-15",
                status="failed",
            )
            store.save(r)
            chunk_dir = staging / "c_failed"
            chunk_dir.mkdir()
            (chunk_dir / "data.csv").write_text("test data")

            cleaned = _cleanup_staging_csvs(store, staging, request_id="req1")
            self.assertEqual(cleaned, 0)
            self.assertTrue(chunk_dir.exists())

    def test_does_not_crash_on_missing_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            store = JobStore(staging / "jobs")

            r = JobRecord(
                chunk_id="c_gone", request_id="req1",
                schema="trades", symbols=["AAPL"], date="2024-01-15",
                status="verified",
            )
            store.save(r)
            # Don't create the chunk dir — it shouldn't exist
            cleaned = _cleanup_staging_csvs(store, staging, request_id="req1")
            self.assertEqual(cleaned, 0)

    def test_filters_by_request_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            store = JobStore(staging / "jobs")

            # Chunk from a different request
            r = JobRecord(
                chunk_id="c_other", request_id="req_other",
                schema="trades", symbols=["AAPL"], date="2024-01-15",
                status="verified",
            )
            store.save(r)
            chunk_dir = staging / "c_other"
            chunk_dir.mkdir()
            (chunk_dir / "data.csv").write_text("test data")

            cleaned = _cleanup_staging_csvs(store, staging, request_id="req1")
            self.assertEqual(cleaned, 0)
            self.assertTrue(chunk_dir.exists())


# ---------------------------------------------------------------------------
# Stale lock detection (Fix #14)
# ---------------------------------------------------------------------------

class TestStaleLockDetection(unittest.TestCase):

    def test_is_pid_alive_returns_true_for_self(self):
        self.assertTrue(_is_pid_alive(os.getpid()))

    def test_is_pid_alive_returns_false_for_nonexistent(self):
        # PID 99999999 almost certainly doesn't exist
        self.assertFalse(_is_pid_alive(99999999))

    def test_lock_writes_pid(self):
        import fcntl
        with tempfile.NamedTemporaryFile(mode="a+", delete=False) as f:
            path = f.name
        try:
            fh = open(path, "a+")
            try:
                _acquire_lock_with_timeout(fh, timeout_s=5)
                fh.seek(0)
                content = fh.read().strip()
                self.assertEqual(content, str(os.getpid()))
            finally:
                fcntl.flock(fh, fcntl.LOCK_UN)
                fh.close()
        finally:
            os.unlink(path)

    def test_stale_lock_is_detected_and_removed(self):
        """A lock file held by a dead PID should be removed automatically."""
        import fcntl
        with tempfile.NamedTemporaryFile(mode="a+", delete=False) as f:
            lock_path = f.name

        try:
            # Write a fake PID that doesn't exist
            with open(lock_path, "w") as f:
                f.write("99999999")

            # Hold an flock on the file to simulate a stale lock
            holder = open(lock_path, "a+")
            fcntl.flock(holder, fcntl.LOCK_EX)

            # Now try to acquire from a different file handle — should detect stale
            contender = open(lock_path, "a+")
            try:
                with self.assertRaises(_StaleLockRemoved):
                    _acquire_lock_with_timeout(contender, timeout_s=2)
            finally:
                contender.close()
            holder.close()
        finally:
            # Clean up — file may or may not exist after stale removal
            if os.path.exists(lock_path):
                os.unlink(lock_path)


# ---------------------------------------------------------------------------
# _to_kdb_ts / _from_kdb_ts
# ---------------------------------------------------------------------------

class TestKdbTimestampConversion(unittest.TestCase):

    def test_to_kdb_ts_basic(self):
        result = _to_kdb_ts("2024-01-15T12:00:00+00:00")
        self.assertTrue(result.startswith("2024.01.15T12:00:00."))

    def test_to_kdb_ts_empty(self):
        self.assertEqual(_to_kdb_ts(""), "")
        self.assertEqual(_to_kdb_ts(None), "")

    def test_to_kdb_ts_invalid(self):
        self.assertEqual(_to_kdb_ts("not a date"), "")

    def test_from_kdb_ts_basic(self):
        result = _from_kdb_ts("2024.01.15T12:00:00.000000000")
        self.assertEqual(result, "2024-01-15T12:00:00.000000+00:00")

    def test_from_kdb_ts_empty(self):
        self.assertEqual(_from_kdb_ts(""), "")

    def test_from_kdb_ts_null(self):
        self.assertEqual(_from_kdb_ts("0Np"), "")

    def test_round_trip(self):
        original = "2024-01-15T12:30:45.123456+00:00"
        kdb = _to_kdb_ts(original)
        back = _from_kdb_ts(kdb)
        self.assertEqual(back, original)


# ---------------------------------------------------------------------------
# _classify_failure
# ---------------------------------------------------------------------------

class TestClassifyFailure(unittest.TestCase):

    def test_quality_error(self):
        self.assertIn("quality", _classify_failure("some msg", "quality_error"))

    def test_load_error(self):
        self.assertIn("loader", _classify_failure("some msg", "load_error"))

    def test_weekend_no_data(self):
        self.assertIn("no data", _classify_failure("no data returned", "api_error"))

    def test_download_error_by_type(self):
        self.assertIn("download", _classify_failure("some msg", "download_error"))

    def test_api_error_by_type(self):
        self.assertIn("API", _classify_failure("some msg", "api_error"))

    def test_parse_error(self):
        self.assertIn("parse", _classify_failure("some msg", "parse_error"))

    def test_cost_limit(self):
        # failure_type="" so the message-based checks are reached
        self.assertIn("cost", _classify_failure("cost exceeds limit", ""))

    def test_timeout(self):
        self.assertIn("timeout", _classify_failure("poll timeout reached", ""))

    def test_unknown_returns_error_msg(self):
        result = _classify_failure("something weird", "")
        self.assertEqual(result, "something weird")

    def test_empty_error_msg(self):
        self.assertEqual(_classify_failure("", ""), "unknown error")

    def test_failure_type_takes_precedence_over_message(self):
        """failure_type='api_error' should match before 'cost' in error_msg."""
        result = _classify_failure("cost exceeds limit", "api_error")
        self.assertIn("API", result)

    def test_download_type_overrides_api_message(self):
        result = _classify_failure("something about api call", "download_error")
        self.assertIn("download", result)

    def test_long_error_msg_truncated(self):
        long_msg = "x" * 200
        result = _classify_failure(long_msg, "")
        self.assertLessEqual(len(result), 120)


# ---------------------------------------------------------------------------
# _write_manifests_for_record
# ---------------------------------------------------------------------------

class TestWriteManifestsForRecord(unittest.TestCase):

    def test_writes_manifest_for_existing_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv_path = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv_path.write_text("h1\nrow1\n")

            chunk = Chunk(
                request_id="req1", chunk_id="c1", dataset="XNAS.ITCH",
                schema="trades", symbols=["AAPL"], date=date(2024, 1, 15),
            )
            record = JobRecord(
                chunk_id="c1", request_id="req1",
                databento_job_id="j1", file_paths=[str(csv_path)],
            )
            _write_manifests_for_record(chunk, record, manifest_dir)
            self.assertTrue((manifest_dir / "c1.json").exists())

    def test_skips_missing_csv(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            chunk = Chunk(
                request_id="req1", chunk_id="c1", dataset="XNAS.ITCH",
                schema="trades", symbols=["AAPL"], date=date(2024, 1, 15),
            )
            record = JobRecord(
                chunk_id="c1", request_id="req1",
                databento_job_id="j1",
                file_paths=["/nonexistent/path.csv"],
            )
            _write_manifests_for_record(chunk, record, manifest_dir)
            # No manifest written for missing CSV
            self.assertFalse(manifest_dir.exists() and list(manifest_dir.iterdir()))

    def test_multi_csv_generates_part_ids(self):
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv1 = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv2 = Path(tmp) / "xnas-itch-20240115.trades_part2.csv"
            csv1.write_text("h1\nrow1\n")
            csv2.write_text("h1\nrow2\n")

            chunk = Chunk(
                request_id="req1", chunk_id="c1", dataset="XNAS.ITCH",
                schema="trades", symbols=["AAPL"], date=date(2024, 1, 15),
            )
            record = JobRecord(
                chunk_id="c1", request_id="req1",
                databento_job_id="j1",
                file_paths=[str(csv1), str(csv2)],
            )
            _write_manifests_for_record(chunk, record, manifest_dir)
            self.assertTrue((manifest_dir / "c1.json").exists())
            self.assertTrue((manifest_dir / "c1_part2.json").exists())


# ---------------------------------------------------------------------------
# count_csv_rows edge cases
# ---------------------------------------------------------------------------

class TestCountCsvRowsEdgeCases(unittest.TestCase):

    def test_empty_file(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("")
            name = f.name
        try:
            self.assertEqual(count_csv_rows(Path(name)), 0)
        finally:
            os.unlink(name)

    def test_no_trailing_newline(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("h1,h2\nrow1,a\nrow2,b")  # no trailing newline
            name = f.name
        try:
            # 2 newlines (after header, after row1) → 2-1=1, but there are 2 data rows
            # This is a known limitation of newline counting; wc -l had the same behavior
            self.assertEqual(count_csv_rows(Path(name)), 1)
        finally:
            os.unlink(name)

    def test_large_row_count(self):
        with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
            f.write("header\n")
            for i in range(1000):
                f.write(f"row{i}\n")
            name = f.name
        try:
            self.assertEqual(count_csv_rows(Path(name)), 1000)
        finally:
            os.unlink(name)


# ---------------------------------------------------------------------------
# _cleanup_staging_csvs without request_id filter
# ---------------------------------------------------------------------------

class TestCleanupStagingCsvsNoFilter(unittest.TestCase):

    def test_cleans_all_verified_when_no_request_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            store = JobStore(staging / "jobs")

            for i in range(3):
                r = JobRecord(
                    chunk_id=f"c{i}", request_id=f"req{i}",
                    schema="trades", symbols=["AAPL"], date="2024-01-15",
                    status="verified",
                )
                store.save(r)
                d = staging / f"c{i}"
                d.mkdir()
                (d / "data.csv").write_text("data")

            cleaned = _cleanup_staging_csvs(store, staging)
            self.assertEqual(cleaned, 3)
            for i in range(3):
                self.assertFalse((staging / f"c{i}").exists())


# ---------------------------------------------------------------------------
# run_chunk — sad paths
# ---------------------------------------------------------------------------

class TestRunChunkSadPaths(unittest.TestCase):
    """Test run_chunk error handling and resume logic."""

    def _make_chunk(self, chunk_id="c_sad_001"):
        return Chunk(
            chunk_id=chunk_id, request_id="req_sad",
            dataset="XNAS.ITCH", schema="trades",
            symbols=["AAPL"], date=date(2024, 1, 15),
        )

    def _setup(self, tmp):
        staging = Path(tmp)
        manifest_dir = staging / "manifests"
        manifest_dir.mkdir()
        store = JobStore(staging / "jobs")
        return staging, manifest_dir, store

    def test_api_failure_marks_record_as_failed(self):
        """When submit_job raises, the record should be saved with status=failed."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            client = MagicMock()
            with patch("orchestrator.estimate_cost", return_value=1.0), \
                 patch("orchestrator.submit_job",
                       side_effect=RuntimeError("Databento API down")):
                result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertFalse(result)
            record = store.load("c_sad_001")
            self.assertEqual(record.status, "failed")
            self.assertIn("Databento API down", record.error_msg)
            self.assertEqual(record.failure_type, "api_error")
            self.assertEqual(record.retries, 1)

    def test_download_returns_zero_csvs_marks_failed(self):
        """When download produces no CSVs, run_chunk should fail with download_error."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            client = MagicMock()
            with patch("orchestrator.estimate_cost", return_value=1.0), \
                 patch("orchestrator.submit_job",
                       return_value={"id": "j1", "state": "queued"}), \
                 patch("orchestrator.poll_until_done",
                       return_value={"id": "j1", "state": "done"}), \
                 patch("orchestrator.download_csv", return_value=[]):
                result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertFalse(result)
            record = store.load("c_sad_001")
            self.assertEqual(record.status, "failed")
            self.assertIn("No CSV files", record.error_msg)

    def test_already_verified_skips_immediately(self):
        """A chunk already in verified status returns True without any API calls."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            # Pre-seed a verified record
            record = JobRecord(
                chunk_id="c_sad_001", request_id="req_sad",
                status="verified", schema="trades",
                symbols=["AAPL"], date="2024-01-15",
            )
            store.save(record)
            client = MagicMock()
            result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertTrue(result)
            # No API calls should have been made
            client.metadata.get_cost.assert_not_called()
            client.batch.submit_job.assert_not_called()

    def test_already_loaded_skips_immediately(self):
        """A chunk in loaded status also returns True immediately."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            record = JobRecord(
                chunk_id="c_sad_001", request_id="req_sad",
                status="loaded", schema="trades",
                symbols=["AAPL"], date="2024-01-15",
            )
            store.save(record)
            client = MagicMock()
            result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertTrue(result)

    def test_resume_from_downloaded_with_good_checksum_skips_download(self):
        """Resume from downloaded state with matching checksum writes manifests without API."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            # Create a fake downloaded CSV
            csv_path = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv_path.write_text("h1\nrow1\n")
            checksum = sha256_of_file(csv_path)
            record = JobRecord(
                chunk_id="c_sad_001", request_id="req_sad",
                status="downloaded", schema="trades",
                symbols=["AAPL"], date="2024-01-15",
                file_paths=[str(csv_path)], checksum=checksum,
            )
            store.save(record)
            client = MagicMock()
            result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertTrue(result)
            # Record should now be loaded
            updated = store.load("c_sad_001")
            self.assertEqual(updated.status, "loaded")
            # No API calls
            client.batch.submit_job.assert_not_called()

    def test_resume_from_downloaded_with_bad_checksum_resubmits(self):
        """Resume with checksum mismatch should fall through to re-submit."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            csv_path = Path(tmp) / "xnas-itch-20240115.trades.csv"
            csv_path.write_text("h1\nrow1\n")
            record = JobRecord(
                chunk_id="c_sad_001", request_id="req_sad",
                status="downloaded", schema="trades",
                symbols=["AAPL"], date="2024-01-15",
                file_paths=[str(csv_path)], checksum="sha256:wrong",
            )
            store.save(record)
            client = MagicMock()
            # After checksum mismatch, it will try to re-submit — make that fail
            # so we can verify the re-submit path was taken
            with patch("orchestrator.estimate_cost", return_value=1.0), \
                 patch("orchestrator.submit_job",
                       side_effect=RuntimeError("re-submit attempted")):
                result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertFalse(result)
            record = store.load("c_sad_001")
            self.assertIn("re-submit attempted", record.error_msg)

    def test_resume_from_submitted_polls_existing_job(self):
        """Resume from submitted state should poll the existing job_id, not re-submit."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            record = JobRecord(
                chunk_id="c_sad_001", request_id="req_sad",
                status="submitted", schema="trades",
                symbols=["AAPL"], date="2024-01-15",
                databento_job_id="existing_j1",
            )
            store.save(record)
            client = MagicMock()
            # Poll should be called, but make it fail to verify the path
            with patch("orchestrator.poll_until_done",
                       side_effect=RuntimeError("Job existing_j1 expired")):
                result = run_chunk(client, chunk, store, staging, manifest_dir)
            self.assertFalse(result)
            # submit_job should NOT have been called — we resumed from submitted
            client.batch.submit_job.assert_not_called()
            record = store.load("c_sad_001")
            self.assertIn("expired", record.error_msg)

    def test_failure_increments_retry_count(self):
        """Each failure should increment retries by 1."""
        with tempfile.TemporaryDirectory() as tmp:
            staging, manifest_dir, store = self._setup(tmp)
            chunk = self._make_chunk()
            client = MagicMock()
            # Fail twice
            for expected_retries in (1, 2):
                with patch("orchestrator.estimate_cost",
                           side_effect=RuntimeError("fail")):
                    run_chunk(client, chunk, store, staging, manifest_dir)
                record = store.load("c_sad_001")
                self.assertEqual(record.retries, expected_retries)


# ---------------------------------------------------------------------------
# KdbJobStore — subprocess failure
# ---------------------------------------------------------------------------

class TestJobStoreSubprocessFailure(unittest.TestCase):

    def test_run_q_raises_on_nonzero_exit(self):
        """If jobstore.q returns non-zero, _run_q should raise RuntimeError."""
        with tempfile.TemporaryDirectory() as tmp:
            store = JobStore(Path(tmp))
            with patch("subprocess.run") as mock_run:
                mock_run.return_value = MagicMock(
                    returncode=1, stdout="", stderr="'type error"
                )
                with self.assertRaises(RuntimeError) as ctx:
                    store._run_q({"op": "load", "chunk_id": "c1"})
                self.assertIn("jobstore.q failed", str(ctx.exception))
                self.assertIn("type error", str(ctx.exception))


# ---------------------------------------------------------------------------
# download_csv — API exception
# ---------------------------------------------------------------------------

class TestDownloadCsvSadPaths(unittest.TestCase):

    def test_api_error_propagates(self):
        """If client.batch.download raises, the exception should propagate."""
        with tempfile.TemporaryDirectory() as tmp:
            chunk_dir = Path(tmp) / "chunk1"
            client = MagicMock()
            client.batch.download.side_effect = ConnectionError("network down")
            with self.assertRaises(ConnectionError):
                download_csv(client, "j1", chunk_dir)


# ---------------------------------------------------------------------------
# sha256_of_file — sad paths
# ---------------------------------------------------------------------------

class TestSha256SadPaths(unittest.TestCase):

    def test_nonexistent_file_raises(self):
        with self.assertRaises(FileNotFoundError):
            sha256_of_file(Path("/nonexistent/file.csv"))


# ---------------------------------------------------------------------------
# write_manifest — bad filename
# ---------------------------------------------------------------------------

class TestWriteManifestSadPaths(unittest.TestCase):

    def test_bad_filename_raises_value_error(self):
        """If the CSV filename has no date, write_manifest should raise."""
        with tempfile.TemporaryDirectory() as tmp:
            manifest_dir = Path(tmp) / "manifests"
            csv_path = Path(tmp) / "nodatehere.csv"
            csv_path.write_text("h1\nrow\n")
            with self.assertRaises(ValueError) as ctx:
                write_manifest("req1", "c1", "j1", "trades", ["AAPL"],
                               csv_path, manifest_dir)
            self.assertIn("Cannot infer date", str(ctx.exception))


# ---------------------------------------------------------------------------
# estimate_cost — edge cases
# ---------------------------------------------------------------------------

class TestEstimateCostSadPaths(unittest.TestCase):

    def test_negative_cost_returned(self):
        """Negative cost from API (should never happen but shouldn't crash)."""
        client = MagicMock()
        client.metadata.get_cost.return_value = -1.0
        result = estimate_cost(client, "XNAS.ITCH", ["AAPL"], "trades",
                               "2024-01-15", "2024-01-16")
        self.assertEqual(result, -1.0)

    def test_none_cost_raises(self):
        """None from the API should raise (float(None) → TypeError)."""
        client = MagicMock()
        client.metadata.get_cost.return_value = None
        with self.assertRaises(RuntimeError):
            estimate_cost(client, "XNAS.ITCH", ["AAPL"], "trades",
                          "2024-01-15", "2024-01-16")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
