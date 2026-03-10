#!/usr/bin/env python3
"""
test_metrics.py — unit tests for code/backfill/metrics.py

Run from PACKAGEHOME:
    python tests/test_metrics.py
Exits 0 on pass, 1 on any failure.
"""

import json
import sys
import tempfile
import time
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "code" / "backfill"))

from metrics import ChunkMetrics, load_chunk_metrics, write_summary


# ---------------------------------------------------------------------------
# ChunkMetrics
# ---------------------------------------------------------------------------

class TestChunkMetrics(unittest.TestCase):

    def test_mark_sets_named_attribute(self):
        m = ChunkMetrics(chunk_id="c1", request_id="r1")
        m.mark("submit_start")
        self.assertGreater(m.submit_start, 0.0)

    def test_duration_is_zero_when_end_not_marked(self):
        # mark start only — end stays at default 0.0, so _dur returns 0
        m = ChunkMetrics(chunk_id="c1", request_id="r1")
        m.mark("submit_start")
        self.assertEqual(m.submit_s, 0.0)

    def test_duration_positive_after_both_marks(self):
        m = ChunkMetrics(chunk_id="c1", request_id="r1")
        m.mark("total_start")
        time.sleep(0.02)
        m.mark("total_end")
        self.assertGreater(m.total_s, 0.0)

    def test_duration_never_negative(self):
        # If end < start (clock oddity), _dur clamps to 0
        m = ChunkMetrics(chunk_id="c1", request_id="r1")
        m.total_start = 100.0
        m.total_end = 50.0   # end before start
        self.assertEqual(m.total_s, 0.0)

    def test_to_dict_contains_all_duration_keys(self):
        m = ChunkMetrics(chunk_id="c1", request_id="r1",
                         schema="trades", date="2024-01-15")
        d = m.to_dict()
        for key in ("submit_s", "poll_s", "download_s", "load_s", "total_s"):
            self.assertIn(key, d, msg=f"missing key: {key}")

    def test_to_dict_contains_identity_fields(self):
        m = ChunkMetrics(chunk_id="c001", request_id="r001",
                         schema="trades", date="2024-01-15",
                         symbols=["AAPL", "MSFT"])
        d = m.to_dict()
        self.assertEqual(d["chunk_id"], "c001")
        self.assertEqual(d["request_id"], "r001")
        self.assertEqual(d["schema"], "trades")
        self.assertEqual(d["symbols"], ["AAPL", "MSFT"])

    def test_to_dict_includes_recorded_at(self):
        m = ChunkMetrics(chunk_id="c1", request_id="r1")
        d = m.to_dict()
        self.assertIn("recorded_at", d)
        self.assertIsInstance(d["recorded_at"], str)

    def test_save_writes_valid_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = ChunkMetrics(chunk_id="c001", request_id="r001",
                             schema="trades", date="2024-01-15")
            path = m.save(Path(tmp))
            self.assertTrue(path.exists())
            with open(path) as f:
                d = json.load(f)
            self.assertEqual(d["chunk_id"], "c001")
            self.assertEqual(d["request_id"], "r001")

    def test_save_path_is_under_metrics_request_dir(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = ChunkMetrics(chunk_id="c001", request_id="r001")
            path = m.save(Path(tmp))
            # Expected: <staging>/metrics/r001/c001.json
            self.assertEqual(path.parent.name, "r001")
            self.assertEqual(path.parent.parent.name, "metrics")

    def test_save_leaves_no_tmp_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = ChunkMetrics(chunk_id="c001", request_id="r001")
            m.save(Path(tmp))
            tmp_files = list(Path(tmp).rglob("*.tmp"))
            self.assertEqual(len(tmp_files), 0)

    def test_save_is_idempotent(self):
        """Saving the same chunk twice should overwrite, not append."""
        with tempfile.TemporaryDirectory() as tmp:
            m = ChunkMetrics(chunk_id="c001", request_id="r001", row_count=100)
            m.save(Path(tmp))
            m.row_count = 200
            m.save(Path(tmp))
            with open(Path(tmp) / "metrics" / "r001" / "c001.json") as f:
                d = json.load(f)
            self.assertEqual(d["row_count"], 200)


# ---------------------------------------------------------------------------
# write_summary
# ---------------------------------------------------------------------------

class TestWriteSummary(unittest.TestCase):
    """
    Rather than calling time.sleep, we set total_start/total_end directly on
    ChunkMetrics instances before saving.  This gives a known, deterministic
    total_s value in the saved JSON without any real delay.
    """

    def _save_chunk(self, staging_dir, chunk_id, request_id,
                    total_s=60.0, row_count=1000, file_bytes=0):
        m = ChunkMetrics(chunk_id=chunk_id, request_id=request_id,
                         row_count=row_count, file_bytes=file_bytes)
        # total_start defaults to 0.0; setting total_end gives a known total_s
        m.total_end = total_s
        m.save(Path(staging_dir))
        return m

    def test_returns_none_when_no_chunks_exist(self):
        with tempfile.TemporaryDirectory() as tmp:
            self.assertIsNone(write_summary(Path(tmp), "req_ghost"))

    def test_returns_path_to_summary_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001")
            path = write_summary(Path(tmp), "r001")
            self.assertIsNotNone(path)
            self.assertTrue(path.exists())
            self.assertEqual(path.name, "summary.json")

    def test_wall_s_written_when_provided(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001", total_s=60.0)
            path = write_summary(Path(tmp), "r001", wall_s=45.0)
            with open(path) as f:
                s = json.load(f)
            self.assertAlmostEqual(s["wall_s"], 45.0, places=2)

    def test_wall_s_is_null_when_not_provided(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001", total_s=60.0)
            path = write_summary(Path(tmp), "r001")
            with open(path) as f:
                s = json.load(f)
            self.assertIsNone(s["wall_s"])

    def test_sum_chunk_s_equals_sum_of_individual_totals(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001", total_s=60.0)
            self._save_chunk(tmp, "c002", "r001", total_s=40.0)
            path = write_summary(Path(tmp), "r001", wall_s=65.0)
            with open(path) as f:
                s = json.load(f)
            self.assertAlmostEqual(s["sum_chunk_s"], 100.0, places=2)

    def test_wall_s_less_than_sum_chunk_s_reflects_parallelism(self):
        """With parallel workers wall time < sum of chunk times."""
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001", total_s=60.0)
            self._save_chunk(tmp, "c002", "r001", total_s=60.0)
            path = write_summary(Path(tmp), "r001", wall_s=65.0)
            with open(path) as f:
                s = json.load(f)
            self.assertLess(s["wall_s"], s["sum_chunk_s"])

    def test_total_rows_is_sum_across_chunks(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001", row_count=1000)
            self._save_chunk(tmp, "c002", "r001", row_count=500)
            path = write_summary(Path(tmp), "r001")
            with open(path) as f:
                s = json.load(f)
            self.assertEqual(s["total_rows"], 1500)

    def test_chunk_count_correct(self):
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(4):
                self._save_chunk(tmp, f"c{i:03d}", "r001")
            path = write_summary(Path(tmp), "r001")
            with open(path) as f:
                s = json.load(f)
            self.assertEqual(s["chunk_count"], 4)

    def test_summary_contains_request_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r_specific")
            path = write_summary(Path(tmp), "r_specific")
            with open(path) as f:
                s = json.load(f)
            self.assertEqual(s["request_id"], "r_specific")

    def test_stage_totals_present(self):
        with tempfile.TemporaryDirectory() as tmp:
            self._save_chunk(tmp, "c001", "r001")
            path = write_summary(Path(tmp), "r001")
            with open(path) as f:
                s = json.load(f)
            for stage in ("submit_s", "poll_s", "download_s", "load_s"):
                self.assertIn(stage, s["stage_totals"],
                              msg=f"missing stage: {stage}")


# ---------------------------------------------------------------------------
# load_chunk_metrics
# ---------------------------------------------------------------------------

class TestLoadChunkMetrics(unittest.TestCase):

    def test_missing_request_returns_empty_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            result = load_chunk_metrics(Path(tmp), "no_such_request")
            self.assertEqual(result, [])

    def test_loads_all_chunk_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            for i in range(3):
                m = ChunkMetrics(chunk_id=f"c{i:03d}", request_id="r001")
                m.save(Path(tmp))
            result = load_chunk_metrics(Path(tmp), "r001")
            self.assertEqual(len(result), 3)

    def test_skips_summary_json(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            m = ChunkMetrics(chunk_id="c001", request_id="r001")
            m.save(staging)
            # plant a summary.json alongside the chunk file
            summary_dir = staging / "metrics" / "r001"
            with open(summary_dir / "summary.json", "w") as f:
                json.dump({"request_id": "r001"}, f)
            result = load_chunk_metrics(staging, "r001")
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["chunk_id"], "c001")

    def test_returned_dicts_contain_chunk_id(self):
        with tempfile.TemporaryDirectory() as tmp:
            m = ChunkMetrics(chunk_id="c_unique", request_id="r001")
            m.save(Path(tmp))
            result = load_chunk_metrics(Path(tmp), "r001")
            self.assertEqual(result[0]["chunk_id"], "c_unique")

    def test_corrupt_file_is_skipped_not_raised(self):
        with tempfile.TemporaryDirectory() as tmp:
            staging = Path(tmp)
            metrics_dir = staging / "metrics" / "r001"
            metrics_dir.mkdir(parents=True)
            # write a valid chunk and a corrupt one
            m = ChunkMetrics(chunk_id="c_good", request_id="r001")
            m.save(staging)
            with open(metrics_dir / "c_bad.json", "w") as f:
                f.write("this is not json {{{")
            result = load_chunk_metrics(staging, "r001")
            # corrupt file silently skipped; only the good one returned
            self.assertEqual(len(result), 1)
            self.assertEqual(result[0]["chunk_id"], "c_good")


# ---------------------------------------------------------------------------

if __name__ == "__main__":
    unittest.main()
