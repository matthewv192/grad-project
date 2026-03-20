"""
metrics.py — per-chunk timing and stage metrics for the backfill pipeline.

Records wall-clock time for each pipeline stage (submit, poll, download, load)
and writes a JSON file per chunk to:
  staging/metrics/<request_id>/<chunk_id>.json

Call write_summary() after all chunks in a request complete to emit
  staging/metrics/<request_id>/summary.json
"""

import json
import logging
import os
import time
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path

@dataclass
class ChunkMetrics:
    """Timing and throughput metrics for one chunk across all pipeline stages."""
    chunk_id: str
    request_id: str
    schema: str = ""
    date: str = ""
    symbols: list = field(default_factory=list)
    row_count: int = 0       # rows in the downloaded CSV (set by Python after download)
    rows_loaded: int = 0     # rows actually written to HDB (set by q loader after .Q.dpft)
    file_bytes: int = 0
    failure_type: str = ""    # api_error | download_error | parse_error | load_error | quality_error

    # Stage timestamps (time.monotonic() seconds)
    submit_start: float = 0.0
    submit_end: float = 0.0
    poll_start: float = 0.0
    poll_end: float = 0.0
    download_start: float = 0.0
    download_end: float = 0.0
    load_start: float = 0.0
    load_end: float = 0.0
    total_start: float = 0.0
    total_end: float = 0.0

    def mark(self, stage: str) -> None:
        """Set a stage timestamp.  stage = 'submit_start', 'poll_end', etc."""
        setattr(self, stage, time.monotonic())

    def _dur(self, s_attr: str, e_attr: str) -> float:
        s = getattr(self, s_attr, 0.0)
        e = getattr(self, e_attr, 0.0)
        return max(0.0, e - s)

    @property
    def submit_s(self) -> float:
        return self._dur("submit_start", "submit_end")

    @property
    def poll_s(self) -> float:
        return self._dur("poll_start", "poll_end")

    @property
    def download_s(self) -> float:
        return self._dur("download_start", "download_end")

    @property
    def load_s(self) -> float:
        return self._dur("load_start", "load_end")

    @property
    def total_s(self) -> float:
        return self._dur("total_start", "total_end")

    def to_dict(self) -> dict:
        d = asdict(self)
        d.update({
            "submit_s": self.submit_s,
            "poll_s": self.poll_s,
            "download_s": self.download_s,
            "load_s": self.load_s,
            "total_s": self.total_s,
            "recorded_at": datetime.now(timezone.utc).isoformat(),
        })
        return d

    def save(self, staging_dir: Path) -> Path:
        metrics_dir = staging_dir / "metrics" / self.request_id
        metrics_dir.mkdir(parents=True, exist_ok=True)
        path = metrics_dir / f"{self.chunk_id}.json"
        tmp = path.with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(self.to_dict(), f, indent=2)
        tmp.rename(path)
        return path


def load_chunk_metrics(staging_dir: Path, request_id: str) -> list[dict]:
    """Load all per-chunk metric files for a given request_id."""
    metrics_dir = staging_dir / "metrics" / request_id
    if not metrics_dir.exists():
        return []
    result = []
    for p in sorted(metrics_dir.glob("*.json")):
        if p.name == "summary.json":
            continue
        try:
            with open(p) as f:
                result.append(json.load(f))
        except Exception as exc:
            logging.getLogger("metrics").warning(
                f"Skipping corrupt chunk metrics file {p.name}: {exc}"
            )
    return result


def write_summary(staging_dir: Path, request_id: str,
                  wall_s: float | None = None) -> Path | None:
    """Aggregate all chunk metrics into a summary.json for the request.

    wall_s: actual elapsed wall-clock seconds for the whole parallel run,
            measured by the caller around _run_chunks_parallel.  When chunks
            run concurrently this is much less than the sum of individual
            chunk durations.  If omitted, wall_s is left as null in the JSON.
    """
    chunks = load_chunk_metrics(staging_dir, request_id)
    if not chunks:
        return None

    total_rows = sum(c.get("row_count", 0) for c in chunks)
    total_rows_loaded = sum(c.get("rows_loaded", 0) for c in chunks)
    total_bytes = sum(c.get("file_bytes", 0) for c in chunks)
    sum_chunk_s = sum(c.get("total_s", 0.0) for c in chunks)

    failure_breakdown: dict[str, int] = {}
    for c in chunks:
        ft = c.get("failure_type", "")
        if ft:
            failure_breakdown[ft] = failure_breakdown.get(ft, 0) + 1

    summary = {
        "request_id": request_id,
        "chunk_count": len(chunks),
        "total_rows": total_rows,
        "total_rows_loaded": total_rows_loaded,
        "total_bytes": total_bytes,
        # wall_s  — real elapsed time (parallel workers overlap, so wall_s ≤ sum_chunk_s)
        # sum_chunk_s — sum of all individual chunk durations (useful for CPU accounting)
        "wall_s": round(wall_s, 3) if wall_s is not None else None,
        "sum_chunk_s": round(sum_chunk_s, 3),
        "avg_chunk_s": round(sum_chunk_s / len(chunks), 3) if chunks else 0.0,
        "stage_totals": {
            "submit_s": sum(c.get("submit_s", 0.0) for c in chunks),
            "poll_s": sum(c.get("poll_s", 0.0) for c in chunks),
            "download_s": sum(c.get("download_s", 0.0) for c in chunks),
            "load_s": sum(c.get("load_s", 0.0) for c in chunks),
        },
        "failed_chunk_count": len(failure_breakdown),
        "failure_breakdown": failure_breakdown,
        "recorded_at": datetime.now(timezone.utc).isoformat(),
    }

    metrics_dir = staging_dir / "metrics" / request_id
    metrics_dir.mkdir(parents=True, exist_ok=True)
    path = metrics_dir / "summary.json"
    with open(path, "w") as f:
        json.dump(summary, f, indent=2)
    return path


def print_metrics(staging_dir: Path, request_id: str | None = None) -> None:
    """Print per-chunk timing metrics to stdout."""
    metrics_root = staging_dir / "metrics"
    if not metrics_root.exists():
        print("No metrics found.")
        return

    req_dirs = sorted(d for d in metrics_root.iterdir() if d.is_dir())
    if request_id:
        target = metrics_root / request_id
        req_dirs = [target] if target.is_dir() else []

    if not req_dirs:
        print(f"No metrics found{f' for {request_id}' if request_id else ''}.")
        return

    for req_dir in req_dirs:
        chunks = load_chunk_metrics(staging_dir, req_dir.name)
        if not chunks:
            continue

        print(f"\nMetrics — {req_dir.name} ({len(chunks)} chunk(s)):")
        print(f"  {'chunk_id':<50} {'dl_rows':>8} {'ld_rows':>8} {'total_s':>8} "
              f"{'submit_s':>9} {'poll_s':>8} {'dl_s':>7} {'load_s':>8}")
        print(f"  {'-'*117}")

        for c in chunks:
            print(f"  {c.get('chunk_id', ''):<50} "
                  f"{c.get('row_count', 0):>8} "
                  f"{c.get('rows_loaded', 0):>8} "
                  f"{c.get('total_s', 0.0):>8.1f} "
                  f"{c.get('submit_s', 0.0):>9.1f} "
                  f"{c.get('poll_s', 0.0):>8.1f} "
                  f"{c.get('download_s', 0.0):>7.1f} "
                  f"{c.get('load_s', 0.0):>8.1f}")

        summary_path = req_dir / "summary.json"
        if summary_path.exists():
            with open(summary_path) as f:
                s = json.load(f)
            mb = s.get("total_bytes", 0) / 1024 / 1024
            wall = s.get("wall_s")
            wall_str = f"{wall:.1f}s wall" if wall is not None else "wall_s unknown"
            print(f"\n  Summary: {s.get('total_rows', 0):,} downloaded, "
                  f"{s.get('total_rows_loaded', 0):,} loaded, "
                  f"{mb:.1f} MB, {wall_str}, "
                  f"{s.get('sum_chunk_s', 0.0):.1f}s total across chunks")
            if s.get("failure_breakdown"):
                breakdown_str = ", ".join(
                    f"{k}: {v}" for k, v in s["failure_breakdown"].items()
                )
                print(f"  Failure breakdown: {breakdown_str}")

    print()
