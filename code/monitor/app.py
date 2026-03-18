"""
app.py — Flask monitoring dashboard for the Databento backfill pipeline.

Read-only: displays job status, metrics, failures, HDB coverage, and disk usage.
Does not submit, retry, or modify any data.

Usage:
    bin/monitor                          # default port 8080
    MONITOR_PORT=9090 bin/monitor        # custom port
"""

import json
import logging
import os
import shutil
import subprocess
import sys
import time
from collections import Counter, defaultdict
from pathlib import Path

from flask import Flask, jsonify, render_template, request

# Add the code directory to path so we can import from backfill.
# Also add the backfill directory itself because orchestrator.py uses
# `from metrics import ...` (sibling-relative, not package-relative).
_CODE_DIR = str(Path(__file__).resolve().parent.parent)
_BACKFILL_DIR = str(Path(__file__).resolve().parent.parent / "backfill")
if _CODE_DIR not in sys.path:
    sys.path.insert(0, _CODE_DIR)
if _BACKFILL_DIR not in sys.path:
    sys.path.insert(0, _BACKFILL_DIR)

from backfill.orchestrator import JobStore, STAGING_DIR, PACKAGE_HOME
from backfill.metrics import load_chunk_metrics
from monitor.hdb_query import list_partitions, partition_detail, coverage_matrix

log = logging.getLogger("monitor")

app = Flask(__name__)

# ---------------------------------------------------------------------------
# Configuration from environment
# ---------------------------------------------------------------------------

_STAGING_DIR = Path(os.environ.get("STAGING_DIR", str(STAGING_DIR)))
_HDB_DIR = Path(os.environ.get("KDBHDB", str(PACKAGE_HOME / "hdb")))
_PACKAGE_HOME = Path(os.environ.get("PACKAGEHOME", str(PACKAGE_HOME)))

# ---------------------------------------------------------------------------
# Simple TTL cache to avoid spawning q subprocesses on every poll
# ---------------------------------------------------------------------------

_cache: dict[str, tuple[float, object]] = {}
CACHE_TTL = 3.0


def _cached(key: str, fn, ttl: float = CACHE_TTL):
    now = time.monotonic()
    if key in _cache:
        ts, val = _cache[key]
        if (now - ts) < ttl:
            return val
    val = fn()
    _cache[key] = (now, val)
    return val


# ---------------------------------------------------------------------------
# Helper: load all job records as dicts
# ---------------------------------------------------------------------------

def _load_all_jobs() -> list[dict]:
    # Prefer the live progress file (written on every status change) over
    # the kdb job store (only flushed periodically).  This lets the dashboard
    # show real-time status transitions during an active backfill.
    progress = JobStore.read_progress(_STAGING_DIR)
    if progress is not None:
        return progress
    store = JobStore(_STAGING_DIR, package_home=_PACKAGE_HOME)
    records = store.load_all()
    return [r.to_dict() for r in records]


# ---------------------------------------------------------------------------
# Routes — HTML
# ---------------------------------------------------------------------------

@app.route("/")
def index():
    return render_template("index.html")


# ---------------------------------------------------------------------------
# Routes — JSON API
# ---------------------------------------------------------------------------

@app.route("/api/jobs")
def api_jobs():
    records = _cached("jobs", _load_all_jobs)
    request_id = request.args.get("request_id")

    by_req: dict[str, list[dict]] = defaultdict(list)
    for r in records:
        by_req[r["request_id"]].append(r)

    if request_id:
        by_req = {k: v for k, v in by_req.items() if k == request_id}

    result = []
    for req_id, recs in sorted(by_req.items(), reverse=True):
        counts = dict(Counter(r["status"] for r in recs))
        all_syms = sorted({s for r in recs for s in (r.get("symbols") or [])})
        chunk_dates = sorted(r.get("date", "") for r in recs if r.get("date"))
        date_range = {
            "start": chunk_dates[0] if chunk_dates else "",
            "end": chunk_dates[-1] if chunk_dates else "",
        }
        updated_times = [r.get("updated_at", "") for r in recs if r.get("updated_at")]
        last_updated = max(updated_times) if updated_times else ""
        result.append({
            "request_id": req_id,
            "schema": recs[0].get("schema", ""),
            "dataset": recs[0].get("dataset", ""),
            "symbols": all_syms,
            "date_range": date_range,
            "last_updated": last_updated,
            "chunk_count": len(recs),
            "status_counts": counts,
            "chunks": sorted(recs, key=lambda r: (r.get("date", ""), r.get("chunk_id", ""))),
        })
    return jsonify(result)


@app.route("/api/metrics")
def api_metrics_list():
    """List all request_ids that have metrics."""
    metrics_root = _STAGING_DIR / "metrics"
    if not metrics_root.exists():
        return jsonify([])
    req_ids = sorted(
        (d.name for d in metrics_root.iterdir() if d.is_dir()),
        reverse=True,
    )
    return jsonify(req_ids)


@app.route("/api/metrics/<request_id>")
def api_metrics_detail(request_id: str):
    """Per-chunk timing metrics and summary for one request."""
    def _load():
        chunks = load_chunk_metrics(_STAGING_DIR, request_id)
        summary_path = _STAGING_DIR / "metrics" / request_id / "summary.json"
        summary = None
        if summary_path.exists():
            with open(summary_path) as f:
                summary = json.load(f)
        return {"chunks": chunks, "summary": summary}

    return jsonify(_cached(f"metrics:{request_id}", _load, ttl=5.0))


@app.route("/api/failures")
def api_failures():
    records = _cached("jobs", _load_all_jobs)
    request_id = request.args.get("request_id")
    failed = [r for r in records if r.get("status") == "failed"]
    if request_id:
        failed = [r for r in failed if r.get("request_id") == request_id]

    by_type: dict[str, list[dict]] = defaultdict(list)
    for r in failed:
        by_type[r.get("failure_type") or "unknown"].append(r)

    result = []
    for ft, recs in sorted(by_type.items()):
        result.append({
            "failure_type": ft,
            "count": len(recs),
            "chunks": sorted(recs, key=lambda r: (r.get("date", ""), r.get("chunk_id", ""))),
        })
    return jsonify(result)


@app.route("/api/hdb/partitions")
def api_hdb_partitions():
    return jsonify(_cached("hdb:partitions", lambda: list_partitions(_HDB_DIR), ttl=10.0))


@app.route("/api/hdb/partition/<date_str>")
def api_hdb_partition_detail(date_str: str):
    table = request.args.get("table", "trades")
    return jsonify(_cached(
        f"hdb:detail:{date_str}:{table}",
        lambda: partition_detail(_HDB_DIR, date_str, table),
        ttl=10.0,
    ))


@app.route("/api/hdb/coverage")
def api_hdb_coverage():
    table = request.args.get("table", "trades")
    start = request.args.get("start")
    end = request.args.get("end")
    return jsonify(_cached(
        f"hdb:coverage:{table}:{start}:{end}",
        lambda: coverage_matrix(_HDB_DIR, table, start, end),
        ttl=15.0,
    ))


@app.route("/api/disk")
def api_disk():
    def _get():
        dirs = {
            "staging": _STAGING_DIR,
            "hdb": _HDB_DIR,
            "logs": _PACKAGE_HOME / "logs",
        }
        result = {}
        for name, path in dirs.items():
            if path.exists():
                try:
                    r = subprocess.run(
                        ["du", "-sb", str(path)],
                        capture_output=True, text=True, timeout=10,
                    )
                    bytes_used = int(r.stdout.split()[0]) if r.returncode == 0 else 0
                except Exception:
                    bytes_used = 0
                result[name] = {"path": str(path), "bytes": bytes_used}
            else:
                result[name] = {"path": str(path), "bytes": 0}
        return result

    return jsonify(_cached("disk", _get, ttl=30.0))


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s [%(name)s] %(message)s")
    port = int(os.environ.get("MONITOR_PORT", "8080"))
    log.info(f"Starting monitor dashboard on http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
