"""
app.py — Flask monitoring dashboard for the Databento backfill pipeline.

Displays job status, metrics, failures, HDB coverage, and disk usage.
Can also submit new backfill requests (with dry-run cost estimation).

Usage:
    bin/monitor                          # default port 8080
    MONITOR_PORT=9090 bin/monitor        # custom port
"""

import json
import logging
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
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
from monitor.hdb_query import list_partitions, partition_detail, coverage_matrix, query_ohlcv, run_query

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


@app.route("/api/query", methods=["POST"])
def api_query():
    """Run an arbitrary qSQL expression against the HDB."""
    data = request.get_json(force=True, silent=True) or {}
    query = (data.get("query") or "").strip()
    if not query:
        return jsonify({"ok": False, "error": "query is required"}), 400
    limit = min(int(data.get("limit", 1000)), 10000)
    return jsonify(run_query(_HDB_DIR, query, limit))


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
# Routes — Chart data API
# ---------------------------------------------------------------------------

@app.route("/api/hdb/ohlcv")
def api_hdb_ohlcv():
    """Query ohlcv_1m data aggregated to daily, optionally adjusted."""
    symbols_raw = request.args.get("symbols", "")
    start = request.args.get("start", "")
    end = request.args.get("end", "")
    adjusted = request.args.get("adjusted", "false").lower() == "true"

    if not symbols_raw or not start or not end:
        return jsonify({"ok": False, "error": "symbols, start, and end are required"}), 400

    symbols = [s.strip().upper() for s in symbols_raw.split(",") if s.strip()]
    if not symbols:
        return jsonify({"ok": False, "error": "no valid symbols"}), 400

    cache_key = f"ohlcv:{','.join(symbols)}:{start}:{end}:{adjusted}"
    def _load():
        return query_ohlcv(_HDB_DIR, _PACKAGE_HOME, symbols, start, end, adjusted)

    try:
        result = _cached(cache_key, _load, ttl=10.0)
    except Exception as exc:
        log.exception(f"ohlcv query exception: {exc}")
        return jsonify({"ok": False, "error": str(exc), "data": [], "adjusted": adjusted}), 500
    if result.get("error"):
        return jsonify({"ok": False, "error": result["error"], "data": [], "adjusted": adjusted})
    return jsonify({"ok": True, "data": result["data"], "adjusted": adjusted})


# ---------------------------------------------------------------------------
# Backfill submission — state and helpers
# ---------------------------------------------------------------------------

_VALID_DATASETS = {"XNAS.ITCH", "XNYS.PILLAR", "IEXG.TOPS", "EQUS.MINI"}
_VALID_SCHEMAS = {"trades", "ohlcv-1m"}
_VALID_STYPES = {"raw_symbol", "instrument_id"}
_SYM_RE = re.compile(r"^[A-Z0-9.\-]{1,20}$")

# Singleton: at most one backfill subprocess at a time.
_active_backfill: dict | None = None


def _backfill_is_running() -> bool:
    """Check if the active backfill subprocess is still alive.

    If the process has exited, capture its return code and clear the slot.
    """
    global _active_backfill
    if _active_backfill is None:
        return False
    proc = _active_backfill["proc"]
    rc = proc.poll()
    if rc is not None:
        _active_backfill["return_code"] = rc
        _active_backfill["finished_at"] = datetime.now(timezone.utc).isoformat()
        # Close the log file handle
        fh = _active_backfill.get("log_fh")
        if fh and not fh.closed:
            fh.close()
        return False
    return True


def _validate_backfill_params(data: dict) -> tuple[dict | None, str | None]:
    """Validate and clean backfill request parameters.

    Returns (clean_params, None) on success or (None, error_message) on failure.
    """
    symbols_raw = (data.get("symbols") or "").strip()
    if not symbols_raw:
        return None, "symbols is required"
    symbols = list(dict.fromkeys(
        s.strip().upper() for s in symbols_raw.split(",") if s.strip()
    ))
    if not symbols:
        return None, "no valid symbols provided"
    if len(symbols) > 500:
        return None, f"too many symbols ({len(symbols)}), max 500"
    for s in symbols:
        if not _SYM_RE.match(s):
            return None, f"invalid symbol: {s!r}"

    start_str = (data.get("start") or "").strip()
    end_str = (data.get("end") or "").strip()
    if not start_str or not end_str:
        return None, "start and end dates are required"
    try:
        start = date.fromisoformat(start_str)
        end = date.fromisoformat(end_str)
    except ValueError:
        return None, "invalid date format (use YYYY-MM-DD)"
    if start > end:
        return None, "start must be <= end"
    if end > date.today():
        return None, "end must not be in the future"
    if (end - start).days > 365:
        return None, "date range exceeds 365 days"

    dataset = (data.get("dataset") or "XNAS.ITCH").strip()
    if dataset not in _VALID_DATASETS:
        return None, f"invalid dataset: {dataset!r}"

    schema = (data.get("schema") or "trades").strip()
    if schema not in _VALID_SCHEMAS:
        return None, f"invalid schema: {schema!r}"

    chunk_size = data.get("chunk_size", 20)
    try:
        chunk_size = int(chunk_size)
    except (ValueError, TypeError):
        return None, "chunk_size must be an integer"
    if not 1 <= chunk_size <= 100:
        return None, "chunk_size must be between 1 and 100"

    stype_in = (data.get("stype_in") or "raw_symbol").strip()
    if stype_in not in _VALID_STYPES:
        return None, f"invalid stype_in: {stype_in!r}"

    return {
        "symbols": symbols,
        "start": start_str,
        "end": end_str,
        "dataset": dataset,
        "schema": schema,
        "chunk_size": chunk_size,
        "stype_in": stype_in,
    }, None


def _build_orchestrator_cmd(params: dict, dry_run: bool = False) -> list[str]:
    """Build the orchestrator subprocess command from validated params."""
    cmd = [
        sys.executable,
        str(_PACKAGE_HOME / "code" / "backfill" / "orchestrator.py"),
        "--symbols", ",".join(params["symbols"]),
        "--start", params["start"],
        "--end", params["end"],
        "--dataset", params["dataset"],
        "--schema", params["schema"],
        "--chunk-size", str(params["chunk_size"]),
        "--stype-in", params["stype_in"],
    ]
    if dry_run:
        cmd.append("--dry-run")
    return cmd


def _orchestrator_env() -> dict[str, str]:
    """Build the environment for orchestrator subprocesses.

    The orchestrator uses sibling-relative imports (e.g. `from metrics import ...`),
    so code/backfill must be on PYTHONPATH.
    """
    env = os.environ.copy()
    extra = str(_PACKAGE_HOME / "code" / "backfill")
    existing = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{extra}:{existing}" if existing else extra
    return env


def _read_log_tail(path: Path, lines: int = 50) -> str:
    """Read the last N lines from a log file."""
    if not path.exists():
        return ""
    try:
        result = subprocess.run(
            ["tail", "-n", str(lines), str(path)],
            capture_output=True, text=True, timeout=5,
        )
        return result.stdout if result.returncode == 0 else ""
    except Exception:
        return ""


# ---------------------------------------------------------------------------
# Routes — Backfill submission API
# ---------------------------------------------------------------------------

@app.route("/api/backfill/dry-run", methods=["POST"])
def api_backfill_dry_run():
    """Run a cost estimate (--dry-run) synchronously and return the result."""
    data = request.get_json(force=True, silent=True) or {}
    params, err = _validate_backfill_params(data)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    cmd = _build_orchestrator_cmd(params, dry_run=True)
    try:
        result = subprocess.run(
            cmd,
            cwd=str(_PACKAGE_HOME),
            env=_orchestrator_env(),
            capture_output=True, text=True,
            timeout=120,
        )
    except subprocess.TimeoutExpired:
        return jsonify({"ok": False, "error": "dry-run timed out after 120s"}), 504

    output = result.stdout + result.stderr
    if result.returncode != 0:
        return jsonify({"ok": False, "error": output.strip()[-500:]}), 500

    # Parse the "Total: N chunk(s), estimated $X.XXXX" line
    chunks_count = 0
    estimated_cost = 0.0
    cost_limit = 50.0
    m = re.search(r"Total:\s+(\d+)\s+chunk\(s\),\s+estimated\s+\$([0-9.]+)", output)
    if m:
        chunks_count = int(m.group(1))
        estimated_cost = float(m.group(2))
    m2 = re.search(r"Limit:\s+\$([0-9.]+)", output)
    if m2:
        cost_limit = float(m2.group(1))

    return jsonify({
        "ok": True,
        "chunks": chunks_count,
        "estimated_cost": estimated_cost,
        "cost_limit": cost_limit,
        "over_budget": estimated_cost > cost_limit,
        "raw_output": output,
    })


@app.route("/api/backfill/submit", methods=["POST"])
def api_backfill_submit():
    """Launch a backfill as a background subprocess."""
    global _active_backfill
    if _backfill_is_running():
        return jsonify({
            "ok": False,
            "error": "A backfill is already running",
            "pid": _active_backfill["proc"].pid,
            "params": _active_backfill["params"],
        }), 409

    data = request.get_json(force=True, silent=True) or {}
    params, err = _validate_backfill_params(data)
    if err:
        return jsonify({"ok": False, "error": err}), 400

    cmd = _build_orchestrator_cmd(params)
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = _PACKAGE_HOME / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"monitor_submit_{ts}.log"
    log_fh = open(log_path, "w")

    proc = subprocess.Popen(
        cmd,
        cwd=str(_PACKAGE_HOME),
        env=_orchestrator_env(),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )

    _active_backfill = {
        "proc": proc,
        "log_fh": log_fh,
        "pid": proc.pid,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "params": params,
        "log_path": str(log_path),
        "return_code": None,
        "finished_at": None,
    }

    log.info(f"Backfill started: PID={proc.pid} params={params}")
    return jsonify({
        "ok": True,
        "pid": proc.pid,
        "params": params,
        "log_path": str(log_path),
    })


@app.route("/api/backfill/status")
def api_backfill_status():
    """Return the status of the current/last submitted backfill."""
    if _active_backfill is None:
        return jsonify({"running": False, "pid": None})

    running = _backfill_is_running()
    elapsed = 0.0
    if _active_backfill.get("started_at"):
        started = datetime.fromisoformat(_active_backfill["started_at"])
        elapsed = (datetime.now(timezone.utc) - started).total_seconds()

    return jsonify({
        "running": running,
        "pid": _active_backfill["pid"],
        "started_at": _active_backfill.get("started_at"),
        "finished_at": _active_backfill.get("finished_at"),
        "params": _active_backfill.get("params"),
        "elapsed_s": round(elapsed, 1),
        "return_code": _active_backfill.get("return_code"),
    })


@app.route("/api/backfill/cancel", methods=["POST"])
def api_backfill_cancel():
    """Send SIGTERM to the running backfill subprocess."""
    global _active_backfill
    if not _backfill_is_running():
        return jsonify({"ok": False, "error": "No backfill is running"}), 404

    proc = _active_backfill["proc"]
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
    _backfill_is_running()  # capture return code
    log.info(f"Backfill cancelled: PID={_active_backfill['pid']}")
    return jsonify({"ok": True, "signal": "SIGTERM"})


@app.route("/api/backfill/retry", methods=["POST"])
def api_backfill_retry():
    """Launch --retry-failed as a background subprocess."""
    global _active_backfill
    if _backfill_is_running():
        return jsonify({
            "ok": False,
            "error": "A backfill is already running",
            "pid": _active_backfill["proc"].pid,
        }), 409

    cmd = [
        sys.executable,
        str(_PACKAGE_HOME / "code" / "backfill" / "orchestrator.py"),
        "--retry-failed",
    ]
    ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    log_dir = _PACKAGE_HOME / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"monitor_retry_{ts}.log"
    log_fh = open(log_path, "w")

    proc = subprocess.Popen(
        cmd,
        cwd=str(_PACKAGE_HOME),
        env=_orchestrator_env(),
        stdout=log_fh,
        stderr=subprocess.STDOUT,
    )

    _active_backfill = {
        "proc": proc,
        "log_fh": log_fh,
        "pid": proc.pid,
        "started_at": datetime.now(timezone.utc).isoformat(),
        "params": {"retry": True},
        "log_path": str(log_path),
        "return_code": None,
        "finished_at": None,
    }

    log.info(f"Retry started: PID={proc.pid}")
    return jsonify({"ok": True, "pid": proc.pid})


@app.route("/api/backfill/log")
def api_backfill_log():
    """Return the tail of the backfill subprocess log."""
    if _active_backfill is None:
        return jsonify({"log": ""})
    lines = min(int(request.args.get("lines", 30)), 500)
    log_path = Path(_active_backfill.get("log_path", ""))
    return jsonify({"log": _read_log_tail(log_path, lines)})


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                        format="%(asctime)s [%(name)s] %(message)s")
    port = int(os.environ.get("MONITOR_PORT", "8080"))
    log.info(f"Starting monitor dashboard on http://localhost:{port}")
    app.run(host="0.0.0.0", port=port, debug=False)
