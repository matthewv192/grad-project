"""
hdb_query.py — read-only q subprocess queries for HDB inspection.

All functions follow the same pattern: build an inline q script, run it via
subprocess, parse JSON output.  No IPC connections are used — each call
spawns a short-lived q process that loads the HDB, runs the query, and exits.
"""

import json
import logging
import os
import subprocess
from pathlib import Path

log = logging.getLogger("monitor")


def _run_q(script: str, hdb_dir: Path, timeout: int = 30) -> str | None:
    """Run an inline q script against the HDB directory.  Returns stdout or None on failure."""
    env = {**os.environ, "TZ": "UTC", "KDBHDB": str(hdb_dir)}
    try:
        result = subprocess.run(
            ["q", "-q"], input=script, text=True,
            capture_output=True, cwd=str(hdb_dir), env=env, timeout=timeout,
        )
        if result.returncode != 0:
            log.warning(f"q query failed (rc={result.returncode}): {result.stderr.strip()}")
            return None
        return result.stdout.strip()
    except subprocess.TimeoutExpired:
        log.warning(f"q query timed out after {timeout}s")
        return None
    except Exception as exc:
        log.warning(f"q query error: {exc}")
        return None


def list_partitions(hdb_dir: Path) -> list[dict]:
    """List all date partitions with row counts per table.

    Returns: [{"date": "2024.01.17", "table": "trades", "rows": 54321}, ...]
    """
    if not hdb_dir.exists():
        return []

    script = (
        'hdb:hsym`$getenv`KDBHDB;\n'
        'symFile:` sv hdb,`sym;\n'
        'if[count key symFile; `sym set get symFile];\n'
        'system "l ",1_string hdb;\n'
        'dts:asc date;\n'
        'tbls:`trades`ohlcv_1m inter tables[];\n'
        'r:raze {[t] {[t;d]\n'
        '  c:@[{[t;d] count ?[t;enlist(=;`date;d);0b;()]};(t;d);0j];\n'
        '  `date`table`rows!(string d;string t;c)\n'
        ' }[t;] each dts} each tbls;\n'
        '-1 .j.j r;\n'
        'exit 0\n'
    )
    out = _run_q(script, hdb_dir)
    if not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return []


def partition_detail(hdb_dir: Path, date_str: str, table: str = "trades") -> list[dict]:
    """Per-sym row counts for a single date partition.

    Returns: [{"sym": "AAPL", "exchange": "XNAS.ITCH", "rows": 12345}, ...]
    """
    if not hdb_dir.exists():
        return []

    q_date = date_str.replace("-", ".")
    script = (
        'hdb:hsym`$getenv`KDBHDB;\n'
        'symFile:` sv hdb,`sym;\n'
        'if[count key symFile; `sym set get symFile];\n'
        'system "l ",1_string hdb;\n'
        f'dt:"D"$"{q_date}";\n'
        f'tbl:`{table};\n'
        'if[not tbl in tables[]; -1 "[]"; exit 0];\n'
        'r:0!select rows:count i by sym, exchange from tbl where date=dt;\n'
        'r:update sym:string sym, exchange:string exchange from r;\n'
        '-1 .j.j r;\n'
        'exit 0\n'
    )
    out = _run_q(script, hdb_dir)
    if not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return []


def query_ohlcv(hdb_dir: Path, package_home: Path, symbols: list[str],
                start: str, end: str, adjusted: bool = False) -> dict:
    """Query ohlcv_1m 1-minute bars, optionally with corporate action adjustments.

    Returns {"data": [...], "error": None} on success or
            {"data": [], "error": "message"} on failure.

    Each row is a 1-minute bar with time as an ISO string:
      [{"sym":"AAPL","time":"2026-01-14T09:30:00","close":337.08,...}, ...]

    When adjusted=True, loads adjlib.q and applies backward adjustment factors.
    """
    if not hdb_dir.exists():
        return {"data": [], "error": f"HDB directory not found: {hdb_dir}"}

    q_start = start.replace("-", ".")
    q_end = end.replace("-", ".")
    sym_list = ";".join(f"`{s}" for s in symbols)
    syms_q = f"({sym_list})" if len(symbols) > 1 else f"enlist`{symbols[0]}"

    # Common: convert timestamp to ISO string (2026-01-14T09:30:00) for Plotly
    fmt_cols = 'fmt:{ssr[10#string "d"$x;".";"-"],"T",8#string "t"$x};\nr:update sym:string sym, time:fmt each time from r;\n'

    if adjusted:
        pkg = str(package_home)
        # Fetch raw bars AND adjusted bars, join raw_close onto adjusted result
        script = (
            f'system "l {pkg}/code/adjlib/adjlib.q";\n'
            f'hdb:hsym`$"{hdb_dir}";\n'
            'symFile:` sv hdb,`sym;\n'
            'if[count key symFile; `sym set get symFile];\n'
            'system "l ",1_string hdb;\n'
            'loadAllRefData[];\n'
            f'syms:{syms_q};\n'
            f'sd:"D"$"{q_start}"; ed:"D"$"{q_end}";\n'
            'rawBars:select sym, time, raw_close:close from ohlcv_1m where date within (sd;ed), sym in syms;\n'
            'adjBars:getAdjustedClose[syms;sd;ed;`backward;0Np];\n'
            'adjBars:select sym, time, open, high, low, close, adj_close, volume from adjBars;\n'
            'r:adjBars lj `sym`time xkey rawBars;\n'
            + fmt_cols +
            '-1 .j.j r;\n'
            'exit 0\n'
        )
    else:
        script = (
            f'hdb:hsym`$"{hdb_dir}";\n'
            'symFile:` sv hdb,`sym;\n'
            'if[count key symFile; `sym set get symFile];\n'
            'system "l ",1_string hdb;\n'
            f'syms:{syms_q};\n'
            f'sd:"D"$"{q_start}"; ed:"D"$"{q_end}";\n'
            'if[not `ohlcv_1m in tables[]; -1 "[]"; exit 0];\n'
            'r:select sym, time, open, high, low, close, volume from ohlcv_1m where date within (sd;ed), sym in syms;\n'
            + fmt_cols +
            '-1 .j.j r;\n'
            'exit 0\n'
        )

    env = {**os.environ, "TZ": "UTC", "KDBHDB": str(hdb_dir),
           "PACKAGEHOME": str(package_home),
           "STAGING_DIR": os.environ.get("STAGING_DIR", str(package_home / "staging"))}
    try:
        result = subprocess.run(
            ["q", "-q"], input=script, text=True,
            capture_output=True, cwd=str(hdb_dir), env=env, timeout=30,
        )
        if result.returncode != 0:
            err = result.stderr.strip() or result.stdout.strip()
            log.warning(f"ohlcv query failed (rc={result.returncode}): {err}")
            return {"data": [], "error": f"q error: {err[-300:]}"}
        # q logging (-1 calls in adjlib/ref_tables) goes to stdout before
        # the JSON line. The JSON is always the last non-empty line.
        lines = result.stdout.strip().splitlines()
        json_line = ""
        for line in reversed(lines):
            line = line.strip()
            if line.startswith("[") or line.startswith("{"):
                json_line = line
                break
        if not json_line:
            return {"data": [], "error": None}
        return {"data": json.loads(json_line), "error": None}
    except subprocess.TimeoutExpired:
        log.warning("ohlcv query timed out")
        return {"data": [], "error": "q query timed out after 30s"}
    except json.JSONDecodeError as exc:
        log.warning(f"ohlcv query JSON error: {exc}")
        return {"data": [], "error": f"Failed to parse q output as JSON"}
    except Exception as exc:
        log.warning(f"ohlcv query error: {exc}")
        return {"data": [], "error": str(exc)}


def coverage_matrix(hdb_dir: Path, table: str = "trades",
                    start: str | None = None, end: str | None = None) -> list[dict]:
    """Sym x date coverage with row counts.

    Returns: [{"sym": "AAPL", "date": "2024.01.17", "rows": 54321}, ...]
    """
    if not hdb_dir.exists():
        return []

    where_clause = ""
    if start and end:
        q_start = start.replace("-", ".")
        q_end = end.replace("-", ".")
        where_clause = f'enlist(within;`date;("D"$"{q_start}";"D"$"{q_end}"))'
    else:
        where_clause = "()"

    script = (
        'hdb:hsym`$getenv`KDBHDB;\n'
        'symFile:` sv hdb,`sym;\n'
        'if[count key symFile; `sym set get symFile];\n'
        'system "l ",1_string hdb;\n'
        f'tbl:`{table};\n'
        'if[not tbl in tables[]; -1 "[]"; exit 0];\n'
        f'r:0!?[tbl;{where_clause};`sym`date!`sym`date;enlist[`rows]!enlist(count;`i)];\n'
        'r:update sym:string sym, date:string date from r;\n'
        '-1 .j.j r;\n'
        'exit 0\n'
    )
    out = _run_q(script, hdb_dir, timeout=60)
    if not out:
        return []
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return []
