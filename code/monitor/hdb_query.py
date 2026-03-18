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
