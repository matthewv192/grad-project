"""
orchestrator.py — Databento batch backfill orchestrator.

Responsibilities (Python side of the Python↔q split):
  - Chunking: one Databento job per (day × symbol-batch). Each chunk is
    independent so a single failure doesn't block everything else.
  - Job store: every state transition is written to
    staging/metadata/backfill_jobs (binary kdb table) so a crashed run can resume.
  - Idempotency: before submitting a chunk we check its stored status and
    skip (or fast-forward) accordingly.
  - Retry: --retry-failed re-queues failed chunks with exponential backoff
    (min(2^retries, 60) seconds), capped at 60s.
  - Multi-CSV: when a Databento job delivers multiple CSV files, each gets
    its own manifest (chunk IDs suffixed _part2, _part3, …). All paths are
    stored in the job record so a resumed run re-writes all manifests.
  - Cost safeguard: BACKFILL_MAX_COST_USD (default $50) blocks over-budget
    requests before submission. Unexpected estimation failures abort the run
    rather than silently disabling the guard.
  - TZ=UTC: the q subprocess always runs with TZ=UTC set.
  - Metrics: a stub metrics JSON is written at chunk start so a crash never
    leaves a gap; q updates it with load timing after the partition write.
  - Duplicate request-id detection: explicit --request-id values are checked
    against existing job records; a collision with different parameters aborts.

Python <-> q boundary:
  Python owns: API calls, downloads, manifests, job store, metrics stub.
  q owns:      CSV parsing, kdb+ writes, HDB partition management, quality checks.
  They communicate via CSV + JSON files in staging/.
"""

import argparse
import collections
import fcntl
import hashlib
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import databento as db
from metrics import ChunkMetrics, write_summary, print_metrics

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

class _JsonFormatter(logging.Formatter):
    """Emit each log record as a single JSON object."""

    def format(self, record: logging.LogRecord) -> str:
        obj = {
            "time": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        if record.exc_info and record.exc_info[1] is not None:
            obj["exc"] = self.formatException(record.exc_info)
        return json.dumps(obj, default=str)


_LOG_FMT = _JsonFormatter()


def _configure_logging() -> None:
    """Attach the stdout handler. Called once at module import.

    Guarded against re-import: if the root logger already has handlers
    (e.g. in tests that import this module multiple times) we do nothing,
    which prevents duplicate log lines.
    """
    root = logging.getLogger()
    if root.handlers:
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(_LOG_FMT)
    root.setLevel(logging.INFO)
    root.addHandler(handler)


def _add_file_logging(log_dir: Path, request_id: str) -> None:
    """Add a rotating file handler for a specific request. Called from main()."""
    from logging.handlers import RotatingFileHandler
    log_dir.mkdir(parents=True, exist_ok=True)
    fh = RotatingFileHandler(
        log_dir / f"{request_id}.log", maxBytes=10_000_000, backupCount=5,
    )
    fh.setFormatter(_LOG_FMT)
    logging.getLogger().addHandler(fh)


_configure_logging()
log = logging.getLogger("orchestrator")


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_DATASET = "XNAS.ITCH"
DEFAULT_SCHEMA = "trades"
DEFAULT_CHUNK_SIZE = 20

_HERE = Path(__file__).resolve().parent
STAGING_DIR = Path(os.environ.get("STAGING_DIR", str(_HERE / "../../staging")))
PACKAGE_HOME = Path(os.environ.get("PACKAGEHOME", str(_HERE / "../..")))

POLL_INTERVAL_S = 10
POLL_TIMEOUT_S = 3600


def _parse_env_float(name: str, default: float) -> float:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return float(raw)
    except ValueError:
        log.warning(f"Invalid {name}={raw!r} (not a number), using default {default}")
        return default


def _parse_env_int(name: str, default: int) -> int:
    raw = os.environ.get(name, "")
    if not raw:
        return default
    try:
        return int(raw)
    except ValueError:
        log.warning(f"Invalid {name}={raw!r} (not an integer), using default {default}")
        return default


MAX_COST_USD = _parse_env_float("BACKFILL_MAX_COST_USD", 50.0)
MAX_RETRIES = _parse_env_int("BACKFILL_MAX_RETRIES", 3)

# Throttle concurrent Databento API calls.  Allows up to 10 in-flight API
# requests at a time (submit, poll, download, estimate) even when the thread
# pool has 12 workers.  This prevents hammering the Databento API while still
# allowing good throughput.
_API_SEMAPHORE = threading.Semaphore(10)

# Job statuses in lifecycle order — chunks in these states are not re-submitted
TERMINAL_STATUSES = {"loaded", "verified"}


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Chunk:
    """One unit of work: a single day for a single batch of symbols."""
    request_id: str
    chunk_id: str
    dataset: str
    schema: str
    symbols: list
    date: date   # single calendar day
    stype_in: str = "raw_symbol"


@dataclass
class JobRecord:
    """
    Persistent state for one Chunk. Stored in staging/metadata/backfill_jobs (kdb binary table).
    Status lifecycle:
      submitted → running → downloaded → loaded → verified
                                               ↘ failed
    """
    chunk_id: str
    request_id: str
    databento_job_id: str = ""
    dataset: str = DEFAULT_DATASET
    schema: str = ""
    symbols: list = field(default_factory=list)
    date: str = ""          # ISO YYYY-MM-DD
    status: str = "pending"
    retries: int = 0
    error_msg: str = ""
    failure_type: str = ""     # api_error | download_error | parse_error | load_error | quality_error
    file_paths: list = field(default_factory=list)  # all CSVs from this job
    checksum: str = ""
    row_count: int = 0
    created_at: str = ""
    updated_at: str = ""

    def touch(self):
        self.updated_at = datetime.now(timezone.utc).isoformat()

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "JobRecord":
        # Only pass fields that exist in the dataclass to handle older records.
        # Migrate old records that stored file_path but not file_paths.
        if d.get("file_path") and not d.get("file_paths"):
            d = {**d, "file_paths": [d["file_path"]]}
        known = {f.name for f in cls.__dataclass_fields__.values()}
        return cls(**{k: v for k, v in d.items() if k in known})


# ---------------------------------------------------------------------------
# Job store — kdb binary table at staging/metadata/backfill_jobs
# ---------------------------------------------------------------------------

def _to_kdb_ts(iso_str: str) -> str:
    """Convert an ISO datetime string to kdb+ timestamp format.

    Input:  "2024-01-15T12:00:00.123456+00:00"  (from datetime.isoformat())
    Output: "2024.01.15T12:00:00.123456000"      (what q's "P"$ parser expects)
    Returns "" for empty/None input.
    """
    if not iso_str:
        return ""
    try:
        dt = datetime.fromisoformat(iso_str)
        if dt.tzinfo:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt.strftime("%Y.%m.%dT%H:%M:%S.") + f"{dt.microsecond:06d}000"
    except (ValueError, TypeError):
        return ""


def _from_kdb_ts(kdb_str: str) -> str:
    """Convert a kdb+ timestamp string back to ISO format.

    Input:  "2024.01.15T12:00:00.123456000"  (from q's string of timestamp)
    Output: "2024-01-15T12:00:00.123456+00:00"
    Returns "" for empty or null ("0Np") input.
    """
    if not kdb_str or kdb_str == "0Np":
        return ""
    try:
        date_part = kdb_str[:10].replace(".", "-")   # "2024.01.15" → "2024-01-15"
        time_part = kdb_str[11:19]                    # "12:00:00"
        frac = kdb_str[20:26] if len(kdb_str) > 20 else "000000"  # 6 microsecond digits
        return f"{date_part}T{time_part}.{frac}+00:00"
    except (ValueError, IndexError):
        return kdb_str


class KdbJobStore:
    """
    kdb-backed persistent job store with write batching.

    Writes are buffered in a Python-side dict and flushed to the kdb binary
    table in a single subprocess call (one q invocation per flush instead of
    one per save).  Reads are served from the in-memory cache after an initial
    load from disk.

    The binary table file lives at:
        <staging_dir>/metadata/backfill_jobs

    Thread safety: a threading.Lock serialises concurrent saves within one
    process.  A file-level flock prevents races across multiple orchestrator
    processes during flush.
    """

    # Flush to disk after this many buffered writes
    _FLUSH_THRESHOLD = 20

    def __init__(self, staging_dir: Path,
                 package_home: Path = None):
        if package_home is None:
            package_home = PACKAGE_HOME
        self.jobs_file = staging_dir / "metadata" / "backfill_jobs"
        self.package_home = package_home
        self._lock = threading.Lock()
        self.jobs_file.parent.mkdir(parents=True, exist_ok=True)
        # In-memory cache: chunk_id → JobRecord
        self._cache: dict[str, JobRecord] = {}
        self._dirty: dict[str, dict] = {}  # chunk_id → kdb-ready dict
        self._cache_loaded = False

    def _run_q(self, cmd: dict) -> str:
        """Spawn a q subprocess, load jobstore.q, return stdout."""
        q_script = "\\l code/backfill/jobstore.q\n"
        env = {**os.environ,
               "JOBS_FILE": str(self.jobs_file.resolve()),
               "JOBSTORE_CMD": json.dumps(cmd),
               "TZ": "UTC"}
        result = subprocess.run(
            ["q", "-q"],
            input=q_script,
            text=True,
            cwd=str(self.package_home),
            env=env,
            capture_output=True,
        )
        if result.returncode != 0:
            stderr = result.stderr.strip()
            raise RuntimeError(
                f"jobstore.q failed (rc={result.returncode}): {stderr}"
            )
        return result.stdout.strip()

    def _run_q_locked(self, cmd: dict) -> str:
        """Run a q operation with file-level locking for cross-process safety."""
        lock_path = self.jobs_file.parent / ".jobstore.lock"
        with open(lock_path, "a") as lf:
            fcntl.flock(lf, fcntl.LOCK_EX)
            try:
                return self._run_q(cmd)
            finally:
                fcntl.flock(lf, fcntl.LOCK_UN)

    def _record_to_dict(self, record: "JobRecord") -> dict:
        """Convert a JobRecord to a JSON-serializable dict for jobstore.q."""
        d = record.to_dict()
        d["created_at"] = _to_kdb_ts(d.get("created_at", ""))
        d["updated_at"] = _to_kdb_ts(d.get("updated_at", ""))
        d.setdefault("min_ts", "")
        d.setdefault("max_ts", "")
        d.setdefault("failure_type", "")
        d.setdefault("file_path", "")
        d.setdefault("dataset", record.dataset if hasattr(record, "dataset") else DEFAULT_DATASET)
        return d

    def _dict_to_record(self, d: dict) -> "JobRecord":
        """Convert a dict from jobstore.q JSON output back to a JobRecord."""
        d = dict(d)
        d["created_at"] = _from_kdb_ts(d.get("created_at", ""))
        d["updated_at"] = _from_kdb_ts(d.get("updated_at", ""))
        if isinstance(d.get("error_msg"), list):
            d["error_msg"] = d["error_msg"][0] if d["error_msg"] else ""
        date_val = d.get("date", "")
        if date_val and "." in date_val:
            d["date"] = date_val.replace(".", "-")
        return JobRecord.from_dict(d)

    def _parse_json(self, out: str, operation: str) -> list[dict]:
        """Parse JSON from jobstore.q output, with error handling."""
        try:
            return json.loads(out)
        except json.JSONDecodeError as exc:
            log.error(f"jobstore.q returned invalid JSON for {operation}: "
                      f"{exc}. Raw output: {out!r:.200}")
            return []

    def _ensure_cache(self) -> None:
        """Load the full job store from disk into the in-memory cache (once)."""
        if self._cache_loaded:
            return
        out = self._run_q({"op": "loadAll"})
        if out:
            rows = self._parse_json(out, "loadAll (cache init)")
            for d in rows:
                rec = self._dict_to_record(d)
                self._cache[rec.chunk_id] = rec
        self._cache_loaded = True

    def flush(self) -> None:
        """Write all buffered records to the kdb binary table in one subprocess."""
        if not self._dirty:
            return
        records = list(self._dirty.values())
        self._run_q_locked({
            "op": "batchUpsert",
            "records": records,
        })
        self._dirty.clear()

    def save(self, record: "JobRecord") -> None:
        record.touch()
        with self._lock:
            self._cache[record.chunk_id] = record
            self._dirty[record.chunk_id] = self._record_to_dict(record)
            if len(self._dirty) >= self._FLUSH_THRESHOLD:
                self.flush()

    def load(self, chunk_id: str) -> "JobRecord | None":
        with self._lock:
            self._ensure_cache()
            return self._cache.get(chunk_id)

    def load_all(self) -> "list[JobRecord]":
        with self._lock:
            self._ensure_cache()
            return list(self._cache.values())

    def load_failed(self) -> "list[JobRecord]":
        with self._lock:
            self._ensure_cache()
            return [r for r in self._cache.values()
                    if r.status == "failed" and r.retries < MAX_RETRIES]


# Keep backward-compatible name
JobStore = KdbJobStore


# ---------------------------------------------------------------------------
# US equity trading calendar
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Exchange calendars — pluggable per dataset prefix.
#
# To add a new exchange region:
#   1. Define its holiday set (e.g. _LSE_HOLIDAYS for London)
#   2. Add entries to _EXCHANGE_CALENDARS mapping dataset prefixes to the set
#
# Datasets not in the map fall through to _NYSE_HOLIDAYS (US default).
# European examples (not yet populated):
#   _LSE_HOLIDAYS:  set[date] = { ... }   # London Stock Exchange
#   _XETR_HOLIDAYS: set[date] = { ... }   # Deutsche Börse / XETRA
#   _XPAR_HOLIDAYS: set[date] = { ... }   # Euronext Paris
# ---------------------------------------------------------------------------

_NYSE_HOLIDAYS: set[date] = {
    # 2015
    date(2015, 1, 1),   # New Year's Day
    date(2015, 1, 19),  # MLK Day
    date(2015, 2, 16),  # Presidents' Day
    date(2015, 4, 3),   # Good Friday
    date(2015, 5, 25),  # Memorial Day
    date(2015, 7, 3),   # Independence Day (observed)
    date(2015, 9, 7),   # Labor Day
    date(2015, 11, 26), # Thanksgiving
    date(2015, 12, 25), # Christmas
    # 2016
    date(2016, 1, 1),   # New Year's Day
    date(2016, 1, 18),  # MLK Day
    date(2016, 2, 15),  # Presidents' Day
    date(2016, 3, 25),  # Good Friday
    date(2016, 5, 30),  # Memorial Day
    date(2016, 7, 4),   # Independence Day
    date(2016, 9, 5),   # Labor Day
    date(2016, 11, 24), # Thanksgiving
    date(2016, 12, 26), # Christmas (observed)
    # 2017
    date(2017, 1, 2),   # New Year's Day (observed)
    date(2017, 1, 16),  # MLK Day
    date(2017, 2, 20),  # Presidents' Day
    date(2017, 4, 14),  # Good Friday
    date(2017, 5, 29),  # Memorial Day
    date(2017, 7, 4),   # Independence Day
    date(2017, 9, 4),   # Labor Day
    date(2017, 11, 23), # Thanksgiving
    date(2017, 12, 25), # Christmas
    # 2018
    date(2018, 1, 1),   # New Year's Day
    date(2018, 1, 15),  # MLK Day
    date(2018, 2, 19),  # Presidents' Day
    date(2018, 3, 30),  # Good Friday
    date(2018, 5, 28),  # Memorial Day
    date(2018, 7, 4),   # Independence Day
    date(2018, 9, 3),   # Labor Day
    date(2018, 11, 22), # Thanksgiving
    date(2018, 12, 5),  # National Day of Mourning (George H.W. Bush)
    date(2018, 12, 25), # Christmas
    # 2019
    date(2019, 1, 1),   # New Year's Day
    date(2019, 1, 21),  # MLK Day
    date(2019, 2, 18),  # Presidents' Day
    date(2019, 4, 19),  # Good Friday
    date(2019, 5, 27),  # Memorial Day
    date(2019, 7, 4),   # Independence Day
    date(2019, 9, 2),   # Labor Day
    date(2019, 11, 28), # Thanksgiving
    date(2019, 12, 25), # Christmas
    # 2020
    date(2020, 1, 1),   # New Year's Day
    date(2020, 1, 20),  # MLK Day
    date(2020, 2, 17),  # Presidents' Day
    date(2020, 4, 10),  # Good Friday
    date(2020, 5, 25),  # Memorial Day
    date(2020, 7, 3),   # Independence Day (observed)
    date(2020, 9, 7),   # Labor Day
    date(2020, 11, 26), # Thanksgiving
    date(2020, 12, 25), # Christmas
    # 2021
    date(2021, 1, 1),   # New Year's Day
    date(2021, 1, 18),  # MLK Day
    date(2021, 2, 15),  # Presidents' Day
    date(2021, 4, 2),   # Good Friday
    date(2021, 5, 31),  # Memorial Day
    date(2021, 7, 5),   # Independence Day (observed)
    date(2021, 9, 6),   # Labor Day
    date(2021, 11, 25), # Thanksgiving
    date(2021, 12, 24), # Christmas (observed)
    # 2022
    date(2022, 1, 17),  # MLK Day
    date(2022, 2, 21),  # Presidents' Day
    date(2022, 4, 15),  # Good Friday
    date(2022, 5, 30),  # Memorial Day
    date(2022, 6, 20),  # Juneteenth (observed, first year)
    date(2022, 7, 4),   # Independence Day
    date(2022, 9, 5),   # Labor Day
    date(2022, 11, 24), # Thanksgiving
    date(2022, 12, 26), # Christmas (observed)
    # 2023
    date(2023, 1, 2),   # New Year's Day (observed)
    date(2023, 1, 16),  # MLK Day
    date(2023, 2, 20),  # Presidents' Day
    date(2023, 4, 7),   # Good Friday
    date(2023, 5, 29),  # Memorial Day
    date(2023, 6, 19),  # Juneteenth
    date(2023, 7, 4),   # Independence Day
    date(2023, 9, 4),   # Labor Day
    date(2023, 11, 23), # Thanksgiving
    date(2023, 12, 25), # Christmas
    # 2024
    date(2024, 1, 1),   # New Year's Day
    date(2024, 1, 15),  # MLK Day
    date(2024, 2, 19),  # Presidents' Day
    date(2024, 3, 29),  # Good Friday
    date(2024, 5, 27),  # Memorial Day
    date(2024, 6, 19),  # Juneteenth
    date(2024, 7, 4),   # Independence Day
    date(2024, 9, 2),   # Labor Day
    date(2024, 11, 28), # Thanksgiving
    date(2024, 12, 25), # Christmas
    # 2025
    date(2025, 1, 1),   # New Year's Day
    date(2025, 1, 20),  # MLK Day
    date(2025, 2, 17),  # Presidents' Day
    date(2025, 4, 18),  # Good Friday
    date(2025, 5, 26),  # Memorial Day
    date(2025, 6, 19),  # Juneteenth
    date(2025, 7, 4),   # Independence Day
    date(2025, 9, 1),   # Labor Day
    date(2025, 11, 27), # Thanksgiving
    date(2025, 12, 25), # Christmas
    # 2026
    date(2026, 1, 1),   # New Year's Day
    date(2026, 1, 19),  # MLK Day
    date(2026, 2, 16),  # Presidents' Day
    date(2026, 4, 3),   # Good Friday
    date(2026, 5, 25),  # Memorial Day
    date(2026, 6, 19),  # Juneteenth
    date(2026, 7, 3),   # Independence Day (observed)
    date(2026, 9, 7),   # Labor Day
    date(2026, 11, 26), # Thanksgiving
    date(2026, 12, 25), # Christmas
    # 2027
    date(2027, 1, 1),   # New Year's Day
    date(2027, 1, 18),  # MLK Day
    date(2027, 2, 15),  # Presidents' Day
    date(2027, 3, 26),  # Good Friday
    date(2027, 5, 31),  # Memorial Day
    date(2027, 6, 18),  # Juneteenth (observed)
    date(2027, 7, 5),   # Independence Day (observed)
    date(2027, 9, 6),   # Labor Day
    date(2027, 11, 25), # Thanksgiving
    date(2027, 12, 24), # Christmas (observed)
    # 2028
    date(2028, 1, 17),  # MLK Day
    date(2028, 2, 21),  # Presidents' Day
    date(2028, 4, 14),  # Good Friday
    date(2028, 5, 29),  # Memorial Day
    date(2028, 6, 19),  # Juneteenth
    date(2028, 7, 4),   # Independence Day
    date(2028, 9, 4),   # Labor Day
    date(2028, 11, 23), # Thanksgiving
    date(2028, 12, 25), # Christmas
    # 2029
    date(2029, 1, 1),   # New Year's Day
    date(2029, 1, 15),  # MLK Day
    date(2029, 2, 19),  # Presidents' Day
    date(2029, 3, 30),  # Good Friday
    date(2029, 5, 28),  # Memorial Day
    date(2029, 6, 19),  # Juneteenth
    date(2029, 7, 4),   # Independence Day
    date(2029, 9, 3),   # Labor Day
    date(2029, 11, 22), # Thanksgiving
    date(2029, 12, 25), # Christmas
    # 2030
    date(2030, 1, 1),   # New Year's Day
    date(2030, 1, 21),  # MLK Day
    date(2030, 2, 18),  # Presidents' Day
    date(2030, 4, 19),  # Good Friday
    date(2030, 5, 27),  # Memorial Day
    date(2030, 6, 19),  # Juneteenth
    date(2030, 7, 4),   # Independence Day
    date(2030, 9, 2),   # Labor Day
    date(2030, 11, 28), # Thanksgiving
    date(2030, 12, 25), # Christmas
}


# Map Databento dataset prefixes to their holiday calendars.
# All current US equity datasets share the NYSE calendar.
# Add entries here for non-US exchanges (e.g. "XLSE": _LSE_HOLIDAYS).
_EXCHANGE_CALENDARS: dict[str, set[date]] = {
    "XNAS": _NYSE_HOLIDAYS,    # NASDAQ
    "XNYS": _NYSE_HOLIDAYS,    # NYSE
    "IEXG": _NYSE_HOLIDAYS,    # IEX
    "EQUS": _NYSE_HOLIDAYS,    # Equity US Mini
    "XBOS": _NYSE_HOLIDAYS,    # NASDAQ BX
    "XPSX": _NYSE_HOLIDAYS,    # NASDAQ PSX
    "ARCX": _NYSE_HOLIDAYS,    # NYSE Arca
    "BATS": _NYSE_HOLIDAYS,    # Cboe BZX
    "EDGA": _NYSE_HOLIDAYS,    # Cboe EDGA
    "EDGX": _NYSE_HOLIDAYS,    # Cboe EDGX
}


def _get_holidays(dataset: str) -> set[date]:
    """Return the holiday set for a dataset, falling back to NYSE."""
    prefix = dataset.split(".")[0] if "." in dataset else dataset
    return _EXCHANGE_CALENDARS.get(prefix, _NYSE_HOLIDAYS)


def is_trading_day(d: date, dataset: str = "XNAS.ITCH") -> bool:
    """Return True if the given date is a trading day for the dataset's exchange."""
    if d.weekday() >= 5:
        return False
    if d in _get_holidays(dataset):
        return False
    return True


def trading_days(start: date, end: date, dataset: str = "XNAS.ITCH") -> list[date]:
    """Return the list of trading days in [start, end] for the given dataset."""
    holidays = _get_holidays(dataset)
    return [d for d in _date_range(start, end)
            if d.weekday() < 5 and d not in holidays]


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def generate_chunks(request_id: str, symbols: list[str], start: date,
                    end: date, chunk_size: int, schema: str,
                    dataset: str, stype_in: str = "raw_symbol") -> list[Chunk]:
    """
    Split a backfill request into one-day × one-symbol-batch chunks.

    Only trading days (weekdays excluding NYSE holidays) are included.
    Weekends and holidays are skipped automatically — no Databento API
    calls are made for non-trading days.

    Symbols are grouped into batches of chunk_size. Each batch maps to one
    Databento batch job. For chunk_size=20 with 40 symbols over 5 trading
    days this produces 2 batches × 5 days = 10 chunks.

    chunk_size=1 uses single-symbol chunk IDs (<request>_<date>_<SYM>).
    chunk_size>1 uses batch-index IDs (<request>_<date>_b000, b001, ...).
    """
    chunks = []
    batches = [symbols[i:i + chunk_size]
               for i in range(0, len(symbols), chunk_size)]
    d = start
    while d <= end:
        if not is_trading_day(d, dataset):
            d += timedelta(days=1)
            continue
        date_str = d.strftime("%Y.%m.%d")
        for batch_idx, batch_syms in enumerate(batches):
            if chunk_size == 1:
                safe_sym = re.sub(r"[^A-Za-z0-9]", "_", batch_syms[0])
                chunk_id = f"{request_id}_{date_str}_{safe_sym}"
            else:
                chunk_id = f"{request_id}_{date_str}_b{batch_idx:03d}"
            chunks.append(Chunk(
                request_id=request_id,
                chunk_id=chunk_id,
                dataset=dataset,
                schema=schema,
                symbols=batch_syms,
                date=d,
                stype_in=stype_in,
            ))
        d += timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------

def _classify_error(exc: Exception) -> str:
    """Categorise an exception into a pipeline failure type."""
    msg = str(exc).lower()
    if isinstance(exc, db.BentoError):
        return "api_error"
    if "timed out" in msg or "expired" in msg or "failed at databento" in msg:
        return "api_error"
    if "cost" in msg or "budget" in msg or "limit" in msg:
        return "api_error"
    if "no csv files" in msg or "checksum" in msg:
        return "download_error"
    if "cannot infer date" in msg or "parsing" in msg:
        return "parse_error"
    if "manifest" in msg or "permission denied" in msg:
        return "load_error"
    return "api_error"


def estimate_cost(client: db.Historical, dataset: str, symbols: list[str],
                  schema: str, start: str, end: str,
                  stype_in: str = "raw_symbol") -> float:
    try:
        with _API_SEMAPHORE:
            return float(client.metadata.get_cost(
                dataset=dataset, symbols=symbols, schema=schema,
                start=start, end=end, stype_in=stype_in,
            ))
    except db.BentoError as exc:
        # Databento API errors (e.g. unknown dataset, bad symbol) — raise so the
        # caller is never left without a cost guard. Fix the dataset/symbol and retry.
        raise RuntimeError(
            f"Cost estimation failed: {exc}. "
            "Cannot submit without cost data — fix the dataset/symbol and retry, "
            "or set BACKFILL_MAX_COST_USD=0 to disable the guard explicitly."
        ) from exc
    except Exception as exc:
        # Unexpected errors (network timeout, SDK bug, etc.) — re-raise so
        # the caller is not silently left without a cost guard.
        raise RuntimeError(f"Cost estimation failed unexpectedly: {exc}") from exc


# ---------------------------------------------------------------------------
# Submit / poll / download / convert (unchanged from M1)
# ---------------------------------------------------------------------------

def submit_job(client: db.Historical, dataset: str, symbols: list[str],
               schema: str, start: str, end: str,
               stype_in: str = "raw_symbol") -> dict:
    log.info(f"Submitting: dataset={dataset} schema={schema} "
                f"symbols={symbols} start={start} end={end} stype_in={stype_in}")
    # Request CSV directly so no local DBN-to-CSV conversion is needed.
    # pretty_px/pretty_ts give human-readable prices and ISO timestamps,
    # which is exactly what loader.q expects. map_symbols=True adds the
    # symbol column to the output.
    with _API_SEMAPHORE:
        job = client.batch.submit_job(
            dataset=dataset, symbols=symbols, schema=schema,
            start=start, end=end,
            encoding="csv", compression=None,
            pretty_px=True, pretty_ts=True, map_symbols=True,
            stype_in=stype_in,
        )
    log.info(f"Submitted job_id={job['id']} state={job.get('state')}")
    return job


def poll_until_done(client: db.Historical, job_id: str) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_S
    while True:
        if time.monotonic() > deadline:
            raise RuntimeError(f"Timed out waiting for job {job_id}")
        # Fetch all non-terminal + expired states in a single API call to
        # avoid a second round-trip when the job expires between calls.
        try:
            with _API_SEMAPHORE:
                jobs = client.batch.list_jobs(
                    states=["queued", "processing", "done", "expired"]
                )
        except Exception as sdk_exc:
            if "failed" in str(sdk_exc).lower() or "jobstate" in str(sdk_exc).lower():
                raise RuntimeError(
                    f"Job {job_id} failed at Databento: {sdk_exc}"
                ) from sdk_exc
            raise
        match = next((j for j in jobs if j["id"] == job_id), None)
        if match is None:
            raise RuntimeError(f"Job {job_id} not found")
        state = match.get("state", "")
        log.info(f"Polling job_id={job_id} state={state}")
        if state == "done":
            return match
        if state == "expired":
            raise RuntimeError(f"Job {job_id} expired")
        time.sleep(POLL_INTERVAL_S)


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def count_csv_rows(path: Path) -> int:
    """Count data rows in a CSV file (excluding the header).

    Reads only raw bytes to count newlines efficiently without decoding
    the entire file into Python strings.
    """
    count = 0
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            count += chunk.count(b"\n")
    return max(count - 1, 0)  # subtract header line


def infer_date_from_filename(name: str) -> str:
    """Extract YYYY-MM-DD from a Databento CSV filename.

    Anchored to the known Databento delivery formats to avoid matching
    hashes or other digit runs that appear before the date:
      - xnas-itch-20240603.trades.csv
      - equs-mini-20240117.ohlcv-1m.csv
      - DBNJ-XXXXX_20240603_trades.csv  (legacy)
    """
    m = re.search(r"[-_](\d{4})(\d{2})(\d{2})[._]", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    raise ValueError(
        f"Cannot infer date from filename {name!r}: "
        "expected a name containing YYYY-MM-DD or YYYYMMDD preceded by '-' or '_'"
    )


def download_csv(client: db.Historical, job_id: str,
                 chunk_dir: Path) -> list[Path]:
    """Download a completed batch job and return the CSV file paths.

    Because we submit with encoding='csv', the service delivers CSVs
    directly — no local conversion step is needed.
    """
    chunk_dir.mkdir(parents=True, exist_ok=True)

    log.info(f"Downloading job_id={job_id}")
    with _API_SEMAPHORE:
        downloaded = client.batch.download(job_id=job_id, output_dir=str(chunk_dir))
    log.info(f"Downloaded {len(downloaded)} file(s)")

    csv_paths = [pp for p in downloaded if (pp := Path(p)).suffix == ".csv"]
    for p in csv_paths:
        log.info(f"CSV: {p.name} ({p.stat().st_size} bytes)")
    return csv_paths


def write_manifest(request_id: str, chunk_id: str, job_id: str, schema: str,
                   symbols: list[str], csv_path: Path,
                   manifest_dir: Path, dataset: str = DEFAULT_DATASET,
                   row_count: int | None = None,
                   checksum: str | None = None) -> dict:
    """Write a manifest JSON for one CSV file, atomically (tmp + rename).

    row_count and checksum may be passed in by the caller to avoid re-scanning
    a file that was already hashed/counted during download.  If omitted, they
    are computed here (used on the resume path where the values weren't cached).
    """
    manifest_dir.mkdir(parents=True, exist_ok=True)
    if row_count is None:
        row_count = count_csv_rows(csv_path)
    if checksum is None:
        checksum = sha256_of_file(csv_path)
    manifest = {
        "request_id": request_id,
        "chunk_id": chunk_id,
        "databento_job_id": job_id,
        "exchange": dataset,
        "schema": schema,
        "date": infer_date_from_filename(csv_path.name),
        "symbols": symbols,
        "file_path": str(csv_path.resolve()),
        "row_count": row_count,
        "checksum": checksum,
        "min_ts": "",
        "max_ts": "",
        "created_at": datetime.now(timezone.utc).isoformat(),
    }
    manifest_path = manifest_dir / f"{chunk_id}.json"
    tmp = manifest_path.with_suffix(".tmp")
    with open(tmp, "w") as f:
        json.dump(manifest, f, indent=2)
    tmp.rename(manifest_path)
    log.info(f"Manifest: {manifest_path.name} rows={row_count}")
    return manifest


def _cleanup_staging_csvs(job_store: "KdbJobStore", staging_dir: Path,
                          request_id: str | None = None) -> int:
    """Remove CSV directories for chunks that reached 'verified' status.

    Returns the number of directories cleaned up.  Failures are logged as
    warnings and never abort the pipeline — the CSVs are no longer needed
    once the data is in the HDB.
    """
    records = job_store.load_all()
    if request_id:
        records = [r for r in records if r.request_id == request_id]
    verified = [r for r in records if r.status == "verified"]
    cleaned = 0
    for r in verified:
        chunk_dir = staging_dir / r.chunk_id
        if chunk_dir.is_dir():
            try:
                shutil.rmtree(chunk_dir)
                cleaned += 1
            except OSError as exc:
                log.warning(f"Could not clean up {chunk_dir}: {exc}")
    if cleaned:
        log.info(f"Cleaned up {cleaned} staging CSV director(ies)")
    return cleaned


# ---------------------------------------------------------------------------
# q loader invocation
# ---------------------------------------------------------------------------

def _is_pid_alive(pid: int) -> bool:
    """Check whether a process with the given PID is still running."""
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        # Process exists but we don't have permission to signal it
        return True


def _acquire_lock_with_timeout(fh, timeout_s: int = 300) -> None:
    """Acquire an exclusive flock with a timeout and stale-lock detection.

    Uses LOCK_NB (non-blocking) with a retry loop so a hung or crashed process
    that holds the lock does not cause this process to block indefinitely.

    Stale lock detection: the holding PID is written to the lock file after
    acquisition.  On contention, we read that PID and check if it's alive.
    If the holding process is dead, we log a warning and delete the stale lock
    file so the next attempt succeeds.

    Raises RuntimeError if the lock cannot be acquired within timeout_s seconds.
    """
    deadline = time.monotonic() + timeout_s
    stale_checked = False
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            # Write our PID so other processes can detect stale locks
            fh.seek(0)
            fh.truncate()
            fh.write(str(os.getpid()))
            fh.flush()
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Could not acquire q loader lock within {timeout_s}s. "
                    "Another process may be holding it, or a previous run crashed "
                    f"while holding the lock. Delete the stale lock file to recover: "
                    f"{fh.name}"
                )
            # Stale lock detection: check once per acquisition attempt cycle
            if not stale_checked:
                stale_checked = True
                try:
                    fh.seek(0)
                    content = fh.read().strip()
                    if content and content.isdigit():
                        holder_pid = int(content)
                        if not _is_pid_alive(holder_pid):
                            log.warning(
                                f"Stale lock detected: PID {holder_pid} is no longer "
                                f"running. Releasing stale lock: {fh.name}"
                            )
                            # Close and recreate the lock file to break the stale flock
                            lock_path = fh.name
                            fh.close()
                            os.unlink(lock_path)
                            # Caller will re-open; signal via RuntimeError
                            raise _StaleLockRemoved(lock_path)
                except (_StaleLockRemoved, OSError):
                    raise
                except Exception:
                    pass  # best-effort; fall through to normal retry
            time.sleep(5)


class _StaleLockRemoved(Exception):
    """Internal signal: stale lock file was removed, caller should retry."""
    pass


def run_q_loader(package_home: Path, manifest_dir: Path,
                 request_id: str | None = None) -> None:
    hdb_dir = os.environ.get("KDBHDB", str(package_home / "hdb"))
    loader_script = package_home / "code" / "backfill" / "loader.q"

    if not loader_script.exists():
        raise FileNotFoundError(f"Loader script not found: {loader_script}")

    # Acquire an exclusive process-level lock before invoking the q loader.
    # This prevents concurrent orchestrator processes (e.g. parallel chunk runs
    # or a manual re-run) from calling .Q.dpft on the same partition simultaneously.
    lock_path = Path(hdb_dir).parent / ".q_loader.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)

    # If a stale lock is detected (dead PID), the lock file is removed and
    # _StaleLockRemoved is raised.  We retry once with a fresh file handle.
    for attempt in range(2):
        lock_fh = open(lock_path, "a+")
        try:
            _acquire_lock_with_timeout(lock_fh, timeout_s=300)
            try:
                _run_q_loader_locked(package_home, manifest_dir, hdb_dir,
                                     request_id=request_id)
            finally:
                fcntl.flock(lock_fh, fcntl.LOCK_UN)
            return  # success
        except _StaleLockRemoved:
            log.info("Retrying lock acquisition after stale lock removal")
            continue
        finally:
            if not lock_fh.closed:
                lock_fh.close()


def _hdb_already_loaded_dates(hdb_dir: Path, dates: list[date],
                               schema: str, dataset: str) -> set[date]:
    """Return the subset of dates already in the HDB for (schema, exchange=dataset).

    Runs a compact q one-liner that reads each partition's exchange column file
    and checks whether the target exchange is present.  Failures are swallowed
    so a broken HDB never blocks a fresh backfill.
    """
    if not hdb_dir.exists() or not dates:
        return set()

    schema_internal = schema.replace("-", "_")
    # Build kdb+ date literals: 2024.06.10 2024.06.11 (space-separated)
    dates_literal = " ".join(d.strftime("%Y.%m.%d") for d in sorted(dates))

    # Inline q script: load sym (for enumeration), then for each date check
    # whether the target exchange appears in the partition's exchange column.
    q_script = (
        f'hdb:hsym`$"{hdb_dir}";'
        f'schema:`{schema_internal};'
        f'exch:`$"{dataset}";'
        f'dates:{dates_literal};'
        f'symFile:` sv hdb,`sym;'
        f'if[count key symFile;sym:get symFile];'
        f'existing:{{[d]'
        f' pd:` sv hdb,(`$string d),schema;'
        f' if[not count key pd;:0b];'
        f' ef:` sv pd,`exchange;'
        f' if[not count key ef;:0b];'
        f' exch in distinct get ef'
        f'}} each dates;'
        f'-1 each string dates where existing;\n'
        f'exit 0\n'
    )

    try:
        result = subprocess.run(
            ["q", "-q"], input=q_script, text=True,
            capture_output=True, cwd=str(hdb_dir),
            env={**os.environ, "TZ": "UTC"},
            timeout=30,
        )
        existing: set[date] = set()
        for line in result.stdout.splitlines():
            line = line.strip()
            if line:
                try:
                    # q outputs dates as YYYY.MM.DD; convert to ISO YYYY-MM-DD
                    existing.add(date.fromisoformat(line.replace(".", "-")))
                except ValueError:
                    pass
        return existing
    except Exception as exc:
        log.warning(f"Pre-flight HDB check failed (proceeding anyway): {exc}")
        return set()


def _run_ref_ingest(symbols: list[str], start: date, end: date) -> None:
    """Invoke ref_ingest.py for the given symbols/range after a successful load.

    Best-effort: failures are logged as warnings and never abort the backfill.
    The market data is already in the HDB; reference data is supplementary.
    """
    script = PACKAGE_HOME / "code" / "reference" / "ref_ingest.py"
    if not script.exists():
        log.warning("ref_ingest.py not found — skipping reference data update")
        return
    cmd = [
        sys.executable, str(script),
        "--symbols", ",".join(symbols),
        "--start", str(start),
        "--end", str(end),
    ]
    env = {**os.environ, "STAGING_DIR": str(STAGING_DIR)}
    log.info(f"Updating reference data: symbols={symbols} range={start}..{end}")
    result = subprocess.run(
        cmd, capture_output=True, text=True,
        cwd=str(PACKAGE_HOME), env=env,
    )
    for line in result.stdout.splitlines():
        log.info(f"[ref_ingest] {line}")
    if result.returncode != 0:
        log.warning(
            f"ref_ingest exited {result.returncode} — "
            "reference data may be incomplete (market data load was successful)"
        )


def _run_q_loader_locked(package_home: Path, manifest_dir: Path,
                         hdb_dir: str,
                         request_id: str | None = None) -> None:
    # Pipe q commands via stdin so we can load the script then call runLoader[].
    # cwd=package_home ensures relative \l paths inside loader.q resolve correctly.
    q_script = "\\l code/backfill/loader.q\nrunLoader[]\nexit 0\n"
    cmd = ["q", "-q"]
    jobs_file = manifest_dir.parent / "backfill_jobs"
    env = {**os.environ,
           "STAGING_DIR": str(manifest_dir.parent.parent),
           "JOBS_FILE": str(jobs_file.resolve()),
           "KDBHDB": hdb_dir,
           "TZ": "UTC"}
    # Pass REQUEST_ID so manifest.q can filter to only this run's manifests,
    # preventing cross-contamination when parallel backfill runs share the
    # staging/metadata/manifests/ directory.
    if request_id:
        env["REQUEST_ID"] = request_id

    log.info(f"Invoking q loader: cwd={package_home}")
    result = subprocess.run(
        cmd, input=q_script, text=True,
        cwd=str(package_home), env=env, capture_output=True,
    )
    for line in result.stdout.splitlines():
        log.info(f"[q] {line}")
    for line in result.stderr.splitlines():
        log.warning(f"[q stderr] {line}")
    if result.returncode != 0:
        raise RuntimeError(f"q loader exited {result.returncode}")
    log.info("q loader done")


# ---------------------------------------------------------------------------
# Per-chunk pipeline
# ---------------------------------------------------------------------------

def _write_manifests_for_record(chunk: Chunk, record: "JobRecord",
                                 manifest_dir: Path) -> None:
    """Write manifests for all CSVs stored in a job record (used on resume)."""
    all_paths = record.file_paths
    for part_idx, raw_path in enumerate(all_paths):
        part_path = Path(raw_path)
        if not part_path.exists():
            log.warning(f"Chunk {chunk.chunk_id}: CSV not found on resume, "
                           f"skipping manifest for {part_path.name}")
            continue
        part_chunk_id = (chunk.chunk_id if part_idx == 0
                         else f"{chunk.chunk_id}_part{part_idx + 1}")
        write_manifest(chunk.request_id, part_chunk_id,
                       record.databento_job_id, chunk.schema,
                       chunk.symbols, part_path, manifest_dir,
                       dataset=chunk.dataset)


def run_chunk(client: db.Historical, chunk: Chunk, job_store: JobStore,
              staging_dir: Path, manifest_dir: Path) -> bool:
    """
    Execute the full pipeline for one chunk. Returns True on success.

    Idempotency: if the job store already shows this chunk as loaded/verified,
    we skip immediately. If it was previously downloaded, we verify the stored
    checksum matches the file on disk before skipping to manifest writing
    (re-downloads if the file was corrupted or deleted).
    """
    record = job_store.load(chunk.chunk_id)
    date_str = chunk.date.isoformat()
    # Databento uses exclusive end: to request a single day, end = day + 1
    end_str = (chunk.date + timedelta(days=1)).isoformat()

    # Start metrics
    metrics = ChunkMetrics(
        chunk_id=chunk.chunk_id,
        request_id=chunk.request_id,
        schema=chunk.schema,
        date=date_str,
        symbols=chunk.symbols,
    )
    metrics.mark("total_start")
    # Write an in-progress stub immediately so a crash leaves a partial record
    # rather than a gap in the metrics directory for this chunk.
    metrics.save(staging_dir)

    # ---- Already done? ----
    if record and record.status in TERMINAL_STATUSES:
        log.info(f"Chunk {chunk.chunk_id} already {record.status}, skipping")
        return True

    # ---- Resume from downloaded, or failed-after-download? ----
    # If a previous run completed the download but then crashed, skip the
    # re-download as long as the primary CSV still exists with a matching checksum.
    if record and record.status in ("downloaded", "failed") and record.file_paths:
        csv_path = Path(record.file_paths[0])
        if csv_path.exists():
            # If no checksum was stored it means the previous run crashed during
            # download before the file was fully written.  Treat a missing checksum
            # the same as a mismatch: re-download rather than trusting a potentially
            # partial file.
            checksum_ok = (bool(record.checksum)
                           and sha256_of_file(csv_path) == record.checksum)
            if not checksum_ok:
                log.warning(
                    f"Chunk {chunk.chunk_id}: checksum mismatch on resume "
                    f"(expected {record.checksum[:16]}...) — re-downloading"
                )
                record.status = "pending"
                record.checksum = ""
                record.file_paths = []
                job_store.save(record)
                # Fall through to re-submit below
            else:
                log.info(f"Chunk {chunk.chunk_id}: resume OK, skipping download")
                _write_manifests_for_record(chunk, record, manifest_dir)
                record.status = "loaded"
                job_store.save(record)
                return True

    # ---- Resume from submitted/running? ----
    job_id = None
    if record and record.status in ("submitted", "running") and record.databento_job_id:
        log.info(f"Chunk {chunk.chunk_id} resuming from {record.status}, "
                    f"polling job {record.databento_job_id}")
        job_id = record.databento_job_id
    else:
        # Fresh start — initialise record
        retries = record.retries if record else 0
        record = JobRecord(
            chunk_id=chunk.chunk_id,
            request_id=chunk.request_id,
            dataset=chunk.dataset,
            schema=chunk.schema,
            symbols=chunk.symbols,
            date=date_str,
            retries=retries,
            created_at=datetime.now(timezone.utc).isoformat(),
        )

    try:
        # ---- Submit if we don't already have a job_id ----
        if job_id is None:
            cost = estimate_cost(client, chunk.dataset, chunk.symbols,
                                 chunk.schema, date_str, end_str,
                                 stype_in=chunk.stype_in)
            if MAX_COST_USD > 0 and cost > MAX_COST_USD:
                raise RuntimeError(
                    f"Cost ${cost:.4f} exceeds limit ${MAX_COST_USD:.2f}"
                )

            metrics.mark("submit_start")
            record.status = "submitted"
            job_store.save(record)

            job = submit_job(client, chunk.dataset, chunk.symbols,
                             chunk.schema, date_str, end_str,
                             stype_in=chunk.stype_in)
            job_id = job["id"]
            record.databento_job_id = job_id
            record.status = "running"
            job_store.save(record)
            metrics.mark("submit_end")

        # ---- Poll ----
        metrics.mark("poll_start")
        job = poll_until_done(client, job_id)
        metrics.mark("poll_end")
        log.info(f"Job {job_id} done, cost={job.get('cost')}")

        # ---- Download ----
        metrics.mark("download_start")
        chunk_dir = staging_dir / chunk.chunk_id
        csv_paths = download_csv(client, job_id, chunk_dir)
        if not csv_paths:
            raise RuntimeError("No CSV files produced after download")

        if len(csv_paths) > 1:
            log.warning(
                f"Job {job_id} delivered {len(csv_paths)} CSVs; "
                "writing one manifest per file"
            )

        # Use primary CSV for job-store record (first file)
        csv_path = csv_paths[0]
        record.file_paths = [str(p.resolve()) for p in csv_paths]
        record.checksum = sha256_of_file(csv_path)
        record.row_count = count_csv_rows(csv_path)
        record.status = "downloaded"
        job_store.save(record)
        metrics.mark("download_end")
        metrics.row_count = record.row_count
        metrics.file_bytes = csv_path.stat().st_size

        # ---- Write manifest (one per CSV) ----
        # Pass the already-computed row_count/checksum for the primary CSV to
        # avoid re-reading it here.  Additional parts (part_idx > 0) are scanned
        # fresh since their values were not computed during the download phase.
        for part_idx, part_path in enumerate(csv_paths):
            part_chunk_id = (chunk.chunk_id if part_idx == 0
                             else f"{chunk.chunk_id}_part{part_idx + 1}")
            write_manifest(chunk.request_id, part_chunk_id, job_id,
                           chunk.schema, chunk.symbols, part_path, manifest_dir,
                           dataset=chunk.dataset,
                           row_count=record.row_count if part_idx == 0 else None,
                           checksum=record.checksum if part_idx == 0 else None)

        record.status = "loaded"
        job_store.save(record)

        metrics.mark("total_end")
        metrics.save(staging_dir)
        return True

    except Exception as exc:
        ft = _classify_error(exc)
        record.retries += 1
        record.status = "failed"
        record.error_msg = str(exc)
        record.failure_type = ft
        job_store.save(record)
        metrics.failure_type = ft
        metrics.mark("total_end")
        metrics.save(staging_dir)
        log.exception(f"Chunk {chunk.chunk_id} failed [{ft}] (attempt {record.retries}): {exc}")
        return False


# ---------------------------------------------------------------------------
# Parallel chunk runner
# ---------------------------------------------------------------------------

def _run_chunks_parallel(client: db.Historical, chunks: list[Chunk],
                         job_store: JobStore, staging_dir: Path,
                         manifest_dir: Path,
                         max_workers: int,
                         backoffs: dict | None = None) -> tuple[int, int]:
    """
    Run chunks concurrently using a thread pool. Returns (succeeded, failed).

    backoffs: optional dict mapping chunk_id → seconds to sleep before starting.
              Used by --retry-failed to honour per-chunk exponential backoff without
              serialising the whole retry queue.

    Thread safety: each chunk writes to its own files (job store, manifest,
    metrics, staging dir) so no shared mutable state requires locking here.
    The q loader is invoked once by the caller after all futures complete.
    """
    def _work(chunk: Chunk) -> bool:
        wait = (backoffs or {}).get(chunk.chunk_id, 0)
        if wait:
            log.info(f"Chunk {chunk.chunk_id}: waiting {wait}s before retry")
            time.sleep(wait)
        return run_chunk(client, chunk, job_store, staging_dir, manifest_dir)

    succeeded = 0
    failed = 0
    total = len(chunks)
    done = 0
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(_work, chunk): chunk for chunk in chunks}
        for future in as_completed(futures):
            chunk = futures[future]
            try:
                ok = future.result()
            except Exception as exc:
                log.exception(f"Chunk {chunk.chunk_id} raised unexpected exception: {exc}")
                ok = False
            done += 1
            if ok:
                succeeded += 1
            else:
                failed += 1
            status = "ok" if ok else "FAIL"
            if len(chunk.symbols) == 1:
                label = chunk.symbols[0]
            else:
                label = f"{chunk.symbols[0]}+{len(chunk.symbols) - 1}"
            print(f"\r  [{done}/{total}] {label} {chunk.date} — {status}    ",
                  end="", flush=True)
    print()  # newline after progress line
    return succeeded, failed


# ---------------------------------------------------------------------------
# Run summary — human-readable terminal output
# ---------------------------------------------------------------------------

def _classify_failure(error_msg: str, failure_type: str) -> str:
    """Return a human-readable reason for a chunk failure."""
    ft = failure_type.lower()
    em = error_msg.lower()
    if ft == "quality_error":
        return "quality check failed (duplicates / ordering / nulls)"
    if ft == "load_error":
        return "q loader error"
    if "weekend" in em or "holiday" in em or "no data" in em or "empty" in em:
        return "no data (weekend / public holiday / trading halt)"
    if ft == "download_error" or "download" in em:
        return "download error"
    if ft == "api_error" or "databento" in em or "api" in em:
        return "Databento API error"
    if ft == "parse_error":
        return "CSV parse error"
    if "cost" in em or "limit" in em:
        return "cost limit exceeded"
    if "timeout" in em:
        return "poll timeout"
    return error_msg[:120] if error_msg else "unknown error"


def _print_run_summary(records: list, title: str = "Backfill Summary") -> None:
    """Print a human-readable terminal summary after a run completes."""
    verified = [r for r in records if r.status == "verified"]
    failed   = [r for r in records if r.status == "failed"]

    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

    if verified:
        # Group by dataset (exchange) → collect unique syms and total rows
        by_exchange: dict[str, dict] = collections.defaultdict(lambda: {"syms": set(), "rows": 0})
        for r in verified:
            by_exchange[r.dataset]["syms"].update(r.symbols)
            by_exchange[r.dataset]["rows"] += r.row_count

        all_syms = sorted({s for r in verified for s in r.symbols})
        total_rows = sum(r.row_count for r in verified)

        print(f"\n  STATUS : SUCCESS ({len(verified)} chunk(s) verified)")
        print(f"  SYMBOLS: {', '.join(all_syms)}")
        print()
        print(f"  {'Exchange':<20} {'Rows':>12}  Symbols")
        print(f"  {'-'*55}")
        for exchange, info in sorted(by_exchange.items()):
            syms_str = ", ".join(sorted(info["syms"]))
            print(f"  {exchange:<20} {info['rows']:>12,}  {syms_str}")
        print(f"  {'-'*55}")
        print(f"  {'TOTAL':<20} {total_rows:>12,}")
    else:
        print(f"\n  STATUS : NO DATA LOADED")

    if failed:
        print(f"\n  FAILURES ({len(failed)} chunk(s)):")
        # Group failures by reason so repeated causes appear once

        reasons = collections.Counter(
            _classify_failure(r.error_msg, r.failure_type) for r in failed
        )
        for reason, count in reasons.most_common():
            print(f"    x{count}  {reason}")
        print(f"\n  Tip: run with --retry-failed to requeue, or "
              f"--status for per-chunk detail.")

    print(f"{'='*60}\n")


# ---------------------------------------------------------------------------
# Status display
# ---------------------------------------------------------------------------

def print_status(staging_dir: Path, request_id: str | None = None) -> None:
    """Print a summary table of all job records in the job store."""
    store = JobStore(staging_dir)
    records = store.load_all()

    if not records:
        print("No job records found.")
        return

    if request_id:
        records = [r for r in records if r.request_id == request_id]
        if not records:
            print(f"No records for request_id={request_id}")
            return

    # Group by request_id

    by_req: dict[str, list[JobRecord]] = collections.defaultdict(list)
    for r in records:
        by_req[r.request_id].append(r)

    print(f"\n{'request_id':<40} {'schema':<10} {'chunks':>6} "
          f"{'submitted':>9} {'running':>7} {'downloaded':>10} "
          f"{'loaded':>6} {'verified':>8} {'failed':>6}")
    print("-" * 110)

    for req_id, recs in sorted(by_req.items()):
        counts = collections.Counter(r.status for r in recs)
        schema = recs[0].schema if recs else ""
        print(f"{req_id:<40} {schema:<10} {len(recs):>6} "
              f"{counts.get('submitted', 0):>9} "
              f"{counts.get('running', 0):>7} "
              f"{counts.get('downloaded', 0):>10} "
              f"{counts.get('loaded', 0):>6} "
              f"{counts.get('verified', 0):>8} "
              f"{counts.get('failed', 0):>6}")

        # Print failed chunks with their error messages
        for r in recs:
            if r.status == "failed":
                ft = f" [{r.failure_type}]" if r.failure_type else ""
                print(f"  FAILED {r.chunk_id}{ft}: {r.error_msg[:80]}")

    print()


def print_failures(staging_dir: Path, request_id: str | None = None) -> None:
    """Print a detailed failure breakdown grouped by type, date, and symbol."""
    store = JobStore(staging_dir)
    records = store.load_all()
    if request_id:
        records = [r for r in records if r.request_id == request_id]
    failed = [r for r in records if r.status == "failed"]

    if not failed:
        print("No failed chunks found.")
        return

    print(f"\n{'='*70}")
    print(f"  Failure Report — {len(failed)} chunk(s)")
    print(f"{'='*70}\n")

    # Group by failure type
    by_type: dict[str, list] = collections.defaultdict(list)
    for r in failed:
        by_type[r.failure_type or "unknown"].append(r)

    for ft, recs in sorted(by_type.items()):
        print(f"  {ft} ({len(recs)} chunk(s)):")
        for r in sorted(recs, key=lambda x: (x.date, x.chunk_id)):
            syms = ", ".join(r.symbols[:3])
            if len(r.symbols) > 3:
                syms += f" +{len(r.symbols) - 3}"
            err = r.error_msg[:60] if r.error_msg else ""
            print(f"    {r.date}  {syms:<20s}  retries={r.retries}  {err}")
        print()

    print(f"  Tip: run with --retry-failed to requeue these chunks.")
    print(f"{'='*70}\n")


def print_gaps(hdb_dir: Path, symbols: list[str], start: date, end: date,
               schema: str, dataset: str) -> None:
    """Print a per-symbol gaps report showing missing trading days in the HDB.

    Only trading days (weekdays excluding NYSE holidays) are checked.
    Weekends and holidays are excluded automatically.
    """
    schema_internal = schema.replace("-", "_")
    check_dates = trading_days(start, end, dataset)
    if not check_dates:
        print(f"No trading days in {start}..{end}")
        return

    skipped = len(list(_date_range(start, end))) - len(check_dates)

    # Build q script that checks each date × sym for data presence
    dates_literal = " ".join(d.strftime("%Y.%m.%d") for d in check_dates)
    syms_literal = "`" + "`".join(symbols)

    q_script = (
        f'hdb:hsym`$"{hdb_dir}";'
        f'system "l ",1_string hdb;'
        f'dates:{dates_literal};'
        f'syms:{syms_literal};'
        f'schema:`{schema_internal};'
        f'exch:`$"{dataset}";'
        f'r:{{[s] present:{{[s;d] '
        f'  t:select from {schema_internal} where date=d,sym=s,exchange=exch;'
        f'  count t'
        f'}}[s;] each dates;'
        f'-1 (string s),"|",("|" sv string present)'
        f'}} each syms;\n'
        f'exit 0\n'
    )

    try:
        result = subprocess.run(
            ["q", "-q"], input=q_script, text=True,
            capture_output=True, cwd=str(hdb_dir),
            env={**os.environ, "TZ": "UTC"},
            timeout=60,
        )
    except Exception as exc:
        log.warning(f"Gaps report failed: {exc}")
        print(f"Could not generate gaps report: {exc}")
        return

    print(f"\n{'='*70}")
    print(f"  Gaps Report — {dataset} {schema} {start}..{end}")
    print(f"  Trading days: {len(check_dates)}  (skipped {skipped} weekends/holidays)")
    print(f"{'='*70}\n")
    print(f"  {'Symbol':<10} {'Present':>7} {'Missing':>7} {'Coverage':>8}  Missing dates")
    print(f"  {'-'*65}")

    total_expected = 0
    total_present = 0
    for line in result.stdout.splitlines():
        line = line.strip()
        if not line or "|" not in line:
            continue
        parts = line.split("|")
        sym = parts[0]
        counts = [int(x) for x in parts[1:] if x]
        if len(counts) != len(check_dates):
            continue
        present = sum(1 for c in counts if c > 0)
        missing = len(check_dates) - present
        total_expected += len(check_dates)
        total_present += present
        pct = f"{100 * present / len(check_dates):.0f}%"
        missing_dates = [str(d) for d, c in zip(check_dates, counts) if c == 0]
        missing_str = ", ".join(missing_dates[:5])
        if len(missing_dates) > 5:
            missing_str += f" +{len(missing_dates) - 5} more"
        print(f"  {sym:<10} {present:>7} {missing:>7} {pct:>8}  {missing_str}")

    print(f"  {'-'*65}")
    total_pct = f"{100 * total_present / total_expected:.0f}%" if total_expected else "—"
    print(f"  {'TOTAL':<10} {total_present:>7} {total_expected - total_present:>7} {total_pct:>8}")
    print(f"{'='*70}\n")


def _date_range(start: date, end: date) -> list[date]:
    """Return a list of dates from start to end inclusive."""
    result = []
    d = start
    while d <= end:
        result.append(d)
        d += timedelta(days=1)
    return result


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Databento historical batch backfill orchestrator."
    )
    # --symbols/--start/--end are required UNLESS --retry-failed or --status
    parser.add_argument("--symbols", default=None,
                        help="Comma-separated symbols, e.g. AAPL,MSFT")
    parser.add_argument("--start", default=None,
                        help="Start date YYYY-MM-DD (inclusive)")
    parser.add_argument("--end", default=None,
                        help="End date YYYY-MM-DD (inclusive)")
    parser.add_argument("--schema", default=DEFAULT_SCHEMA,
                        choices=("trades", "ohlcv-1m"))
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Symbols per Databento batch job (default: {DEFAULT_CHUNK_SIZE}). "
                             f"Larger values reduce API round-trips; smaller values give finer retry granularity.")
    parser.add_argument("--request-id",
                        help="Override auto-generated request_id")
    parser.add_argument("--retry-failed", action="store_true",
                        help="Retry all failed chunks from the job store")
    parser.add_argument("--status", action="store_true",
                        help="Print job status summary and exit")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print chunk plan and cost estimate; don't submit")
    parser.add_argument("--download-only", action="store_true",
                        help="Download and stage data only; do not invoke the q loader")
    parser.add_argument("--load-only", action="store_true",
                        help="Skip API calls; run q loader on existing manifests only")
    parser.add_argument("--metrics", action="store_true",
                        help="Print per-chunk timing metrics and exit")
    parser.add_argument("--failures", action="store_true",
                        help="Print detailed failure breakdown and exit")
    parser.add_argument("--gaps", action="store_true",
                        help="Print per-symbol gaps report for the HDB and exit "
                             "(requires --symbols, --start, --end)")
    parser.add_argument("--workers", type=int, default=12,
                        help="Maximum parallel chunk workers (default: 12)")
    parser.add_argument("--stype-in", default="raw_symbol",
                        help="Databento symbol type for submit/cost calls "
                             "(default: raw_symbol)")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None) -> None:
    args = parse_args(argv)

    skip_load = args.download_only

    # ---- Status mode: no API key needed ----
    if args.status:
        print_status(STAGING_DIR, request_id=args.request_id)
        return

    # ---- Metrics mode: no API key needed ----
    if args.metrics:
        print_metrics(STAGING_DIR, request_id=args.request_id)
        return

    # ---- Failures mode: no API key needed ----
    if args.failures:
        print_failures(STAGING_DIR, request_id=args.request_id)
        return

    # ---- Gaps mode: no API key needed, but requires symbols/start/end ----
    if args.gaps:
        if not args.symbols or not args.start or not args.end:
            log.error("--gaps requires --symbols, --start, and --end")
            sys.exit(1)
        symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
        hdb_dir = Path(os.environ.get("KDBHDB", str(PACKAGE_HOME / "hdb")))
        print_gaps(hdb_dir, symbols, date.fromisoformat(args.start),
                   date.fromisoformat(args.end), args.schema, args.dataset)
        return

    # ---- Load-only mode: run q loader on whatever manifests already exist ----
    if args.load_only:
        run_id = (args.request_id
                  or f"load_only_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
        _add_file_logging(PACKAGE_HOME / "logs", run_id)
        manifest_dir = STAGING_DIR / "metadata" / "manifests"
        log.info("load-only mode: running q loader on existing manifests")
        try:
            run_q_loader(PACKAGE_HOME, manifest_dir)
        except Exception as exc:
            log.exception(f"q loader failed: {exc}")
            sys.exit(1)
        return

    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        log.error("DATABENTO_API_KEY is not set.")
        sys.exit(1)

    job_store = JobStore(STAGING_DIR)
    manifest_dir = STAGING_DIR / "metadata" / "manifests"
    client = db.Historical(api_key)

    # ---- Retry mode: load failed chunks from job store ----
    if args.retry_failed:
        failed = job_store.load_failed()
        if not failed:
            log.info("No failed chunks to retry.")
            return
        retry_id = (args.request_id
                    or f"retry_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
        _add_file_logging(PACKAGE_HOME / "logs", retry_id)
        log.info(f"Retrying {len(failed)} failed chunk(s)")

        retry_chunks = []
        backoffs = {}
        for record in failed:
            wait = min(2 ** record.retries, 60)
            chunk = Chunk(
                request_id=record.request_id,
                chunk_id=record.chunk_id,
                dataset=record.dataset,
                schema=record.schema,
                symbols=record.symbols,
                date=date.fromisoformat(record.date),
            )
            record.status = "pending"
            record.error_msg = ""
            job_store.save(record)
            retry_chunks.append(chunk)
            backoffs[chunk.chunk_id] = wait

        _run_chunks_parallel(client, retry_chunks, job_store, STAGING_DIR,
                             manifest_dir,
                             max_workers=args.workers, backoffs=backoffs)
        job_store.flush()

        if not skip_load:
            try:
                run_q_loader(PACKAGE_HOME, manifest_dir)
            except Exception as exc:
                log.exception(f"q loader failed: {exc}")
                sys.exit(1)
            # ---- Update reference data for retried symbols/range ----
            retry_syms = sorted({s for c in retry_chunks for s in c.symbols})
            retry_start = min(c.date for c in retry_chunks)
            retry_end = max(c.date for c in retry_chunks)
            _run_ref_ingest(retry_syms, retry_start, retry_end)
            _cleanup_staging_csvs(job_store, STAGING_DIR)

        # ---- Terminal summary for retry run ----
        retry_ids = {c.request_id for c in retry_chunks}
        retry_records = [r for r in job_store.load_all() if r.request_id in retry_ids]
        _print_run_summary(retry_records, title=f"Retry Summary — {retry_id}")
        return

    # ---- Normal mode: require symbols/start/end ----
    if not args.symbols or not args.start or not args.end:
        log.error("--symbols, --start, --end are required "
                     "(or use --retry-failed / --status / --load-only)")
        sys.exit(1)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    request_id = (args.request_id
                  or f"req_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
                     f"_{uuid.uuid4().hex[:6]}")

    _add_file_logging(PACKAGE_HOME / "logs", request_id)

    # Detect a --request-id that collides with an existing run (different
    # symbols/date range → silent corruption; same range → idempotent is fine).
    if args.request_id:
        existing = [r for r in job_store.load_all()
                    if r.request_id == request_id]
        if existing:
            existing_dates = sorted({r.date for r in existing})
            existing_syms  = sorted({s for r in existing for s in r.symbols})
            new_dates = sorted({(start + timedelta(days=i)).isoformat()
                                for i in range((end - start).days + 1)})
            new_syms = sorted(symbols)
            if existing_dates != new_dates or existing_syms != new_syms:
                log.error(
                    f"request_id={request_id!r} already exists in the job store "
                    f"with different parameters. "
                    f"Existing: dates={existing_dates} syms={existing_syms}. "
                    f"Requested: dates={new_dates} syms={new_syms}. "
                    "Use a different --request-id or omit it to auto-generate one."
                )
                sys.exit(1)
            log.warning(
                f"request_id={request_id!r} already exists with matching "
                "parameters — treating as idempotent resume"
            )

    chunks = generate_chunks(request_id, symbols, start, end,
                             args.chunk_size, args.schema, args.dataset,
                             stype_in=args.stype_in)

    # ---- Pre-flight HDB check: skip dates already loaded for this exchange ----
    hdb_dir = Path(os.environ.get("KDBHDB", str(PACKAGE_HOME / "hdb")))
    all_dates = sorted({c.date for c in chunks})
    existing_dates = _hdb_already_loaded_dates(
        hdb_dir, all_dates, args.schema, args.dataset)
    if existing_dates:
        skipped_chunks = [c for c in chunks if c.date in existing_dates]
        chunks = [c for c in chunks if c.date not in existing_dates]
        log.warning(
            f"Pre-flight: {len(skipped_chunks)} chunk(s) already in HDB "
            f"(exchange={args.dataset} schema={args.schema}) — skipped before API submission. "
            f"Dates: {sorted(str(d) for d in existing_dates)}"
        )
        print(f"\nNOTE: {len(skipped_chunks)} chunk(s) already in HDB — skipping "
              f"(saves API cost):")
        for d in sorted(existing_dates):
            print(f"  {args.dataset}  {args.schema}  {d}  — already loaded")
        if not chunks:
            print("\nAll requested chunks already in HDB. Nothing to do.\n")
            log.info("Pre-flight: all chunks already present, exiting cleanly")
            return
        print()

    log.info(f"request_id={request_id} chunks={len(chunks)} "
                f"symbols={len(symbols)} dates={start}..{end}")

    # ---- Dry run: cost estimate and chunk plan ----
    if args.dry_run:
        total_cost = 0.0
        print(f"\nChunk plan for {request_id}:")
        print(f"  {'chunk_id':<50} {'symbols':>7} {'est_cost':>10}")
        print(f"  {'-'*70}")
        for chunk in chunks:
            end_date = (chunk.date + timedelta(days=1)).isoformat()
            cost = estimate_cost(client, chunk.dataset, chunk.symbols,
                                 chunk.schema,
                                 chunk.date.isoformat(), end_date)
            total_cost += cost
            print(f"  {chunk.chunk_id:<50} {len(chunk.symbols):>7} ${cost:>9.4f}")
        print(f"\n  Total: {len(chunks)} chunk(s), estimated ${total_cost:.4f}")
        print(f"  Limit:  ${MAX_COST_USD:.2f}\n")
        return

    # ---- Run all chunks (parallel) ----
    log.info(f"Running {len(chunks)} chunk(s) with up to {args.workers} worker(s)")
    _wall_start = time.time()
    succeeded, failed = _run_chunks_parallel(
        client, chunks, job_store, STAGING_DIR, manifest_dir,
        max_workers=args.workers,
    )
    _wall_s = time.time() - _wall_start
    job_store.flush()

    log.info(f"Chunks complete: {succeeded} succeeded, {failed} failed")

    if failed:
        log.warning(
            f"{failed} chunk(s) failed. Run with --retry-failed to requeue, "
            f"or check staging/metadata/backfill_jobs for details."
        )

    # ---- Invoke q loader for all downloaded manifests ----
    if not skip_load and succeeded > 0:
        try:
            run_q_loader(PACKAGE_HOME, manifest_dir, request_id=request_id)
        except Exception as exc:
            log.exception(f"q loader failed: {exc}")
            sys.exit(1)
        # ---- Update reference data (corp actions + adj factors) ----
        _run_ref_ingest(symbols, start, end)
        # ---- Clean up staging CSVs for verified chunks ----
        _cleanup_staging_csvs(job_store, STAGING_DIR, request_id=request_id)

    # Emit metrics summary for this request
    summary_path = write_summary(STAGING_DIR, request_id, wall_s=_wall_s)
    if summary_path:
        log.info(f"Metrics summary: {summary_path}")

    # ---- Terminal summary ----
    run_records = [r for r in job_store.load_all() if r.request_id == request_id]
    _print_run_summary(run_records, title=f"Backfill Summary — {request_id}")

    if failed > 0:
        sys.exit(1)

    log.info(f"Backfill complete: request_id={request_id}")


if __name__ == "__main__":
    main()
