"""
orchestrator.py — Databento batch backfill orchestrator.

Responsibilities (Python side of the Python↔q split):
  - Chunking: one Databento job per (day × symbol-batch). Each chunk is
    independent so a single failure doesn't block everything else.
  - Job store: every state transition is written to
    staging/metadata/jobs/<chunk_id>.json so a crashed run can resume.
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
import fcntl
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
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

_LOG_FMT = logging.Formatter(
    '{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}',
    datefmt="%Y-%m-%dT%H:%M:%S",
)


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


def _j(msg: str) -> str:
    return json.dumps(msg)


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_DATASET = "XNAS.ITCH"
DEFAULT_SCHEMA = "trades"
SUPPORTED_SCHEMAS = ("trades", "ohlcv-1m")
DEFAULT_CHUNK_SIZE = 10
MAX_COST_USD = float(os.environ.get("BACKFILL_MAX_COST_USD", "50.0"))
MAX_RETRIES = int(os.environ.get("BACKFILL_MAX_RETRIES", "3"))

_HERE = Path(__file__).resolve().parent
STAGING_DIR = Path(os.environ.get("STAGING_DIR", str(_HERE / "../../staging")))
PACKAGE_HOME = Path(os.environ.get("PACKAGEHOME", str(_HERE / "../..")))

POLL_INTERVAL_S = 10
POLL_TIMEOUT_S = 3600

# Job statuses in lifecycle order
TERMINAL_STATUSES = {"loaded", "verified"}
SKIP_STATUSES = TERMINAL_STATUSES  # chunks in these states are not re-submitted


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
    Persistent state for one Chunk. Stored as JSON in staging/metadata/jobs/.
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
# Job store — one JSON file per chunk in staging/metadata/jobs/
# ---------------------------------------------------------------------------

class JobStore:
    """
    Lightweight file-based job store.
    Each chunk_id maps to a JSON file: <jobs_dir>/<chunk_id>.json
    Writing is atomic-ish: we write to a temp file then rename.
    """

    def __init__(self, staging_dir: Path):
        self.jobs_dir = staging_dir / "metadata" / "jobs"
        self.jobs_dir.mkdir(parents=True, exist_ok=True)

    def _path(self, chunk_id: str) -> Path:
        return self.jobs_dir / f"{chunk_id}.json"

    def save(self, record: JobRecord) -> None:
        record.touch()
        tmp = self._path(record.chunk_id).with_suffix(".tmp")
        with open(tmp, "w") as f:
            json.dump(record.to_dict(), f, indent=2)
        tmp.rename(self._path(record.chunk_id))

    def load(self, chunk_id: str) -> JobRecord | None:
        p = self._path(chunk_id)
        if not p.exists():
            return None
        with open(p) as f:
            return JobRecord.from_dict(json.load(f))

    def load_all(self) -> list[JobRecord]:
        records = []
        for p in sorted(self.jobs_dir.glob("*.json")):
            try:
                with open(p) as f:
                    records.append(JobRecord.from_dict(json.load(f)))
            except Exception as exc:
                log.warning(_j(f"Could not read job record {p.name}: {exc}"))
        return records

    def load_failed(self) -> list[JobRecord]:
        return [r for r in self.load_all()
                if r.status == "failed" and r.retries < MAX_RETRIES]


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def generate_chunks(request_id: str, symbols: list[str], start: date,
                    end: date, chunk_size: int, schema: str,
                    dataset: str, stype_in: str = "raw_symbol") -> list[Chunk]:
    """
    Split a backfill request into one-day × one-symbol × one-exchange chunks.

    For a 3-day request with 4 symbols this produces 3 × 4 = 12 chunks.
    Each chunk maps to exactly one Databento batch job, giving the finest
    possible retry granularity (a single symbol/day failure doesn't affect
    any other symbol or day).

    The chunk_size parameter is no longer used for batching but is kept in
    the signature so existing CLI calls (--chunk-size) don't break.
    """
    chunks = []
    d = start
    while d <= end:
        date_str = d.strftime("%Y.%m.%d")
        for sym in symbols:
            safe_sym = re.sub(r"[^A-Za-z0-9]", "_", sym)
            chunk_id = f"{request_id}_{date_str}_{safe_sym}"
            chunks.append(Chunk(
                request_id=request_id,
                chunk_id=chunk_id,
                dataset=dataset,
                schema=schema,
                symbols=[sym],
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
    log.info(_j(f"Submitting: dataset={dataset} schema={schema} "
                f"symbols={symbols} start={start} end={end} stype_in={stype_in}"))
    # Request CSV directly so no local DBN-to-CSV conversion is needed.
    # pretty_px/pretty_ts give human-readable prices and ISO timestamps,
    # which is exactly what loader.q expects. map_symbols=True adds the
    # symbol column to the output.
    job = client.batch.submit_job(
        dataset=dataset, symbols=symbols, schema=schema,
        start=start, end=end,
        encoding="csv", compression=None,
        pretty_px=True, pretty_ts=True, map_symbols=True,
        stype_in=stype_in,
    )
    log.info(_j(f"Submitted job_id={job['id']} state={job.get('state')}"))
    return job


def poll_until_done(client: db.Historical, job_id: str) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_S
    while True:
        # Check deadline before making the API call so a hung/slow API response
        # cannot cause us to loop past the timeout indefinitely.
        if time.monotonic() > deadline:
            raise RuntimeError(f"Timed out waiting for job {job_id}")
        # NOTE: The Databento batch SDK does not expose a single-job lookup
        # endpoint, so we must list ALL jobs in the given states and then
        # filter by job_id.  This is O(total jobs in your account) — for
        # accounts with many historical jobs the response can be large.
        # If this becomes a bottleneck, consider caching the full job list
        # across concurrent poll_until_done calls for the same request.
        #
        # The SDK's JobState enum only recognises 'queued', 'processing',
        # 'done', 'expired' — passing 'failed' causes a validation error.
        # We catch that case below and surface it as a clear RuntimeError.
        try:
            jobs = client.batch.list_jobs(
                states=["queued", "processing", "done"]
            )
        except Exception as sdk_exc:
            if "failed" in str(sdk_exc).lower() or "jobstate" in str(sdk_exc).lower():
                raise RuntimeError(
                    f"Job {job_id} failed at Databento: {sdk_exc}"
                ) from sdk_exc
            raise
        match = next((j for j in jobs if j["id"] == job_id), None)
        if match is None:
            expired = client.batch.list_jobs(states=["expired"])
            match = next((j for j in expired if j["id"] == job_id), None)
            if match:
                raise RuntimeError(f"Job {job_id} expired")
            raise RuntimeError(f"Job {job_id} not found")
        state = match.get("state", "")
        log.info(_j(f"Polling job_id={job_id} state={state}"))
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

    Uses wc -l which reads only newline bytes, avoiding a full Python-level
    line-by-line scan that would re-read the entire file after download.
    """
    result = subprocess.run(["wc", "-l", str(path)],
                            capture_output=True, text=True, check=True)
    return int(result.stdout.split()[0]) - 1  # subtract header line


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

    log.info(_j(f"Downloading job_id={job_id}"))
    downloaded = client.batch.download(job_id=job_id, output_dir=str(chunk_dir))
    log.info(_j(f"Downloaded {len(downloaded)} file(s)"))

    csv_paths = [Path(p) for p in downloaded if Path(p).suffix == ".csv"]
    for p in csv_paths:
        log.info(_j(f"CSV: {p.name} ({p.stat().st_size} bytes)"))
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
    log.info(_j(f"Manifest: {manifest_path.name} rows={row_count}"))
    return manifest


# ---------------------------------------------------------------------------
# q loader invocation
# ---------------------------------------------------------------------------

def _acquire_lock_with_timeout(fh, timeout_s: int = 300) -> None:
    """Acquire an exclusive flock with a timeout.

    Uses LOCK_NB (non-blocking) with a retry loop so a hung or crashed process
    that holds the lock does not cause this process to block indefinitely.
    Raises RuntimeError if the lock cannot be acquired within timeout_s seconds.
    """
    deadline = time.monotonic() + timeout_s
    while True:
        try:
            fcntl.flock(fh, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return
        except BlockingIOError:
            if time.monotonic() >= deadline:
                raise RuntimeError(
                    f"Could not acquire q loader lock within {timeout_s}s. "
                    "Another process may be holding it, or a previous run crashed "
                    f"while holding the lock. Delete the stale lock file to recover: "
                    f"{fh.name}"
                )
            time.sleep(5)


def run_q_loader(package_home: Path, manifest_dir: Path) -> None:
    hdb_dir = os.environ.get("KDBHDB", str(package_home / "hdb"))
    loader_script = package_home / "code" / "backfill" / "loader.q"
    torq_home = os.environ.get("TORQHOME", str(package_home / "../TorQ"))

    if not loader_script.exists():
        raise FileNotFoundError(f"Loader script not found: {loader_script}")

    # Acquire an exclusive process-level lock before invoking the q loader.
    # This prevents concurrent orchestrator processes (e.g. parallel chunk runs
    # or a manual re-run) from calling .Q.dpft on the same partition simultaneously.
    lock_path = Path(hdb_dir).parent / ".q_loader.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_fh = open(lock_path, "w")
    try:
        _acquire_lock_with_timeout(lock_fh, timeout_s=300)
        _run_q_loader_locked(package_home, manifest_dir, hdb_dir, torq_home)
    finally:
        fcntl.flock(lock_fh, fcntl.LOCK_UN)
        lock_fh.close()


def _run_q_loader_locked(package_home: Path, manifest_dir: Path,
                         hdb_dir: str, torq_home: str) -> None:
    # Pipe q commands via stdin so we can load the script then call runLoader[].
    # cwd=package_home ensures relative \l paths inside loader.q resolve correctly.
    q_script = "\\l code/backfill/loader.q\nrunLoader[]\nexit 0\n"
    cmd = ["q", "-q"]
    env = {**os.environ,
           "STAGING_DIR": str(manifest_dir.parent.parent),
           "KDBHDB": hdb_dir,
           "TORQHOME": torq_home,
           "TZ": "UTC"}

    log.info(_j(f"Invoking q loader: cwd={package_home}"))
    result = subprocess.run(
        cmd, input=q_script, text=True,
        cwd=str(package_home), env=env, capture_output=True,
    )
    for line in result.stdout.splitlines():
        log.info(_j(f"[q] {line}"))
    for line in result.stderr.splitlines():
        log.warning(_j(f"[q stderr] {line}"))
    if result.returncode != 0:
        raise RuntimeError(f"q loader exited {result.returncode}")
    log.info(_j("q loader done"))


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
            log.warning(_j(f"Chunk {chunk.chunk_id}: CSV not found on resume, "
                           f"skipping manifest for {part_path.name}"))
            continue
        part_chunk_id = (chunk.chunk_id if part_idx == 0
                         else f"{chunk.chunk_id}_part{part_idx + 1}")
        write_manifest(chunk.request_id, part_chunk_id,
                       record.databento_job_id, chunk.schema,
                       chunk.symbols, part_path, manifest_dir,
                       dataset=chunk.dataset)


def run_chunk(client: db.Historical, chunk: Chunk, job_store: JobStore,
              staging_dir: Path, manifest_dir: Path,
              skip_load: bool = False) -> bool:
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
    if record and record.status in SKIP_STATUSES:
        log.info(_j(f"Chunk {chunk.chunk_id} already {record.status}, skipping"))
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
                log.warning(_j(
                    f"Chunk {chunk.chunk_id}: checksum mismatch on resume "
                    f"(expected {record.checksum[:16]}...) — re-downloading"
                ))
                record.status = "pending"
                record.checksum = ""
                record.file_paths = []
                job_store.save(record)
                # Fall through to re-submit below
            else:
                log.info(_j(f"Chunk {chunk.chunk_id}: resume OK, skipping download"))
                _write_manifests_for_record(chunk, record, manifest_dir)
                record.status = "loaded"
                job_store.save(record)
                return True

    # ---- Resume from submitted/running? ----
    job_id = None
    if record and record.status in ("submitted", "running") and record.databento_job_id:
        log.info(_j(f"Chunk {chunk.chunk_id} resuming from {record.status}, "
                    f"polling job {record.databento_job_id}"))
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
        log.info(_j(f"Job {job_id} done, cost={job.get('cost')}"))

        # ---- Download ----
        metrics.mark("download_start")
        chunk_dir = staging_dir / chunk.chunk_id
        csv_paths = download_csv(client, job_id, chunk_dir)
        if not csv_paths:
            raise RuntimeError("No CSV files produced after download")

        if len(csv_paths) > 1:
            log.warning(_j(
                f"Job {job_id} delivered {len(csv_paths)} CSVs; "
                "writing one manifest per file"
            ))

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
        log.error(_j(f"Chunk {chunk.chunk_id} failed [{ft}] (attempt {record.retries}): {exc}"))
        return False


# ---------------------------------------------------------------------------
# Parallel chunk runner
# ---------------------------------------------------------------------------

def _run_chunks_parallel(client: db.Historical, chunks: list[Chunk],
                         job_store: JobStore, staging_dir: Path,
                         manifest_dir: Path, skip_load: bool,
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
            log.info(_j(f"Chunk {chunk.chunk_id}: waiting {wait}s before retry"))
            time.sleep(wait)
        return run_chunk(client, chunk, job_store, staging_dir, manifest_dir,
                         skip_load=skip_load)

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
                log.error(_j(f"Chunk {chunk.chunk_id} raised unexpected exception: {exc}"))
                ok = False
            done += 1
            if ok:
                succeeded += 1
            else:
                failed += 1
            status = "ok" if ok else "FAIL"
            sym = chunk.symbols[0] if chunk.symbols else chunk.chunk_id
            print(f"\r  [{done}/{total}] {sym} {chunk.date} — {status}    ",
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
    from collections import defaultdict

    verified = [r for r in records if r.status == "verified"]
    failed   = [r for r in records if r.status == "failed"]

    print(f"\n{'='*60}")
    print(f"  {title}")
    print(f"{'='*60}")

    if verified:
        # Group by dataset (exchange) → collect unique syms and total rows
        by_exchange: dict[str, dict] = defaultdict(lambda: {"syms": set(), "rows": 0})
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
        from collections import Counter
        reasons = Counter(
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
    from collections import defaultdict, Counter
    by_req: dict[str, list[JobRecord]] = defaultdict(list)
    for r in records:
        by_req[r.request_id].append(r)

    print(f"\n{'request_id':<40} {'schema':<10} {'chunks':>6} "
          f"{'submitted':>9} {'running':>7} {'downloaded':>10} "
          f"{'loaded':>6} {'failed':>6}")
    print("-" * 100)

    for req_id, recs in sorted(by_req.items()):
        counts = Counter(r.status for r in recs)
        schema = recs[0].schema if recs else ""
        print(f"{req_id:<40} {schema:<10} {len(recs):>6} "
              f"{counts.get('submitted', 0):>9} "
              f"{counts.get('running', 0):>7} "
              f"{counts.get('downloaded', 0):>10} "
              f"{counts.get('loaded', 0):>6} "
              f"{counts.get('failed', 0):>6}")

        # Print failed chunks with their error messages
        for r in recs:
            if r.status == "failed":
                ft = f" [{r.failure_type}]" if r.failure_type else ""
                print(f"  FAILED {r.chunk_id}{ft}: {r.error_msg[:80]}")

    print()


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def parse_args(argv=None):
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
                        choices=SUPPORTED_SCHEMAS)
    parser.add_argument("--dataset", default=DEFAULT_DATASET)
    parser.add_argument("--chunk-size", type=int, default=DEFAULT_CHUNK_SIZE,
                        help=f"Symbols per batch job (default: {DEFAULT_CHUNK_SIZE})")
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
    parser.add_argument("--workers", type=int, default=12,
                        help="Maximum parallel chunk workers (default: 12)")
    parser.add_argument("--stype-in", default="raw_symbol",
                        help="Databento symbol type for submit/cost calls "
                             "(default: raw_symbol)")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
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

    # ---- Load-only mode: run q loader on whatever manifests already exist ----
    if args.load_only:
        run_id = (args.request_id
                  or f"load_only_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
        _add_file_logging(PACKAGE_HOME / "logs", run_id)
        manifest_dir = STAGING_DIR / "metadata" / "manifests"
        log.info(_j("load-only mode: running q loader on existing manifests"))
        try:
            run_q_loader(PACKAGE_HOME, manifest_dir)
        except Exception as exc:
            log.error(_j(f"q loader failed: {exc}"))
            sys.exit(1)
        return

    api_key = os.environ.get("DATABENTO_API_KEY")
    if not api_key:
        log.error(_j("DATABENTO_API_KEY is not set."))
        sys.exit(1)

    job_store = JobStore(STAGING_DIR)
    manifest_dir = STAGING_DIR / "metadata" / "manifests"
    client = db.Historical(api_key)

    # ---- Retry mode: load failed chunks from job store ----
    if args.retry_failed:
        failed = job_store.load_failed()
        if not failed:
            log.info(_j("No failed chunks to retry."))
            return
        retry_id = (args.request_id
                    or f"retry_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}")
        _add_file_logging(PACKAGE_HOME / "logs", retry_id)
        log.info(_j(f"Retrying {len(failed)} failed chunk(s)"))

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
                             manifest_dir, skip_load=skip_load,
                             max_workers=args.workers, backoffs=backoffs)

        if not skip_load:
            try:
                run_q_loader(PACKAGE_HOME, manifest_dir)
            except Exception as exc:
                log.error(_j(f"q loader failed: {exc}"))
                sys.exit(1)

        # ---- Terminal summary for retry run ----
        retry_ids = {c.request_id for c in retry_chunks}
        retry_records = [r for r in job_store.load_all() if r.request_id in retry_ids]
        _print_run_summary(retry_records, title=f"Retry Summary — {retry_id}")
        return

    # ---- Normal mode: require symbols/start/end ----
    if not args.symbols or not args.start or not args.end:
        log.error(_j("--symbols, --start, --end are required "
                     "(or use --retry-failed / --status / --load-only)"))
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
                log.error(_j(
                    f"request_id={request_id!r} already exists in the job store "
                    f"with different parameters. "
                    f"Existing: dates={existing_dates} syms={existing_syms}. "
                    f"Requested: dates={new_dates} syms={new_syms}. "
                    "Use a different --request-id or omit it to auto-generate one."
                ))
                sys.exit(1)
            log.warning(_j(
                f"request_id={request_id!r} already exists with matching "
                "parameters — treating as idempotent resume"
            ))

    chunks = generate_chunks(request_id, symbols, start, end,
                             args.chunk_size, args.schema, args.dataset,
                             stype_in=args.stype_in)

    log.info(_j(f"request_id={request_id} chunks={len(chunks)} "
                f"symbols={len(symbols)} dates={start}..{end}"))

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
    log.info(_j(f"Running {len(chunks)} chunk(s) with up to {args.workers} worker(s)"))
    _wall_start = time.time()
    succeeded, failed = _run_chunks_parallel(
        client, chunks, job_store, STAGING_DIR, manifest_dir,
        skip_load=skip_load, max_workers=args.workers,
    )
    _wall_s = time.time() - _wall_start

    log.info(_j(f"Chunks complete: {succeeded} succeeded, {failed} failed"))

    if failed:
        log.warning(_j(
            f"{failed} chunk(s) failed. Run with --retry-failed to requeue, "
            f"or check staging/metadata/jobs/ for details."
        ))

    # ---- Invoke q loader for all downloaded manifests ----
    if not skip_load and succeeded > 0:
        try:
            run_q_loader(PACKAGE_HOME, manifest_dir)
        except Exception as exc:
            log.error(_j(f"q loader failed: {exc}"))
            sys.exit(1)

    # Emit metrics summary for this request
    summary_path = write_summary(STAGING_DIR, request_id, wall_s=_wall_s)
    if summary_path:
        log.info(_j(f"Metrics summary: {summary_path}"))

    # ---- Terminal summary ----
    run_records = [r for r in job_store.load_all() if r.request_id == request_id]
    _print_run_summary(run_records, title=f"Backfill Summary — {request_id}")

    if failed > 0:
        sys.exit(1)

    log.info(_j(f"Backfill complete: request_id={request_id}"))


if __name__ == "__main__":
    main()
