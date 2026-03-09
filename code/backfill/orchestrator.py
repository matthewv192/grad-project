"""
orchestrator.py — Databento batch backfill orchestrator.

Milestone 2 additions over M1:
  - Chunking: one Databento job per (day × symbol-batch). Each chunk is
    independent so a single failure doesn't block everything else.
  - Job store: every state transition is written to
    staging/metadata/jobs/<chunk_id>.json so a crashed run can resume.
  - Idempotency: before submitting a chunk we check its stored status and
    skip (or fast-forward) accordingly.
  - Retry: --retry-failed re-queues all failed chunks with retries < MAX_RETRIES,
    using exponential backoff (2^retries seconds).
  - Status: --status prints a human-readable summary of all job records.

Python <-> q boundary (unchanged from M1):
  Python owns: API calls, downloads, DBN→CSV, manifests, job store.
  q owns:      kdb+ writes, HDB partition management.
  They communicate via CSV + JSON files in staging/.
"""

import argparse
import hashlib
import json
import logging
import os
import re
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, field, asdict
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import databento as db

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format='{"time":"%(asctime)s","level":"%(levelname)s","logger":"%(name)s","msg":%(message)s}',
    datefmt="%Y-%m-%dT%H:%M:%S",
    stream=sys.stdout,
)
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
    schema: str = ""
    symbols: list = field(default_factory=list)
    date: str = ""          # ISO YYYY-MM-DD
    status: str = "pending"
    retries: int = 0
    error_msg: str = ""
    file_path: str = ""
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
        # Only pass fields that exist in the dataclass to handle older records
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
                    dataset: str) -> list[Chunk]:
    """
    Split a backfill request into one-day × one-symbol-batch chunks.

    For a 3-day request with 25 symbols and chunk_size=10, this produces
    3 × 3 = 9 chunks: each day gets batches [0:10], [10:20], [20:25].

    Keeping one day per chunk keeps the Databento job small and lets the
    retry logic target exactly the day+batch that failed.
    """
    chunks = []
    d = start
    while d <= end:
        date_str = d.strftime("%Y.%m.%d")
        for batch_idx in range(0, len(symbols), chunk_size):
            batch = symbols[batch_idx: batch_idx + chunk_size]
            batch_num = batch_idx // chunk_size
            chunk_id = f"{request_id}_{date_str}_b{batch_num:03d}"
            chunks.append(Chunk(
                request_id=request_id,
                chunk_id=chunk_id,
                dataset=dataset,
                schema=schema,
                symbols=batch,
                date=d,
            ))
        d += timedelta(days=1)
    return chunks


# ---------------------------------------------------------------------------
# Cost estimation
# ---------------------------------------------------------------------------

def estimate_cost(client: db.Historical, dataset: str, symbols: list[str],
                  schema: str, start: str, end: str) -> float:
    try:
        return float(client.metadata.get_cost(
            dataset=dataset, symbols=symbols, schema=schema,
            start=start, end=end, stype_in="raw_symbol",
        ))
    except Exception as exc:
        log.warning(_j(f"Cost estimation failed (will proceed): {exc}"))
        return 0.0


# ---------------------------------------------------------------------------
# Submit / poll / download / convert (unchanged from M1)
# ---------------------------------------------------------------------------

def submit_job(client: db.Historical, dataset: str, symbols: list[str],
               schema: str, start: str, end: str) -> dict:
    log.info(_j(f"Submitting: dataset={dataset} schema={schema} "
                f"symbols={symbols} start={start} end={end}"))
    # Request CSV directly so no local DBN-to-CSV conversion is needed.
    # pretty_px/pretty_ts give human-readable prices and ISO timestamps,
    # which is exactly what loader.q expects. map_symbols=True adds the
    # symbol column to the output.
    job = client.batch.submit_job(
        dataset=dataset, symbols=symbols, schema=schema,
        start=start, end=end,
        encoding="csv", compression=None,
        pretty_px=True, pretty_ts=True, map_symbols=True,
        stype_in="raw_symbol",
    )
    log.info(_j(f"Submitted job_id={job['id']} state={job.get('state')}"))
    return job


def poll_until_done(client: db.Historical, job_id: str) -> dict:
    deadline = time.monotonic() + POLL_TIMEOUT_S
    while True:
        jobs = client.batch.list_jobs(states=["queued", "processing", "done"])
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
        if time.monotonic() > deadline:
            raise RuntimeError(f"Timed out waiting for job {job_id}")
        time.sleep(POLL_INTERVAL_S)


def sha256_of_file(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return "sha256:" + h.hexdigest()


def count_csv_rows(path: Path) -> int:
    with open(path) as f:
        return sum(1 for _ in f) - 1  # exclude header


def infer_date_from_filename(name: str) -> str:
    """Extract YYYY-MM-DD from a Databento CSV filename.

    Handles both formats:
      - xnas-itch-20240603.trades.csv  (actual Databento delivery format)
      - DBNJ-XXXXX_20240603_trades.csv (legacy/documented format)
    """
    m = re.search(r"(\d{4})(\d{2})(\d{2})", name)
    if m:
        return f"{m.group(1)}-{m.group(2)}-{m.group(3)}"
    return ""


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
                   manifest_dir: Path) -> dict:
    manifest_dir.mkdir(parents=True, exist_ok=True)
    row_count = count_csv_rows(csv_path)
    checksum = sha256_of_file(csv_path)
    manifest = {
        "request_id": request_id,
        "chunk_id": chunk_id,
        "databento_job_id": job_id,
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
    with open(manifest_path, "w") as f:
        json.dump(manifest, f, indent=2)
    log.info(_j(f"Manifest: {manifest_path.name} rows={row_count}"))
    return manifest


# ---------------------------------------------------------------------------
# q loader invocation
# ---------------------------------------------------------------------------

def run_q_loader(package_home: Path, manifest_dir: Path) -> None:
    hdb_dir = os.environ.get("KDBHDB", str(package_home / "hdb"))
    loader_script = package_home / "code" / "backfill" / "loader.q"
    torq_home = os.environ.get("TORQHOME", str(package_home / "../TorQ"))

    if not loader_script.exists():
        log.error(_j(f"Loader script not found: {loader_script}"))
        return

    # Pipe q commands via stdin so we can load the script then call runLoader[].
    # cwd=package_home ensures relative \l paths inside loader.q resolve correctly.
    q_script = "\\l code/backfill/loader.q\nrunLoader[]\nexit 0\n"
    cmd = ["q", "-q"]
    env = {**os.environ,
           "STAGING_DIR": str(manifest_dir.parent.parent),
           "KDBHDB": hdb_dir,
           "TORQHOME": torq_home}

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

def run_chunk(client: db.Historical, chunk: Chunk, job_store: JobStore,
              staging_dir: Path, manifest_dir: Path,
              skip_load: bool = False) -> bool:
    """
    Execute the full pipeline for one chunk. Returns True on success.

    Idempotency: if the job store already shows this chunk as loaded/verified,
    we skip immediately. If it was previously downloaded, we skip straight to
    manifest writing. If it was submitted/running, we pick up from polling.
    """
    record = job_store.load(chunk.chunk_id)
    date_str = chunk.date.isoformat()
    # Databento uses exclusive end: to request a single day, end = day + 1
    end_str = (chunk.date + timedelta(days=1)).isoformat()

    # ---- Already done? ----
    if record and record.status in SKIP_STATUSES:
        log.info(_j(f"Chunk {chunk.chunk_id} already {record.status}, skipping"))
        return True

    # ---- Resume from downloaded? ----
    if record and record.status == "downloaded" and record.file_path:
        log.info(_j(f"Chunk {chunk.chunk_id} already downloaded, skipping to manifest"))
        csv_path = Path(record.file_path)
        if csv_path.exists():
            write_manifest(chunk.request_id, chunk.chunk_id,
                           record.databento_job_id, chunk.schema,
                           chunk.symbols, csv_path, manifest_dir)
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
                                 chunk.schema, date_str, end_str)
            if MAX_COST_USD > 0 and cost > MAX_COST_USD:
                raise RuntimeError(
                    f"Cost ${cost:.4f} exceeds limit ${MAX_COST_USD:.2f}"
                )

            record.status = "submitted"
            job_store.save(record)

            job = submit_job(client, chunk.dataset, chunk.symbols,
                             chunk.schema, date_str, end_str)
            job_id = job["id"]
            record.databento_job_id = job_id
            record.status = "running"
            job_store.save(record)

        # ---- Poll ----
        job = poll_until_done(client, job_id)
        log.info(_j(f"Job {job_id} done, cost={job.get('cost')}"))

        # ---- Download ----
        chunk_dir = staging_dir / chunk.chunk_id
        csv_paths = download_csv(client, job_id, chunk_dir)
        if not csv_paths:
            raise RuntimeError("No CSV files produced after download")

        # One Databento job per day → typically one CSV; take the first
        csv_path = csv_paths[0]
        record.file_path = str(csv_path.resolve())
        record.checksum = sha256_of_file(csv_path)
        record.row_count = count_csv_rows(csv_path)
        record.status = "downloaded"
        job_store.save(record)

        # ---- Write manifest ----
        write_manifest(chunk.request_id, chunk.chunk_id, job_id,
                       chunk.schema, chunk.symbols, csv_path, manifest_dir)

        record.status = "loaded"
        job_store.save(record)
        return True

    except Exception as exc:
        record.retries += 1
        record.status = "failed"
        record.error_msg = str(exc)
        job_store.save(record)
        log.error(_j(f"Chunk {chunk.chunk_id} failed (attempt {record.retries}): {exc}"))
        return False


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
                print(f"  FAILED {r.chunk_id}: {r.error_msg[:80]}")

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
    parser.add_argument("--skip-load", action="store_true",
                        help="Skip invoking the q loader after download")
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main(argv=None):
    args = parse_args(argv)

    # ---- Status mode: no API key needed ----
    if args.status:
        print_status(STAGING_DIR, request_id=args.request_id)
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
        log.info(_j(f"Retrying {len(failed)} failed chunk(s)"))

        for record in failed:
            # Exponential backoff before resubmitting
            wait = 2 ** record.retries
            log.info(_j(f"Waiting {wait}s before retrying {record.chunk_id}"))
            time.sleep(wait)

            # Reconstruct chunk from stored record
            chunk = Chunk(
                request_id=record.request_id,
                chunk_id=record.chunk_id,
                dataset=DEFAULT_DATASET,  # not stored in record; use default
                schema=record.schema,
                symbols=record.symbols,
                date=date.fromisoformat(record.date),
            )
            # Reset status so run_chunk will resubmit
            record.status = "pending"
            record.error_msg = ""
            job_store.save(record)

            run_chunk(client, chunk, job_store, STAGING_DIR, manifest_dir,
                      skip_load=args.skip_load)

        if not args.skip_load:
            try:
                run_q_loader(PACKAGE_HOME, manifest_dir)
            except Exception as exc:
                log.error(_j(f"q loader failed: {exc}"))
                sys.exit(1)
        return

    # ---- Normal mode: require symbols/start/end ----
    if not args.symbols or not args.start or not args.end:
        log.error(_j("--symbols, --start, --end are required (or use --retry-failed / --status)"))
        sys.exit(1)

    symbols = [s.strip().upper() for s in args.symbols.split(",") if s.strip()]
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)

    request_id = (args.request_id
                  or f"req_{datetime.now(timezone.utc).strftime('%Y%m%d_%H%M%S')}"
                     f"_{uuid.uuid4().hex[:6]}")

    chunks = generate_chunks(request_id, symbols, start, end,
                             args.chunk_size, args.schema, args.dataset)

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

    # ---- Run all chunks ----
    succeeded = 0
    failed = 0
    for chunk in chunks:
        ok = run_chunk(client, chunk, job_store, STAGING_DIR, manifest_dir,
                       skip_load=args.skip_load)
        if ok:
            succeeded += 1
        else:
            failed += 1

    log.info(_j(f"Chunks complete: {succeeded} succeeded, {failed} failed"))

    if failed:
        log.warning(_j(
            f"{failed} chunk(s) failed. Run with --retry-failed to requeue, "
            f"or check staging/metadata/jobs/ for details."
        ))

    # ---- Invoke q loader for all downloaded manifests ----
    if not args.skip_load and succeeded > 0:
        try:
            run_q_loader(PACKAGE_HOME, manifest_dir)
        except Exception as exc:
            log.error(_j(f"q loader failed: {exc}"))
            sys.exit(1)

    if failed > 0:
        sys.exit(1)

    log.info(_j(f"Backfill complete: request_id={request_id}"))


if __name__ == "__main__":
    main()
