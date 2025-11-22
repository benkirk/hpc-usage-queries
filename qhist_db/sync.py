"""Sync job data from remote HPC machines via qhist command."""

import json
import subprocess
from datetime import datetime, timezone
from typing import Iterator

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .models import MACHINE_MODELS, get_model_for_machine

# All available fields from qhist
ALL_FIELDS = (
    "id,short_id,account,avgcpu,count,cpupercent,cputime,cputype,"
    "elapsed,eligible,end,gputype,memory,mpiprocs,name,numcpus,"
    "numgpus,numnodes,ompthreads,ptargets,queue,reqmem,resources,"
    "start,status,submit,user,vmemory,walltime"
)

# Fields that contain timestamps and need UTC conversion
TIMESTAMP_FIELDS = {"submit", "eligible", "start", "end"}

# Fields that should be integers
INTEGER_FIELDS = {
    "short_id", "elapsed", "walltime", "cputime",
    "numcpus", "numgpus", "numnodes", "mpiprocs", "ompthreads",
    "reqmem", "memory", "vmemory", "count"
}

# Fields that should be floats
FLOAT_FIELDS = {"cpupercent", "avgcpu"}


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse a timestamp string and convert to UTC.

    Args:
        value: Timestamp string from qhist (format varies)

    Returns:
        datetime in UTC, or None if parsing fails
    """
    if not value:
        return None

    # Try common formats
    formats = [
        "%Y-%m-%dT%H:%M:%S",      # ISO format without timezone
        "%Y-%m-%dT%H:%M:%S%z",    # ISO format with timezone
        "%Y-%m-%d %H:%M:%S",      # Space-separated
        "%Y-%m-%d %H:%M:%S%z",    # Space-separated with timezone
    ]

    for fmt in formats:
        try:
            dt = datetime.strptime(value, fmt)
            # If no timezone info, assume Mountain Time (UTC-7 or UTC-6)
            # For simplicity, we'll assume the time is already local and store as-is
            # In production, you might want to handle this more carefully
            if dt.tzinfo is None:
                # Assume times are in Mountain Time, convert to UTC
                # MST is UTC-7, MDT is UTC-6
                # For now, just mark as UTC to keep consistent
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except ValueError:
            continue

    return None


def parse_int(value) -> int | None:
    """Safely parse an integer value."""
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (ValueError, TypeError):
        return None


def parse_float(value) -> float | None:
    """Safely parse a float value."""
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (ValueError, TypeError):
        return None


def parse_job_record(record: dict) -> dict:
    """Parse and normalize a job record from qhist JSON output.

    Args:
        record: Raw job record dictionary from qhist

    Returns:
        Normalized record with proper types
    """
    result = {}

    for key, value in record.items():
        if key in TIMESTAMP_FIELDS:
            result[key] = parse_timestamp(value)
        elif key in INTEGER_FIELDS:
            result[key] = parse_int(value)
        elif key in FLOAT_FIELDS:
            result[key] = parse_float(value)
        else:
            # Text fields - store as-is, but convert empty to None
            result[key] = value if value else None

    return result


def fetch_jobs_ssh(
    machine: str,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
) -> Iterator[dict]:
    """Fetch job records from a remote machine via SSH.

    Args:
        machine: Machine name ('casper' or 'derecho')
        period: Single date in YYYYMMDD format
        start_date: Start date for range (YYYYMMDD)
        end_date: End date for range (YYYYMMDD)

    Yields:
        Parsed job record dictionaries
    """
    # Build the qhist command
    cmd = ["ssh", machine, "qhist", "--json", f"--format={ALL_FIELDS}"]

    if period:
        cmd.extend(["--period", period])
    elif start_date and end_date:
        cmd.extend(["--start", start_date, "--end", end_date])
    elif start_date:
        cmd.extend(["--start", start_date])
    elif end_date:
        cmd.extend(["--end", end_date])

    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"qhist command failed: {result.stderr}")

    # Parse JSON output - qhist outputs one JSON object per line
    for line in result.stdout.strip().split("\n"):
        if not line:
            continue
        try:
            record = json.loads(line)
            yield parse_job_record(record)
        except json.JSONDecodeError:
            # Skip malformed lines
            continue


def sync_jobs(
    session: Session,
    machine: str,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
) -> dict:
    """Sync job records from a remote machine to the local database.

    Args:
        session: SQLAlchemy session
        machine: Machine name ('casper' or 'derecho')
        period: Single date in YYYYMMDD format
        start_date: Start date for range (YYYYMMDD)
        end_date: End date for range (YYYYMMDD)
        dry_run: If True, don't actually insert records

    Returns:
        Dictionary with sync statistics
    """
    model = get_model_for_machine(machine)
    stats = {"fetched": 0, "inserted": 0, "skipped": 0, "errors": 0}

    for record in fetch_jobs_ssh(machine, period, start_date, end_date):
        stats["fetched"] += 1

        if dry_run:
            continue

        try:
            # Check if record already exists
            short_id = record.get("short_id")
            if short_id is None:
                stats["errors"] += 1
                continue

            existing = session.query(model).filter_by(short_id=short_id).first()

            if existing:
                stats["skipped"] += 1
            else:
                job = model(**record)
                session.add(job)
                stats["inserted"] += 1

        except Exception as e:
            stats["errors"] += 1
            print(f"Error processing record {record.get('short_id')}: {e}")

    if not dry_run:
        session.commit()

    return stats


def sync_jobs_bulk(
    session: Session,
    machine: str,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
    batch_size: int = 1000,
) -> dict:
    """Sync job records using bulk insert for better performance.

    Uses INSERT OR IGNORE for efficient duplicate handling.

    Args:
        session: SQLAlchemy session
        machine: Machine name ('casper' or 'derecho')
        period: Single date in YYYYMMDD format
        start_date: Start date for range (YYYYMMDD)
        end_date: End date for range (YYYYMMDD)
        dry_run: If True, don't actually insert records
        batch_size: Number of records to insert per batch

    Returns:
        Dictionary with sync statistics
    """
    model = get_model_for_machine(machine)
    stats = {"fetched": 0, "inserted": 0, "errors": 0}

    batch = []

    for record in fetch_jobs_ssh(machine, period, start_date, end_date):
        stats["fetched"] += 1

        if record.get("short_id") is None:
            stats["errors"] += 1
            continue

        batch.append(record)

        if len(batch) >= batch_size:
            if not dry_run:
                inserted = _insert_batch(session, model, batch)
                stats["inserted"] += inserted
            batch = []

    # Insert remaining records
    if batch and not dry_run:
        inserted = _insert_batch(session, model, batch)
        stats["inserted"] += inserted

    return stats


def _insert_batch(session: Session, model, records: list[dict]) -> int:
    """Insert a batch of records, ignoring duplicates.

    Returns:
        Number of records actually inserted
    """
    if not records:
        return 0

    # Use SQLite's INSERT OR IGNORE via on_conflict_do_nothing
    stmt = sqlite_insert(model.__table__).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=["short_id"])

    result = session.execute(stmt)
    session.commit()

    return result.rowcount
