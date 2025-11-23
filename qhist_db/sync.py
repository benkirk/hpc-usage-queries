"""Sync job data from remote HPC machines via qhist command."""

import json
import subprocess
from datetime import datetime, timedelta, timezone
from typing import Iterator

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

from .models import Job

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


def date_range(start_date: str, end_date: str) -> Iterator[str]:
    """Iterate through dates from start to end (inclusive).

    Args:
        start_date: Start date in YYYYMMDD format
        end_date: End date in YYYYMMDD format

    Yields:
        Date strings in YYYYMMDD format
    """
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")

    current = start
    while current <= end:
        yield current.strftime("%Y%m%d")
        current += timedelta(days=1)


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


def parse_job_id(value) -> int | None:
    """Parse a job ID, handling array job format like '6049117[28]'.

    For array jobs, extracts the base job ID before the bracket.
    """
    if value is None or value == "":
        return None
    try:
        # Handle array job IDs like "6049117[28]"
        str_val = str(value)
        if "[" in str_val:
            str_val = str_val.split("[")[0]
        return int(str_val)
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


def parse_job_record(record: dict, full_job_id: str | None = None) -> dict:
    """Parse and normalize a job record from qhist JSON output.

    The qhist JSON has a nested structure that needs to be flattened
    to match our database schema.

    Args:
        record: Raw job record dictionary from qhist
        full_job_id: Full job ID from JSON key (e.g., "2712367.desched1")

    Returns:
        Normalized record with proper types matching database schema
    """
    resource_list = record.get("Resource_List", {})
    resources_used = record.get("resources_used", {})

    # Parse the select string for mpiprocs and ompthreads
    select_str = resource_list.get("select", "")
    mpiprocs = None
    ompthreads = None
    if select_str:
        for part in select_str.split(":"):
            if part.startswith("mpiprocs="):
                mpiprocs = parse_int(part.split("=")[1])
            elif part.startswith("ompthreads="):
                ompthreads = parse_int(part.split("=")[1])

    # Convert hours to seconds for time fields
    def hours_to_seconds(val):
        if val is None:
            return None
        try:
            return int(float(val) * 3600)
        except (ValueError, TypeError):
            return None

    # Convert GB to bytes for memory fields
    def gb_to_bytes(val):
        if val is None:
            return None
        try:
            return int(float(val) * 1024 * 1024 * 1024)
        except (ValueError, TypeError):
            return None

    # Get the short_id which may include array index like "6049117[28]"
    raw_short_id = record.get("short_id", "")

    # Use full_job_id from JSON key (e.g., "2712367.desched1")
    full_id = full_job_id if full_job_id else raw_short_id

    result = {
        # job_id is the scheduler's identifier (not globally unique across years)
        "job_id": full_id,
        # short_id is just the base job number for efficient queries
        "short_id": parse_job_id(raw_short_id),
        "name": record.get("jobname"),
        "user": record.get("user"),
        "account": record.get("account"),

        # Queue and status
        "queue": record.get("queue"),
        "status": record.get("Exit_status"),

        # Timestamps (ctime=submit, etime=eligible, start, end)
        "submit": parse_timestamp(record.get("ctime")),
        "eligible": parse_timestamp(record.get("etime")),
        "start": parse_timestamp(record.get("start")),
        "end": parse_timestamp(record.get("end")),

        # Time metrics (convert from hours to seconds)
        "elapsed": hours_to_seconds(resources_used.get("walltime")),
        "walltime": hours_to_seconds(resource_list.get("walltime")),
        "cputime": hours_to_seconds(resources_used.get("cput")),

        # Resource allocation
        "numcpus": parse_int(resource_list.get("ncpus")),
        "numgpus": parse_int(resource_list.get("ngpus")),
        "numnodes": parse_int(resource_list.get("nodect")),
        "mpiprocs": mpiprocs,
        "ompthreads": ompthreads,

        # Memory (convert from GB to bytes)
        "reqmem": gb_to_bytes(resource_list.get("mem")),
        "memory": gb_to_bytes(resources_used.get("mem")),
        "vmemory": gb_to_bytes(resources_used.get("vmem")),

        # Resource types (not available in JSON, set to None)
        "cputype": None,
        "gputype": None,
        "resources": resource_list.get("select"),
        "ptargets": resource_list.get("preempt_targets"),

        # Performance metrics
        "cpupercent": parse_float(resources_used.get("cpupercent")),
        "avgcpu": parse_float(resources_used.get("avgcpu")),
        "count": parse_int(record.get("run_count")),
    }

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
    # qhist uses -p/--period with format: YYYYMMDD for single day, YYYYMMDD-YYYYMMDD for range
    cmd = ["ssh", machine, "qhist", "-J", f"-f={ALL_FIELDS}"]

    if period:
        cmd.extend(["-p", period])
    elif start_date and end_date:
        cmd.extend(["-p", f"{start_date}-{end_date}"])
    elif start_date:
        # From start_date to today
        cmd.extend(["-p", f"{start_date}-"])
    elif end_date:
        # Up to end_date (use days parameter instead)
        cmd.extend(["-p", end_date])

    # Run the command
    result = subprocess.run(cmd, capture_output=True, text=True)

    if result.returncode != 0:
        raise RuntimeError(f"qhist command failed: {result.stderr}")

    # Parse JSON output - qhist outputs a single JSON object with nested Jobs
    # Structure: { "timestamp": ..., "Jobs": { "jobid": {...}, ... } }
    if not result.stdout.strip():
        return

    try:
        data = json.loads(result.stdout)
        jobs = data.get("Jobs", {})
        for job_id, job_data in jobs.items():
            yield parse_job_record(job_data, full_job_id=job_id)
    except json.JSONDecodeError as e:
        raise RuntimeError(f"Failed to parse qhist JSON output: {e}")


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
    stats = {"fetched": 0, "inserted": 0, "skipped": 0, "errors": 0}

    for record in fetch_jobs_ssh(machine, period, start_date, end_date):
        stats["fetched"] += 1

        if dry_run:
            continue

        try:
            # Check if record already exists (by job_id + submit time)
            job_id = record.get("job_id")
            submit = record.get("submit")
            if not job_id:
                stats["errors"] += 1
                continue

            existing = session.query(Job).filter_by(job_id=job_id, submit=submit).first()

            if existing:
                stats["skipped"] += 1
            else:
                job = Job(**record)
                session.add(job)
                stats["inserted"] += 1

        except Exception as e:
            stats["errors"] += 1
            print(f"Error processing record {record.get('job_id')}: {e}")

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
    verbose: bool = False,
    force: bool = False,
    generate_summary: bool = True,
) -> dict:
    """Sync job records using bulk insert for better performance.

    Uses INSERT OR IGNORE for efficient duplicate handling.
    When a date range is specified, queries one day at a time to avoid
    overwhelming the remote system with large result sets.

    Args:
        session: SQLAlchemy session
        machine: Machine name ('casper' or 'derecho')
        period: Single date in YYYYMMDD format
        start_date: Start date for range (YYYYMMDD)
        end_date: End date for range (YYYYMMDD)
        dry_run: If True, don't actually insert records
        batch_size: Number of records to insert per batch
        verbose: If True, print progress for each day
        force: If True, sync even if day has already been summarized
        generate_summary: If True, generate daily summary after syncing

    Returns:
        Dictionary with sync statistics
    """
    from .summary import get_summarized_dates, generate_daily_summary

    stats = {
        "fetched": 0, "inserted": 0, "errors": 0,
        "days_failed": 0, "failed_days": [],
        "days_skipped": 0, "skipped_days": [],
        "days_summarized": 0,
    }

    # Get already-summarized dates if smart skip is enabled
    summarized_dates = set()
    if not force and not dry_run:
        summarized_dates = get_summarized_dates(session)

    # If date range specified, loop one day at a time
    if start_date and end_date:
        for day in date_range(start_date, end_date):
            day_date = datetime.strptime(day, "%Y%m%d").date()

            # Smart skip: if already summarized, skip fetching
            if day_date in summarized_dates:
                if verbose:
                    print(f"  Skipping {day}... (already summarized)")
                stats["days_skipped"] += 1
                stats["skipped_days"].append(day)
                continue

            if verbose:
                print(f"  Fetching {day}...", end=" ", flush=True)
            day_stats = _sync_single_day(session, machine, day, dry_run, batch_size, verbose)
            stats["fetched"] += day_stats["fetched"]
            stats["inserted"] += day_stats["inserted"]
            stats["errors"] += day_stats["errors"]

            if day_stats.get("failed"):
                stats["days_failed"] += 1
                stats["failed_days"].append(day)
            else:
                if verbose:
                    print(f"{day_stats['fetched']:,} jobs, {day_stats['inserted']:,} new")

                # Generate summary for this day
                if generate_summary and not dry_run and day_stats["fetched"] > 0:
                    generate_daily_summary(session, machine, day_date, replace=True)
                    stats["days_summarized"] += 1
    else:
        # Single day or no date specified
        target_period = period or start_date or end_date
        if target_period:
            day_date = datetime.strptime(target_period, "%Y%m%d").date()

            # Smart skip for single day
            if day_date in summarized_dates:
                if verbose:
                    print(f"  Skipping {target_period}... (already summarized)")
                stats["days_skipped"] = 1
                stats["skipped_days"] = [target_period]
                return stats

        day_stats = _sync_single_day(session, machine, target_period, dry_run, batch_size, verbose)
        stats["fetched"] = day_stats["fetched"]
        stats["inserted"] = day_stats["inserted"]
        stats["errors"] = day_stats["errors"]

        if day_stats.get("failed"):
            stats["days_failed"] = 1
            stats["failed_days"] = [target_period]
        elif generate_summary and not dry_run and target_period and day_stats["fetched"] > 0:
            day_date = datetime.strptime(target_period, "%Y%m%d").date()
            generate_daily_summary(session, machine, day_date, replace=True)
            stats["days_summarized"] = 1

    return stats


def _sync_single_day(
    session: Session,
    machine: str,
    period: str | None,
    dry_run: bool,
    batch_size: int,
    verbose: bool = False,
) -> dict:
    """Sync jobs for a single day.

    Args:
        session: SQLAlchemy session
        machine: Machine name
        period: Date in YYYYMMDD format
        dry_run: If True, don't insert
        batch_size: Batch size for inserts
        verbose: If True, print warnings

    Returns:
        Dictionary with sync statistics for this day
    """
    stats = {"fetched": 0, "inserted": 0, "errors": 0, "failed": False, "error_msg": None}
    batch = []

    try:
        for record in fetch_jobs_ssh(machine, period=period):
            stats["fetched"] += 1

            if not record.get("job_id"):
                stats["errors"] += 1
                continue

            # watch for bad timestamps - very very rarely some of these have been 0 - the UNIX epoch
            if record.get("submit") <= record.get("eligible") <= record.get("start") <= record.get("end"):
                pass
            else:
                stats["errors"] += 1
                stats["error_msg"] = "bad timestamp"
                continue

            batch.append(record)

            if len(batch) >= batch_size:
                if not dry_run:
                    inserted = _insert_batch(session, batch)
                    stats["inserted"] += inserted
                batch = []

        # Insert remaining records
        if batch and not dry_run:
            inserted = _insert_batch(session, batch)
            stats["inserted"] += inserted

    except RuntimeError as e:
        # Handle qhist failures gracefully (e.g., missing accounting data)
        stats["failed"] = True
        stats["error_msg"] = str(e)
        if verbose:
            # Extract just the warning message if present
            error_str = str(e)
            if "missing records" in error_str.lower():
                print(f"SKIPPED (missing accounting data)")
            else:
                print(f"FAILED: {error_str[:100]}")

    return stats


def _insert_batch(session: Session, records: list[dict]) -> int:
    """Insert a batch of records, ignoring duplicates.

    Duplicates are detected by the unique constraint on (job_id, submit).

    Returns:
        Number of records actually inserted
    """
    if not records:
        return 0

    # Use SQLite's INSERT OR IGNORE via on_conflict_do_nothing
    # Conflict is on the unique constraint (job_id, submit)
    stmt = sqlite_insert(Job.__table__).values(records)
    stmt = stmt.on_conflict_do_nothing(index_elements=["job_id", "submit"])

    result = session.execute(stmt)
    session.commit()

    return result.rowcount
