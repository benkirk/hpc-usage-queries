"""Sync job data locally from PBS accounting logs."""
from datetime import datetime, date, timedelta

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

# Optional dependency: rich
try:
    from rich.progress import track

except ImportError:
    track = None

from .database import VALID_MACHINES
from .models import Job, LookupCache
from .pbs_parsers import date_range, date_range_length, parse_date_string
from .utils import normalize_datetime_to_naive, validate_timestamp_ordering


class JobImporter:
    """Handle job imports with normalized schema and charge calculation.

    Delegates lookup-table caching and get-or-create to LookupCache.
    """

    def __init__(self, session: Session, machine: str):
        self.session = session
        self.machine = machine
        self.cache = LookupCache(session)

    def prepare_record(self, record: dict) -> dict:
        """Prepare record for insertion by resolving foreign keys.

        Args:
            record: Raw job record dictionary

        Returns:
            Prepared record with foreign keys resolved
        """
        prepared = record.copy()

        if prepared.get('user'):
            prepared['user_id'] = self.cache.get_or_create_user(prepared['user']).id

        if prepared.get('account'):
            prepared['account_id'] = self.cache.get_or_create_account(prepared['account']).id

        if prepared.get('queue'):
            prepared['queue_id'] = self.cache.get_or_create_queue(prepared['queue']).id

        return prepared


def _insert_batch(session: Session, records: list[dict], importer: JobImporter) -> int:
    """Insert a batch of records, ignoring duplicates.

    Duplicates are detected by the unique constraint on (job_id, submit).

    Args:
        session: SQLAlchemy session
        records: List of job record dictionaries
        importer: JobImporter for handling FK resolution and charge calculation

    Returns:
        Number of records actually inserted
    """
    if not records:
        return 0

    # Prepare records with foreign keys
    prepared = [importer.prepare_record(r) for r in records]

    # Get existing (job_id, submit) pairs to filter out duplicates
    # SQLite stores datetimes as naive, so normalize to naive for comparison
    existing_pairs = set()
    for job_id, submit_dt in session.query(Job.job_id, Job.submit).filter(
        Job.job_id.in_([r['job_id'] for r in prepared])
    ).all():
        # Normalize to naive datetime for comparison
        submit_dt = normalize_datetime_to_naive(submit_dt)
        existing_pairs.add((job_id, submit_dt))

    # Filter out records that already exist or are duplicates within this batch
    # Normalize submit times to naive datetimes for comparison
    seen_keys = set()
    new_records = []
    for r in prepared:
        submit_dt = normalize_datetime_to_naive(r['submit'])

        key = (r['job_id'], submit_dt)
        # Skip if already in database OR already seen in this batch
        if key not in existing_pairs and key not in seen_keys:
            new_records.append(r)
            seen_keys.add(key)

    if not new_records:
        return 0

    # Use ORM bulk_insert_mappings which handles foreign keys properly
    # render_nulls=True ensures NULL foreign keys are properly inserted
    session.bulk_insert_mappings(Job, new_records, render_nulls=True)
    rows_inserted = len(new_records)
    session.flush()

    # Calculate charges for newly inserted jobs
    if rows_inserted > 0:
        from .models import JobCharge
        from sqlalchemy import and_

        # Find jobs that were just inserted (by job_id and submit time)
        # and don't have charges yet
        job_ids = [r['job_id'] for r in prepared]
        submit_times = [r['submit'] for r in prepared]

        jobs = (
            session.query(Job)
            .filter(and_(Job.job_id.in_(job_ids), Job.submit.in_(submit_times)))
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
            .filter(JobCharge.job_id.is_(None))
            .all()
        )

        if jobs:
            charge_records = []
            for job in jobs:
                charges = job.calculate_charges(importer.machine)
                charge_records.append(
                    {
                        'job_id': job.id,
                        'cpu_hours': charges['cpu_hours'],
                        'gpu_hours': charges['gpu_hours'],
                        'memory_hours': charges['memory_hours'],
                        'charge_version': 1,
                    }
                )

            if charge_records:
                session.bulk_insert_mappings(JobCharge, charge_records)

        # Insert JobRecords for PBS log imports (if record_object present)
        # NOTE: Use NEW records (actually inserted), not ALL prepared records
        new_job_ids = [r['job_id'] for r in new_records]
        new_submit_times = [r['submit'] for r in new_records]

        # Create map with NAIVE datetimes for matching (SQLite stores as naive)
        job_ids_map = {}
        for r in new_records:
            if 'record_object' in r:
                submit_naive = normalize_datetime_to_naive(r['submit'])
                job_ids_map[(r['job_id'], submit_naive)] = r

        if job_ids_map:
            from .models import JobRecord

            # Query ALL newly inserted jobs (not just those without charges)
            all_new_jobs = (
                session.query(Job)
                .filter(and_(Job.job_id.in_(new_job_ids), Job.submit.in_(new_submit_times)))
                .outerjoin(JobRecord, Job.id == JobRecord.job_id)
                .filter(JobRecord.job_id.is_(None))
                .all()
            )

            job_records = []
            for job in all_new_jobs:
                # Match job to its original record by (job_id, submit)
                submit_naive = normalize_datetime_to_naive(job.submit)
                record = job_ids_map.get((job.job_id, submit_naive))

                if record and 'record_object' in record:
                    pbs_record = record['record_object']
                    # Use JobRecord class method to handle compression/pickling
                    job_record = JobRecord.from_pbs_record(job.id, pbs_record)
                    job_records.append(job_record)

            if job_records:
                session.add_all(job_records)

    session.commit()
    return rows_inserted


def sync_pbs_logs_bulk(
    session: Session,
    machine: str,
    log_dir: str,
    period: str | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    dry_run: bool = False,
    batch_size: int = 1000,
    verbose: bool = False,
    force: bool = False,
    generate_summary: bool = True,
) -> dict:
    """Sync job records from local PBS accounting logs using bulk insert.

    Parses local PBS log files and has access to additional fields (cputype, gputype)
    from PBS select strings.

    Args:
        session: SQLAlchemy session
        machine: Machine name ('casper' or 'derecho')
        log_dir: Directory containing PBS log files (named YYYYMMDD)
        period: Single date in YYYY-MM-DD format (takes precedence over start/end)
        start_date: Start date for range (YYYY-MM-DD). Defaults to '2024-01-01' if None and period is None.
        end_date: End date for range (YYYY-MM-DD). Defaults to yesterday if None and period is None.
        dry_run: If True, don't actually insert records
        batch_size: Number of records to insert per batch
        verbose: If True, print progress for each day
        force: If True, sync even if day has already been summarized
        generate_summary: If True, generate daily summary after syncing

    Returns:
        Dictionary with sync statistics: {fetched, inserted, errors, days_summarized, days_failed, days_skipped}

    Raises:
        ValueError: If machine is not valid or if neither period nor date range is provided
        RuntimeError: If PBS log file is missing or cannot be parsed
    """
    from pathlib import Path
    from .summary import get_summarized_dates, generate_daily_summary
    from .pbs_read_logs import fetch_jobs_from_pbs_logs

    # Validate machine
    if machine not in VALID_MACHINES:
        raise ValueError(f"Invalid machine: {machine}. Must be one of {VALID_MACHINES}")

    # Validate log_dir
    log_path = Path(log_dir)
    if not log_path.exists():
        raise RuntimeError(f"PBS log directory not found: {log_dir}")

    stats = {
        "fetched": 0, "inserted": 0, "errors": 0,
        "days_failed": 0, "failed_days": [],
        "days_skipped": 0, "skipped_days": [],
        "days_summarized": 0,
    }

    # Apply default date range if not syncing a single period
    if period is None:
        if start_date is None:
            start_date = "2024-01-01"  # Default epoch
        if end_date is None:
            yesterday = date.today() - timedelta(days=1)
            end_date = yesterday.strftime("%Y-%m-%d")

    # Get already-summarized dates if smart skip is enabled
    summarized_dates = set()
    if not force and not dry_run:
        summarized_dates = get_summarized_dates(session)

    # If date range specified, loop one day at a time
    if start_date and end_date:
        days = date_range(start_date, end_date)
        ndays = date_range_length(start_date, end_date)
        iterator = track(days, total=ndays, description="Processing...") if track and verbose else days
        for day in iterator:
            day_date = parse_date_string(day).date()

            # Smart skip: if already summarized, skip fetching
            if day_date in summarized_dates:
                if verbose:
                    print(f"  Skipping {day}... (already summarized)")
                stats["days_skipped"] += 1
                stats["skipped_days"].append(day)
                continue

            day_stats = _sync_pbs_logs_single_day(
                session, machine, log_dir, day, dry_run, batch_size, verbose
            )
            stats["fetched"] += day_stats["fetched"]
            stats["inserted"] += day_stats["inserted"]
            stats["errors"] += day_stats["errors"]

            if day_stats.get("failed"):
                stats["days_failed"] += 1
                stats["failed_days"].append(day)
            else:
                if verbose:
                    print(f"  Parsed {day} - {day_stats['fetched']:,} jobs, {day_stats['inserted']:,} new", flush=True)

                # Generate summary for this day
                if generate_summary and not dry_run and day_stats["fetched"] > 0:
                    generate_daily_summary(session, machine, day_date, replace=True)
                    stats["days_summarized"] += 1
    else:
        # Single day or no date specified
        target_period = period or start_date or end_date
        if target_period:
            day_date = parse_date_string(target_period).date()

            # Smart skip for single day
            if day_date in summarized_dates:
                if verbose:
                    print(f"  Skipping {target_period}... (already summarized)")
                stats["days_skipped"] = 1
                stats["skipped_days"] = [target_period]
                return stats

        day_stats = _sync_pbs_logs_single_day(
            session, machine, log_dir, target_period, dry_run, batch_size, verbose
        )
        stats["fetched"] = day_stats["fetched"]
        stats["inserted"] = day_stats["inserted"]
        stats["errors"] = day_stats["errors"]

        if day_stats.get("failed"):
            stats["days_failed"] = 1
            stats["failed_days"] = [target_period]
        elif generate_summary and not dry_run and target_period and day_stats["fetched"] > 0:
            day_date = parse_date_string(target_period).date()
            generate_daily_summary(session, machine, day_date, replace=True)
            stats["days_summarized"] = 1

    return stats


def _sync_pbs_logs_single_day(
    session: Session,
    machine: str,
    log_dir: str,
    period: str | None,
    dry_run: bool,
    batch_size: int,
    verbose: bool = False,
) -> dict:
    """Sync jobs from PBS logs for a single day.

    Args:
        session: SQLAlchemy session
        machine: Machine name
        log_dir: Directory containing PBS log files
        period: Date in YYYY-MM-DD format
        dry_run: If True, don't insert
        batch_size: Batch size for inserts
        verbose: If True, print warnings

    Returns:
        Dictionary with sync statistics for this day
    """
    from .pbs_read_logs import fetch_jobs_from_pbs_logs

    stats = {"fetched": 0, "inserted": 0, "errors": 0, "failed": False, "error_msg": None}
    batch = []

    # Create importer for this machine if not in dry run mode
    importer = None
    if not dry_run:
        importer = JobImporter(session, machine)

    try:
        for record in fetch_jobs_from_pbs_logs(log_dir=log_dir, machine=machine, date=period):
            stats["fetched"] += 1

            if not record.get("job_id"):
                stats["errors"] += 1
                continue

            # Validate timestamp ordering
            submit = record.get("submit")
            eligible = record.get("eligible")
            start = record.get("start")
            end = record.get("end")

            if not validate_timestamp_ordering(submit, eligible, start, end):
                stats["errors"] += 1
                stats["error_msg"] = "Invalid timestamp ordering"
                continue

            batch.append(record)

            if len(batch) >= batch_size:
                if not dry_run:
                    inserted = _insert_batch(session, batch, importer)
                    stats["inserted"] += inserted
                batch = []

        # Insert remaining records
        if batch and not dry_run:
            inserted = _insert_batch(session, batch, importer)
            stats["inserted"] += inserted

    except RuntimeError as e:
        # Handle PBS log parsing failures gracefully
        stats["failed"] = True
        stats["error_msg"] = str(e)
        if verbose:
            error_str = str(e)
            if "not found" in error_str.lower():
                print(f"  Skipping {period}... (PBS log file not found)")
            elif "Failed to parse" in error_str:
                print(f"  Skipping {period}... (malformed PBS log)")
            else:
                print(f"  Failed to sync {period}: {error_str[:80]}")

    return stats
