"""Sync job data from remote HPC machines via qhist command."""

from datetime import datetime, date, timedelta

from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.orm import Session

# Optional dependency: rich
try:
    from rich.progress import track

except ImportError:
    track = None

from .database import VALID_MACHINES
from .models import Job
from .parsers import date_range, date_range_length, parse_date_string
from .remote import fetch_jobs_ssh

# Re-export for backwards compatibility
from .parsers import (
    ALL_FIELDS,
    TIMESTAMP_FIELDS,
    INTEGER_FIELDS,
    FLOAT_FIELDS,
    parse_timestamp,
    parse_int,
    parse_job_id,
    parse_float,
    parse_job_record,
)


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
        period: Single date in YYYY-MM-DD format
        start_date: Start date for range (YYYY-MM-DD)
        end_date: End date for range (YYYY-MM-DD)
        dry_run: If True, don't actually insert records

    Returns:
        Dictionary with sync statistics
    """
    from .log_config import get_logger
    logger = get_logger(__name__)

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
            logger.error(f"Error processing record {record.get('job_id')}: {e}")

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
        session: SQLAlchemy session (ignored when machine='all')
        machine: Machine name ('casper', 'derecho', or 'all')
        period: Single date in YYYY-MM-DD format (takes precedence over start/end)
        start_date: Start date for range (YYYY-MM-DD). Defaults to '2024-01-01' if None and period is None.
        end_date: End date for range (YYYY-MM-DD). Defaults to yesterday if None and period is None.
        dry_run: If True, don't actually insert records
        batch_size: Number of records to insert per batch
        verbose: If True, print progress for each day
        force: If True, sync even if day has already been summarized
        generate_summary: If True, generate daily summary after syncing

    Returns:
        Dictionary with sync statistics (when machine='all', aggregates stats from all machines)
    """
    from .database import get_session as get_db_session
    from .summary import get_summarized_dates, generate_daily_summary

    # Handle machine='all' by recursively syncing all machines
    if machine == "all":
        combined_stats = {
            "fetched": 0, "inserted": 0, "errors": 0,
            "days_failed": 0, "failed_days": [],
            "days_skipped": 0, "skipped_days": [],
            "days_summarized": 0,
            "machines": {},
        }

        for m in sorted(VALID_MACHINES):
            if verbose:
                print(f"\n{'='*60}")
                print(f"Syncing {m}...")
                print(f"{'='*60}")

            # Get a new session for this machine
            machine_session = get_db_session(m)
            try:
                machine_stats = sync_jobs_bulk(
                    session=machine_session,
                    machine=m,
                    period=period,
                    start_date=start_date,
                    end_date=end_date,
                    dry_run=dry_run,
                    batch_size=batch_size,
                    verbose=verbose,
                    force=force,
                    generate_summary=generate_summary,
                )

                # Aggregate stats
                combined_stats["fetched"] += machine_stats["fetched"]
                combined_stats["inserted"] += machine_stats["inserted"]
                combined_stats["errors"] += machine_stats["errors"]
                combined_stats["days_failed"] += machine_stats["days_failed"]
                combined_stats["failed_days"].extend(machine_stats["failed_days"])
                combined_stats["days_skipped"] += machine_stats["days_skipped"]
                combined_stats["skipped_days"].extend(machine_stats["skipped_days"])
                combined_stats["days_summarized"] += machine_stats["days_summarized"]
                combined_stats["machines"][m] = machine_stats

            finally:
                machine_session.close()

        return combined_stats

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

            day_stats = _sync_single_day(session, machine, day, dry_run, batch_size, verbose)
            stats["fetched"] += day_stats["fetched"]
            stats["inserted"] += day_stats["inserted"]
            stats["errors"] += day_stats["errors"]

            if day_stats.get("failed"):
                stats["days_failed"] += 1
                stats["failed_days"].append(day)
            else:
                if verbose:
                    print(f"  Fetched {day} - {day_stats['fetched']:,} jobs, {day_stats['inserted']:,} new", flush=True)

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

        day_stats = _sync_single_day(session, machine, target_period, dry_run, batch_size, verbose)
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
        period: Date in YYYY-MM-DD format
        dry_run: If True, don't insert
        batch_size: Batch size for inserts
        verbose: If True, print warnings

    Returns:
        Dictionary with sync statistics for this day
    """
    stats = {"fetched": 0, "inserted": 0, "errors": 0, "failed": False, "error_msg": None}
    batch = []

    # Create importer for this machine if not in dry run mode
    importer = None
    if not dry_run:
        importer = JobImporter(session, machine)

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
                    inserted = _insert_batch(session, batch, importer)
                    stats["inserted"] += inserted
                batch = []

        # Insert remaining records
        if batch and not dry_run:
            inserted = _insert_batch(session, batch, importer)
            stats["inserted"] += inserted

    except RuntimeError as e:
        # Handle qhist failures gracefully (e.g., missing accounting data)
        stats["failed"] = True
        stats["error_msg"] = str(e)
        if verbose:
            # Extract just the warning message if present
            error_str = str(e)
            if "missing records" in error_str.lower():
                print(f"  Skipping {period}... (missing accounting data)")
            elif "Failed to parse qhist JSON output" in error_str:
                # JSON parse errors usually indicate missing/corrupted accounting data
                print(f"  Skipping {period}... (malformed accounting data)")
            else:
                print(f"  Failed to sync {period}: {error_str[:80]}")

    return stats


class JobImporter:
    """Handle job imports with normalized schema and charge calculation.

    This class manages job imports with:
    - In-memory caches of lookup tables (users, accounts, queues)
    - Foreign key resolution during import
    - Charge calculation immediately after job insertion
    """

    def __init__(self, session: Session, machine: str):
        """Initialize the importer.

        Args:
            session: SQLAlchemy session
            machine: Machine name ('casper' or 'derecho')
        """
        self.session = session
        self.machine = machine

        # Initialize caches
        self.user_cache = {}  # username -> id
        self.account_cache = {}  # account_name -> id
        self.queue_cache = {}  # queue_name -> id
        self._load_caches()

    def _load_caches(self):
        """Load lookup tables into memory for fast inserts."""
        from .models import Account, Queue, User

        for user in self.session.query(User).all():
            self.user_cache[user.username] = user.id

        for account in self.session.query(Account).all():
            self.account_cache[account.account_name] = account.id

        for queue in self.session.query(Queue).all():
            self.queue_cache[queue.queue_name] = queue.id

    def _get_or_create_user(self, username: str) -> int:
        """Get user ID, creating if necessary.

        Args:
            username: Username string

        Returns:
            User ID
        """
        if username in self.user_cache:
            return self.user_cache[username]

        from .models import User

        user = User(username=username)
        self.session.add(user)
        self.session.flush()
        self.user_cache[username] = user.id
        return user.id

    def _get_or_create_account(self, account_name: str) -> int:
        """Get account ID, creating if necessary.

        Args:
            account_name: Account name string

        Returns:
            Account ID
        """
        if account_name in self.account_cache:
            return self.account_cache[account_name]

        from .models import Account

        account = Account(account_name=account_name)
        self.session.add(account)
        self.session.flush()
        self.account_cache[account_name] = account.id
        return account.id

    def _get_or_create_queue(self, queue_name: str) -> int:
        """Get queue ID, creating if necessary.

        Args:
            queue_name: Queue name string

        Returns:
            Queue ID
        """
        if queue_name in self.queue_cache:
            return self.queue_cache[queue_name]

        from .models import Queue

        queue = Queue(queue_name=queue_name)
        self.session.add(queue)
        self.session.flush()
        self.queue_cache[queue_name] = queue.id
        return queue.id

    def prepare_record(self, record: dict) -> dict:
        """Prepare record for insertion by resolving foreign keys.

        Args:
            record: Raw job record dictionary

        Returns:
            Prepared record with foreign keys resolved
        """
        # Make a copy to avoid mutating the original
        prepared = record.copy()

        # Resolve foreign keys - keep both normalized and denormalized fields
        if 'user' in prepared and prepared['user']:
            prepared['user_id'] = self._get_or_create_user(prepared['user'])

        if 'account' in prepared and prepared['account']:
            prepared['account_id'] = self._get_or_create_account(prepared['account'])

        if 'queue' in prepared and prepared['queue']:
            prepared['queue_id'] = self._get_or_create_queue(prepared['queue'])

        return prepared


def _insert_batch(session: Session, records: list[dict], importer: JobImporter | None = None) -> int:
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
    if importer:
        prepared = [importer.prepare_record(r) for r in records]
    else:
        prepared = records

    # Get existing (job_id, submit) pairs to filter out duplicates
    # SQLite stores datetimes as naive, so normalize to naive for comparison
    existing_pairs = set()
    for job_id, submit_dt in session.query(Job.job_id, Job.submit).filter(
        Job.job_id.in_([r['job_id'] for r in prepared])
    ).all():
        # Normalize to naive datetime for comparison
        if submit_dt and submit_dt.tzinfo:
            submit_dt = submit_dt.replace(tzinfo=None)
        existing_pairs.add((job_id, submit_dt))

    # Filter out records that already exist
    # Normalize submit times to naive datetimes for comparison
    new_records = []
    for r in prepared:
        submit_dt = r['submit']
        if submit_dt and submit_dt.tzinfo:
            submit_dt = submit_dt.replace(tzinfo=None)

        key = (r['job_id'], submit_dt)
        if key not in existing_pairs:
            new_records.append(r)

    if not new_records:
        return 0

    # Use ORM bulk_insert_mappings which handles foreign keys properly
    # render_nulls=True ensures NULL foreign keys are properly inserted
    session.bulk_insert_mappings(Job, new_records, render_nulls=True)
    rows_inserted = len(new_records)
    session.flush()

    # Calculate charges for newly inserted jobs
    if importer and rows_inserted > 0:
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

    session.commit()
    return rows_inserted
