"""Abstract base class and shared machinery for scheduler log synchronization."""

from abc import ABC, abstractmethod
from datetime import date, timedelta
from pathlib import Path
from typing import Iterator

from sqlalchemy.orm import Session

from ..database import VALID_MACHINES
from ..database import Job, JobCharge, JobRecord, LookupCache
from .utils import (
    date_range, date_range_length, parse_date_string,
    normalize_datetime_to_naive, validate_timestamp_ordering,
)

try:
    from rich.progress import track
except ImportError:
    track = None


# Machine → scheduler name mapping.  Add new machines here.
MACHINE_SCHEDULERS = {
    "derecho": "pbs",
    "casper": "pbs",
}


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


class SyncBase(ABC):
    """Abstract base for scheduler log synchronization.

    Subclasses implement only fetch_records(); the full sync lifecycle
    (date iteration, dedup, insert, charge, summarize) is handled here.
    """

    SCHEDULER_NAME: str = ""  # override in subclass

    def __init__(self, session: Session, machine: str):
        if machine not in VALID_MACHINES:
            raise ValueError(f"Invalid machine: {machine}. Must be one of {VALID_MACHINES}")
        self.session = session
        self.machine = machine
        self._importer: JobImporter | None = None

    @property
    def importer(self) -> JobImporter:
        """Lazy-initialised FK resolver."""
        if self._importer is None:
            self._importer = JobImporter(self.session, self.machine)
        return self._importer

    @classmethod
    def scheduler_name(cls) -> str:
        return cls.SCHEDULER_NAME or cls.__name__

    # ------------------------------------------------------------------
    # Abstract interface — implement in each scheduler subclass
    # ------------------------------------------------------------------

    @abstractmethod
    def fetch_records(self, log_dir: str | Path | None, period: str) -> Iterator[dict]:
        """Yield normalized job dicts for a single day (YYYY-MM-DD).

        Args:
            log_dir: Path to scheduler log directory (may be None for schedulers
                     that pull data without a local log file)
            period:  Target date in YYYY-MM-DD format

        Yields:
            Normalized job dictionaries ready for database insertion.
            Include a scheduler-specific key to store the raw record object,
            e.g. 'pbs_record_object' for PBS, 'slurm_record_object' for SLURM.

        Raises:
            RuntimeError: If the log source is unavailable or unreadable
        """
        ...

    # ------------------------------------------------------------------
    # Template methods — scheduler-agnostic orchestration
    # ------------------------------------------------------------------

    def sync(
        self,
        log_dir: str | Path | None,
        period: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
        dry_run: bool = False,
        batch_size: int = 1000,
        verbose: bool = False,
        force: bool = False,
        generate_summary: bool = True,
    ) -> dict:
        """Parse → insert → charge → summarize scheduler logs.

        Args:
            log_dir: Scheduler log directory (required for file-based schedulers)
            period: Single date in YYYY-MM-DD format (takes precedence over start/end)
            start_date: Start of date range (YYYY-MM-DD). Defaults to 2024-01-01.
            end_date: End of date range (YYYY-MM-DD). Defaults to yesterday.
            dry_run: Fetch and parse but skip all DB writes
            batch_size: Records per batch insert
            verbose: Print per-day progress
            force: Sync even for days already summarized
            generate_summary: Regenerate daily_summary after syncing

        Returns:
            dict: {fetched, inserted, errors, days_summarized,
                   days_failed, failed_days, days_skipped, skipped_days}
        """
        from .summary import get_summarized_dates, generate_daily_summary

        if log_dir is not None:
            log_path = Path(log_dir)
            if not log_path.exists():
                raise RuntimeError(f"Log directory not found: {log_dir}")

        stats = {
            "fetched": 0, "inserted": 0, "errors": 0,
            "days_failed": 0, "failed_days": [],
            "days_skipped": 0, "skipped_days": [],
            "days_summarized": 0,
        }

        # Apply default date range when no single period is requested
        if period is None:
            if start_date is None:
                start_date = "2024-01-01"
            if end_date is None:
                end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        # Smart-skip: collect already-summarized dates once up front
        summarized_dates: set = set()
        if not force and not dry_run:
            summarized_dates = get_summarized_dates(self.session)

        if start_date and end_date:
            # Multi-day range
            days = date_range(start_date, end_date)
            ndays = date_range_length(start_date, end_date)
            iterator = track(days, total=ndays, description="Processing...") if track and verbose else days

            for day in iterator:
                day_date = parse_date_string(day).date()

                if day_date in summarized_dates:
                    if verbose:
                        print(f"  Skipping {day}... (already summarized)")
                    stats["days_skipped"] += 1
                    stats["skipped_days"].append(day)
                    continue

                day_stats = self._sync_single_day(log_dir, day, dry_run, batch_size, verbose)
                stats["fetched"] += day_stats["fetched"]
                stats["inserted"] += day_stats["inserted"]
                stats["errors"] += day_stats["errors"]

                if day_stats.get("failed"):
                    stats["days_failed"] += 1
                    stats["failed_days"].append(day)
                else:
                    if verbose:
                        print(
                            f"  Parsed {day} - {day_stats['fetched']:,} jobs, "
                            f"{day_stats['inserted']:,} new",
                            flush=True,
                        )
                    if generate_summary and not dry_run and day_stats["fetched"] > 0:
                        generate_daily_summary(self.session, self.machine, day_date, replace=True)
                        stats["days_summarized"] += 1

        else:
            # Single day
            target_period = period or start_date or end_date
            if target_period:
                day_date = parse_date_string(target_period).date()
                if day_date in summarized_dates:
                    if verbose:
                        print(f"  Skipping {target_period}... (already summarized)")
                    stats["days_skipped"] = 1
                    stats["skipped_days"] = [target_period]
                    return stats

            day_stats = self._sync_single_day(log_dir, target_period, dry_run, batch_size, verbose)
            stats["fetched"] = day_stats["fetched"]
            stats["inserted"] = day_stats["inserted"]
            stats["errors"] = day_stats["errors"]

            if day_stats.get("failed"):
                stats["days_failed"] = 1
                stats["failed_days"] = [target_period]
            elif generate_summary and not dry_run and target_period and day_stats["fetched"] > 0:
                day_date = parse_date_string(target_period).date()
                generate_daily_summary(self.session, self.machine, day_date, replace=True)
                stats["days_summarized"] = 1

        return stats

    def _sync_single_day(
        self,
        log_dir: str | Path | None,
        period: str | None,
        dry_run: bool,
        batch_size: int,
        verbose: bool = False,
    ) -> dict:
        """Sync one day's records via self.fetch_records().

        Returns:
            dict: {fetched, inserted, errors, failed, error_msg}
        """
        stats = {"fetched": 0, "inserted": 0, "errors": 0, "failed": False, "error_msg": None}
        batch = []

        try:
            for record in self.fetch_records(log_dir, period):
                stats["fetched"] += 1

                if not record.get("job_id"):
                    stats["errors"] += 1
                    continue

                if not validate_timestamp_ordering(
                    record.get("submit"), record.get("eligible"),
                    record.get("start"), record.get("end"),
                ):
                    stats["errors"] += 1
                    continue

                batch.append(record)

                if len(batch) >= batch_size:
                    if not dry_run:
                        stats["inserted"] += self._insert_batch(batch)
                    batch = []

            if batch and not dry_run:
                stats["inserted"] += self._insert_batch(batch)

        except RuntimeError as e:
            stats["failed"] = True
            stats["error_msg"] = str(e)
            if verbose:
                error_str = str(e)
                if "not found" in error_str.lower():
                    print(f"  Skipping {period}... (log file not found)")
                elif "Failed to parse" in error_str:
                    print(f"  Skipping {period}... (malformed log)")
                else:
                    print(f"  Failed to sync {period}: {error_str[:80]}")

        return stats

    def _insert_batch(self, records: list[dict]) -> int:
        """Insert a batch of job records with FK resolution and charge calculation.

        Handles deduplication against existing DB rows, bulk insert,
        charge calculation, and optional raw-record storage.

        Args:
            records: List of raw job record dicts from fetch_records()

        Returns:
            Number of records actually inserted
        """
        if not records:
            return 0

        # Resolve foreign keys (user/account/queue → IDs)
        prepared = [self.importer.prepare_record(r) for r in records]

        # Detect duplicates: check (job_id, submit) pairs already in the DB
        existing_pairs: set = set()
        for job_id, submit_dt in self.session.query(Job.job_id, Job.submit).filter(
            Job.job_id.in_([r['job_id'] for r in prepared])
        ).all():
            existing_pairs.add((job_id, normalize_datetime_to_naive(submit_dt)))

        seen_keys: set = set()
        new_records = []
        for r in prepared:
            key = (r['job_id'], normalize_datetime_to_naive(r['submit']))
            if key not in existing_pairs and key not in seen_keys:
                new_records.append(r)
                seen_keys.add(key)

        if not new_records:
            return 0

        # Bulk-insert new rows (render_nulls=True handles NULL FK columns)
        self.session.bulk_insert_mappings(Job, new_records, render_nulls=True)
        self.session.flush()

        # Calculate charges for the newly inserted jobs
        from sqlalchemy import and_

        job_ids = [r['job_id'] for r in new_records]
        submit_times = [r['submit'] for r in new_records]

        uncharged_jobs = (
            self.session.query(Job)
            .filter(and_(Job.job_id.in_(job_ids), Job.submit.in_(submit_times)))
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
            .filter(JobCharge.job_id.is_(None))
            .all()
        )

        if uncharged_jobs:
            charge_records = []
            for job in uncharged_jobs:
                charges = job.calculate_charges(self.machine)
                charge_records.append({
                    'job_id': job.id,
                    'cpu_hours': charges['cpu_hours'],
                    'gpu_hours': charges['gpu_hours'],
                    'memory_hours': charges['memory_hours'],
                    'charge_version': 1,
                })
            if charge_records:
                self.session.bulk_insert_mappings(JobCharge, charge_records)

        # Store raw scheduler records when present.
        # Each scheduler uses its own key (e.g. 'pbs_record_object',
        # 'slurm_record_object') so the gate is: any key ending in '_record_object'.
        RECORD_OBJECT_KEYS = ("pbs_record_object", "slurm_record_object")
        record_map = {}
        for r in new_records:
            for key in RECORD_OBJECT_KEYS:
                if key in r:
                    submit_naive = normalize_datetime_to_naive(r['submit'])
                    record_map[(r['job_id'], submit_naive)] = r[key]
                    break

        if record_map:
            jobs_without_record = (
                self.session.query(Job)
                .filter(and_(Job.job_id.in_(job_ids), Job.submit.in_(submit_times)))
                .outerjoin(JobRecord, Job.id == JobRecord.job_id)
                .filter(JobRecord.job_id.is_(None))
                .all()
            )

            job_records = []
            for job in jobs_without_record:
                submit_naive = normalize_datetime_to_naive(job.submit)
                raw = record_map.get((job.job_id, submit_naive))
                if raw is not None:
                    job_records.append(JobRecord.from_pbs_record(job.id, raw))

            if job_records:
                self.session.add_all(job_records)

        self.session.commit()
        return len(new_records)
