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

# Job fields updated during an upsert (excludes identity/key columns).
UPDATABLE_JOB_FIELDS = frozenset({
    "start", "eligible",                                         # correctable timestamps
    "elapsed", "walltime",                                       # timing metrics
    "numcpus", "numgpus", "numnodes", "mpiprocs", "ompthreads",  # resources
    "reqmem", "memory", "vmemory",                               # memory
    "cputype", "gputype",                                        # type inference
    "resources", "ptargets", "priority", "status", "name",
})

# Scheduler-specific raw-record keys (used by _insert_batch and _update_batch).
RECORD_OBJECT_KEYS = ("pbs_record_object", "slurm_record_object")


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
    (date iteration, dedup, insert/upsert, charge, summarize) is handled here.
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
        upsert: bool = False,
        resummarize_only: bool = False,
        generate_summary: bool = True,
    ) -> dict:
        """Parse → insert/upsert → charge → summarize scheduler logs.

        Args:
            log_dir: Scheduler log directory (required for file-based schedulers;
                     not needed when resummarize_only=True)
            period: Single date in YYYY-MM-DD format (takes precedence over start/end)
            start_date: Start of date range (YYYY-MM-DD). Defaults to 2024-01-01.
            end_date: End of date range (YYYY-MM-DD). Defaults to yesterday.
            dry_run: Fetch and parse but skip all DB writes
            batch_size: Records per batch insert
            verbose: Print per-day progress
            upsert: Update existing records with fresh-parsed values and
                    recalculate charges; also regenerates daily summaries.
                    Bypasses the already-summarized day skip automatically.
            resummarize_only: Skip log parsing; recompute daily_summary rows
                    from current Job/JobCharge data only.
            generate_summary: Regenerate daily_summary after syncing
                    (ignored when resummarize_only=True, which always regenerates)

        Returns:
            dict: {fetched, inserted, updated, errors, days_summarized,
                   days_failed, failed_days, days_skipped, skipped_days}
        """
        from .summary import get_summarized_dates, generate_daily_summary

        stats = {
            "fetched": 0, "inserted": 0, "updated": 0, "errors": 0,
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

        # ----------------------------------------------------------------
        # resummarize_only: skip all log I/O; just regenerate summaries
        # ----------------------------------------------------------------
        if resummarize_only:
            if start_date and end_date:
                for day in date_range(start_date, end_date):
                    day_date = parse_date_string(day).date()
                    generate_daily_summary(self.session, self.machine, day_date, replace=True)
                    stats["days_summarized"] += 1
                    if verbose:
                        print(f"  Resummarized {day}", flush=True)
            else:
                target_period = period or start_date or end_date
                if target_period:
                    day_date = parse_date_string(target_period).date()
                    generate_daily_summary(self.session, self.machine, day_date, replace=True)
                    stats["days_summarized"] = 1
                    if verbose:
                        print(f"  Resummarized {target_period}", flush=True)
            return stats

        # ----------------------------------------------------------------
        # Normal sync path (insert / upsert)
        # ----------------------------------------------------------------
        if log_dir is not None:
            log_path = Path(log_dir)
            if not log_path.exists():
                raise RuntimeError(f"Log directory not found: {log_dir}")

        # upsert bypasses the already-summarized skip (re-parses every day)
        summarized_dates: set = set()
        if not upsert and not dry_run:
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

                day_stats = self._sync_single_day(log_dir, day, dry_run, batch_size, verbose, upsert)
                stats["fetched"] += day_stats["fetched"]
                stats["inserted"] += day_stats["inserted"]
                stats["updated"] += day_stats["updated"]
                stats["errors"] += day_stats["errors"]

                if day_stats.get("failed"):
                    stats["days_failed"] += 1
                    stats["failed_days"].append(day)
                else:
                    if verbose:
                        updated_str = f", {day_stats['updated']:,} updated" if day_stats["updated"] else ""
                        print(
                            f"  Parsed {day} - {day_stats['fetched']:,} jobs, "
                            f"{day_stats['inserted']:,} new{updated_str}",
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

            day_stats = self._sync_single_day(log_dir, target_period, dry_run, batch_size, verbose, upsert)
            stats["fetched"] = day_stats["fetched"]
            stats["inserted"] = day_stats["inserted"]
            stats["updated"] = day_stats["updated"]
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
        upsert: bool = False,
    ) -> dict:
        """Sync one day's records via self.fetch_records().

        Returns:
            dict: {fetched, inserted, updated, errors, failed, error_msg}
        """
        stats = {
            "fetched": 0, "inserted": 0, "updated": 0,
            "errors": 0, "failed": False, "error_msg": None,
        }
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
                        result = self._insert_batch(batch, upsert=upsert)
                        stats["inserted"] += result["inserted"]
                        stats["updated"] += result["updated"]
                    batch = []

            if batch and not dry_run:
                result = self._insert_batch(batch, upsert=upsert)
                stats["inserted"] += result["inserted"]
                stats["updated"] += result["updated"]

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

    def _insert_batch(self, records: list[dict], upsert: bool = False) -> dict:
        """Insert (and optionally update) a batch of job records.

        Handles FK resolution, deduplication, bulk insert, charge calculation,
        and raw-record storage.  When upsert=True, existing records are updated
        via _update_batch() rather than skipped.

        Args:
            records: List of raw job record dicts from fetch_records()
            upsert: If True, update existing records instead of skipping them

        Returns:
            dict: {'inserted': N, 'updated': M}
        """
        if not records:
            return {"inserted": 0, "updated": 0}

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
        existing_records = []
        for r in prepared:
            key = (r['job_id'], normalize_datetime_to_naive(r['submit']))
            if key in seen_keys:
                continue
            seen_keys.add(key)
            if key in existing_pairs:
                if upsert:
                    existing_records.append(r)
            else:
                new_records.append(r)

        n_updated = 0
        if upsert and existing_records:
            n_updated = self._update_batch(existing_records)

        if not new_records:
            return {"inserted": 0, "updated": n_updated}

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

        # Store raw scheduler records when present
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
        return {"inserted": len(new_records), "updated": n_updated}

    def _update_batch(self, records: list[dict]) -> int:
        """Update existing job records with fresh-parsed field values.

        For each record that already exists in the DB (matched by job_id + submit):
        1. Updates UPDATABLE_JOB_FIELDS on the Job row.
        2. Deletes and re-inserts the JobCharge row (recalculates charges).
        3. Deletes and re-inserts the JobRecord row (replaces raw record).

        Args:
            records: Prepared records (FK-resolved) for existing jobs

        Returns:
            Number of jobs updated
        """
        if not records:
            return 0

        from sqlalchemy import and_, delete

        # Fetch existing Job rows to get their DB primary keys
        job_id_list = [r['job_id'] for r in records]
        existing_jobs = (
            self.session.query(Job)
            .filter(Job.job_id.in_(job_id_list))
            .all()
        )

        # Build (job_id, naive_submit) → Job lookup
        job_lookup: dict = {}
        for job in existing_jobs:
            key = (job.job_id, normalize_datetime_to_naive(job.submit))
            job_lookup[key] = job

        update_mappings = []
        db_ids = []
        raw_record_map: dict = {}

        for r in records:
            key = (r['job_id'], normalize_datetime_to_naive(r['submit']))
            existing_job = job_lookup.get(key)
            if existing_job is None:
                continue

            # Build update dict: primary key + all updatable fields present in r
            mapping = {'id': existing_job.id}
            for field in UPDATABLE_JOB_FIELDS:
                if field in r:
                    mapping[field] = r[field]
            update_mappings.append(mapping)
            db_ids.append(existing_job.id)

            for key_name in RECORD_OBJECT_KEYS:
                if key_name in r:
                    raw_record_map[existing_job.id] = r[key_name]
                    break

        if not update_mappings:
            return 0

        # 1. Bulk-update Job fields
        self.session.bulk_update_mappings(Job, update_mappings)
        self.session.flush()

        # 2. Delete + re-insert JobCharge (recalculate from updated Job values)
        self.session.execute(delete(JobCharge).where(JobCharge.job_id.in_(db_ids)))

        updated_jobs = self.session.query(Job).filter(Job.id.in_(db_ids)).all()
        charge_records = []
        for job in updated_jobs:
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

        # 3. Delete + re-insert JobRecord (replace raw scheduler record)
        if raw_record_map:
            self.session.execute(delete(JobRecord).where(JobRecord.job_id.in_(db_ids)))
            job_records = []
            for job in updated_jobs:
                raw = raw_record_map.get(job.id)
                if raw is not None:
                    job_records.append(JobRecord.from_pbs_record(job.id, raw))
            if job_records:
                self.session.add_all(job_records)

        self.session.commit()
        return len(update_mappings)
