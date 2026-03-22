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
        self.cache = LookupCache(session)

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
        incremental: bool = False,
        resummarize_only: bool = False,
        generate_summary: bool = True,
        recalculate: bool = False,
    ) -> dict:
        """Parse → insert/upsert → charge → summarize scheduler logs.

        Args:
            log_dir: Scheduler log directory (required for file-based schedulers;
                     not needed when resummarize_only=True or recalculate=True)
            period: Single date in YYYY-MM-DD format (takes precedence over start/end)
            start_date: Start of date range (YYYY-MM-DD). Defaults to 2024-01-01.
            end_date: End of date range (YYYY-MM-DD). Defaults to yesterday.
            dry_run: Fetch and parse but skip all DB writes
            batch_size: Records per batch insert (also used by recalculate batching)
            verbose: Print per-day progress
            upsert: Update existing records with fresh-parsed values and
                    recalculate charges; also regenerates daily summaries.
                    Bypasses the already-summarized day skip automatically.
            incremental: Insert new records only; skip existing ones.
                    Re-summarizes only when new records were actually inserted.
                    Bypasses the already-summarized day skip. Mutually exclusive
                    with upsert.
            resummarize_only: Skip log parsing; recompute daily_summary rows
                    from current Job/JobCharge data only.
            generate_summary: Regenerate daily_summary after syncing
                    (ignored when resummarize_only=True or recalculate=True,
                    which always regenerate summaries)
            recalculate: Recompute charges for all DB jobs in the date range
                    without re-parsing log files.  No job fields are changed.
                    Regenerates daily summaries.  Mutually exclusive with
                    upsert, incremental, and resummarize_only.

        Returns:
            dict: {fetched, inserted, updated, errors, recalculated,
                   days_summarized, days_failed, failed_days,
                   days_skipped, skipped_days}
        """
        exclusive_flags = sum([bool(upsert), bool(incremental), bool(resummarize_only), bool(recalculate)])
        if exclusive_flags > 1:
            raise ValueError("upsert, incremental, resummarize_only, and recalculate are mutually exclusive")

        from .summary import get_summarized_dates, generate_daily_summary

        stats = {
            "fetched": 0, "inserted": 0, "updated": 0, "errors": 0,
            "recalculated": 0,
            "days_failed": 0, "failed_days": [],
            "days_skipped": 0, "skipped_days": [],
            "days_summarized": 0,
        }

        # Normalize: treat a single period as a degenerate start==end range so
        # all downstream paths share one unified iteration loop.
        if period is not None:
            start_date = period
            end_date = period
        else:
            if start_date is None:
                start_date = "2024-01-01"
            if end_date is None:
                end_date = (date.today() - timedelta(days=1)).strftime("%Y-%m-%d")

        # ----------------------------------------------------------------
        # resummarize_only: skip all log I/O; just regenerate summaries
        # ----------------------------------------------------------------
        if resummarize_only:
            for day in date_range(start_date, end_date):
                day_date = parse_date_string(day).date()
                generate_daily_summary(self.session, self.machine, day_date, replace=True)
                stats["days_summarized"] += 1
                if verbose:
                    print(f"  Resummarized {day}", flush=True)
            return stats

        # ----------------------------------------------------------------
        # recalculate: recompute charges from DB job rows; no log I/O
        # ----------------------------------------------------------------
        if recalculate:
            recalc_stats = self._recalculate_charges(
                start_date=start_date,
                end_date=end_date,
                dry_run=dry_run,
                batch_size=batch_size,
                verbose=verbose,
                generate_summary=generate_summary,
            )
            stats.update(recalc_stats)
            return stats

        # ----------------------------------------------------------------
        # Normal sync path (insert / upsert / incremental)
        # ----------------------------------------------------------------
        if log_dir is not None:
            log_path = Path(log_dir)
            if not log_path.exists():
                raise RuntimeError(f"Log directory not found: {log_dir}")

        # upsert and incremental both bypass the already-summarized skip
        summarized_dates: set = set()
        if not upsert and not incremental and not dry_run:
            summarized_dates = get_summarized_dates(self.session)

        # Unified single-day / multi-day iteration
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
                should_summarize = day_stats["inserted"] > 0 if incremental else day_stats["fetched"] > 0
                if generate_summary and not dry_run and should_summarize:
                    generate_daily_summary(self.session, self.machine, day_date, replace=True)
                    stats["days_summarized"] += 1

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
        prepared = []
        for r in records:
            rec = r.copy()
            if rec.get('user'):
                rec['user_id'] = self.cache.get_or_create_user(rec['user']).id
            if rec.get('account'):
                rec['account_id'] = self.cache.get_or_create_account(rec['account']).id
            if rec.get('queue'):
                rec['queue_id'] = self.cache.get_or_create_queue(rec['queue']).id
            prepared.append(rec)

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
                existing_records.append(r)  # always collect; dispatch below
            else:
                new_records.append(r)

        n_updated = 0
        if upsert and existing_records:
            n_updated = self._update_batch(existing_records)
        elif existing_records:
            # plain / incremental: fill charges for any existing jobs that lack them
            self._fill_missing_charges(existing_records)

        if not new_records:
            return {"inserted": 0, "updated": n_updated}

        # Bulk-insert new rows; ON CONFLICT DO NOTHING handles any duplicates
        # that slipped through the Python-side dedup (e.g. tz-mismatch on
        # remote PostgreSQL servers with non-UTC DateStyle).
        n_inserted = self._bulk_insert_jobs(new_records)

        # Calculate and upsert charges for all newly inserted jobs.
        # The DB trigger (trg_ensure_job_charge) already created a placeholder
        # row (charge_version=0) for each job, so we must UPSERT rather than
        # plain INSERT to overwrite those placeholders with the real values.
        from sqlalchemy import and_

        job_ids = [r['job_id'] for r in new_records]
        submit_times = [normalize_datetime_to_naive(r['submit']) for r in new_records]

        new_jobs = (
            self.session.query(Job)
            .filter(and_(Job.job_id.in_(job_ids), Job.submit.in_(submit_times)))
            .all()
        )

        if new_jobs:
            charge_records = self._compute_charges_for_jobs(new_jobs)
            if charge_records:
                self._upsert_charges(charge_records)

        # Store raw scheduler records when present
        record_map = {}
        for r in new_records:
            raw = r.get('pbs_record_object') or r.get('slurm_record_object')
            if raw is not None:
                record_map[(r['job_id'], normalize_datetime_to_naive(r['submit']))] = raw

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
                raw = record_map.get((job.job_id, normalize_datetime_to_naive(job.submit)))
                if raw is not None:
                    job_records.append(JobRecord.from_pbs_record(job.id, raw))

            if job_records:
                self.session.add_all(job_records)

        self.session.commit()
        return {"inserted": n_inserted, "updated": n_updated}

    def _upsert_charges(self, charge_records: list[dict]) -> None:
        """Dialect-aware upsert for job_charges rows.

        Uses INSERT ... ON CONFLICT (job_id) DO UPDATE so that:
        - New jobs: overwrites the zero-value placeholder created by the
          trg_ensure_job_charge trigger with real calculated values.
        - Existing jobs: replaces stale charge values (e.g. after an upsert
          refreshes the job's resource field values).

        Args:
            charge_records: List of dicts with job_id, cpu_hours, gpu_hours,
                            memory_hours, qos_factor, charge_version keys.
        """
        if not charge_records:
            return

        dialect = self.session.get_bind().dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as _pg_insert
            insert_fn = _pg_insert
        else:
            from sqlalchemy.dialects.sqlite import insert as _sqlite_insert
            insert_fn = _sqlite_insert

        stmt = insert_fn(JobCharge.__table__).values(charge_records)
        stmt = stmt.on_conflict_do_update(
            index_elements=["job_id"],
            set_={
                "cpu_hours":    stmt.excluded.cpu_hours,
                "gpu_hours":    stmt.excluded.gpu_hours,
                "memory_hours": stmt.excluded.memory_hours,
                "qos_factor":   stmt.excluded.qos_factor,
                "charge_version": stmt.excluded.charge_version,
            },
        )
        self.session.execute(stmt)

    def _compute_charges_for_jobs(self, jobs: list) -> list[dict]:
        """Build charge_records dicts from a list of Job ORM objects.

        Shared by _insert_batch(), _update_batch(), and _recalculate_charges()
        to avoid duplicating the charge-calculation loop.

        Args:
            jobs: List of Job ORM objects with job.id populated.

        Returns:
            List of dicts ready to pass to _upsert_charges().
        """
        charge_records = []
        for job in jobs:
            charges = job.calculate_charges(self.machine)
            charge_records.append({
                'job_id':         job.id,
                'cpu_hours':      charges['cpu_hours'],
                'gpu_hours':      charges['gpu_hours'],
                'memory_hours':   charges['memory_hours'],
                'qos_factor':     charges['qos_factor'],
                'charge_version': 1,
            })
        return charge_records

    def _fill_missing_charges(self, records: list[dict]) -> int:
        """Insert charges for existing jobs that lack a real charge row.

        Called during plain and incremental sync for records that are already
        in the DB.  Runs a cheap outerjoin query — returns immediately when all
        jobs already have charge_version=1 (the common case for a current DB).
        Does NOT modify Job fields or JobRecord rows.

        Note: only matches jobs whose naive-UTC submit timestamp is correctly
        stored in the DB.  Historical jobs bulk-loaded with Mountain-Time
        timestamps will not be matched here; use --recalculate for those.

        Args:
            records: FK-resolved dicts for jobs already in the DB.

        Returns:
            Number of jobs that received backfilled charges.
        """
        if not records:
            return 0

        from sqlalchemy import and_, or_

        job_id_list = [r['job_id'] for r in records]
        submit_list = [normalize_datetime_to_naive(r['submit']) for r in records]

        uncharged = (
            self.session.query(Job)
            .filter(and_(Job.job_id.in_(job_id_list), Job.submit.in_(submit_list)))
            .outerjoin(JobCharge, Job.id == JobCharge.job_id)
            .filter(or_(JobCharge.job_id.is_(None), JobCharge.charge_version == 0))
            .all()
        )

        if not uncharged:
            return 0

        charge_records = self._compute_charges_for_jobs(uncharged)
        if charge_records:
            self._upsert_charges(charge_records)
            self.session.commit()

        return len(uncharged)

    def _recalculate_charges(
        self,
        start_date: str,
        end_date: str,
        dry_run: bool = False,
        batch_size: int = 1000,
        verbose: bool = False,
        generate_summary: bool = True,
    ) -> dict:
        """Recompute charges for all DB jobs in a date range — no log parsing.

        Queries the jobs table by Mountain-Time day boundaries, upserts
        job_charges for every job found, and optionally regenerates
        daily_summary rows.  Job fields and JobRecord rows are never modified.

        Designed for:
        - Historical backfill after a bulk-load that skipped charge calculation
        - Re-running after charging-rule changes
        - Recovery after DB issues

        Args:
            start_date: Start date YYYY-MM-DD (inclusive, Mountain Time day)
            end_date:   End date YYYY-MM-DD (inclusive, Mountain Time day)
            dry_run:    If True, compute charges but skip all DB writes
            batch_size: Jobs per query batch (LIMIT/OFFSET pagination)
            verbose:    Print per-day progress
            generate_summary: If True, regenerate daily_summary after each day

        Returns:
            dict: {fetched, recalculated, days_summarized, days_failed, failed_days,
                   inserted, updated, errors, days_skipped, skipped_days}
        """
        from datetime import datetime, time, timedelta, timezone
        from zoneinfo import ZoneInfo

        from .summary import generate_daily_summary
        from .utils import date_range, parse_date_string

        stats = {
            "fetched": 0, "inserted": 0, "updated": 0, "errors": 0,
            "recalculated": 0,
            "days_summarized": 0, "days_failed": 0, "failed_days": [],
            "days_skipped": 0, "skipped_days": [],
        }

        mountain = ZoneInfo("America/Denver")

        for day in date_range(start_date, end_date):
            day_date = parse_date_string(day).date()

            # Compute naive UTC window for this Mountain-Time day
            # (matches the boundary logic in generate_daily_summary)
            start_utc = (
                datetime.combine(day_date, time.min)
                .replace(tzinfo=mountain)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )
            end_utc = (
                datetime.combine(day_date + timedelta(days=1), time.min)
                .replace(tzinfo=mountain)
                .astimezone(timezone.utc)
                .replace(tzinfo=None)
            )

            # Stream jobs in batches to avoid loading the whole table at once
            from sqlalchemy import and_
            day_count = 0
            offset = 0
            while True:
                batch_jobs = (
                    self.session.query(Job)
                    .filter(and_(Job.end >= start_utc, Job.end < end_utc))
                    .order_by(Job.id)
                    .limit(batch_size)
                    .offset(offset)
                    .all()
                )
                if not batch_jobs:
                    break

                stats["fetched"] += len(batch_jobs)
                day_count += len(batch_jobs)

                if not dry_run:
                    charge_records = self._compute_charges_for_jobs(batch_jobs)
                    if charge_records:
                        self._upsert_charges(charge_records)
                        self.session.commit()
                        stats["recalculated"] += len(charge_records)

                # Expire loaded objects so SQLAlchemy releases their memory
                for job in batch_jobs:
                    self.session.expire(job)

                offset += batch_size

            if verbose:
                action = "(dry run)" if dry_run else f"{day_count:,} jobs"
                print(f"  Recalculated {day}: {action}", flush=True)

            if not dry_run and generate_summary and day_count > 0:
                generate_daily_summary(self.session, self.machine, day_date, replace=True)
                stats["days_summarized"] += 1

        return stats

    def _bulk_insert_jobs(self, records: list[dict]) -> int:
        """Dialect-safe bulk insert with conflict handling.

        Uses INSERT OR IGNORE (SQLite) / INSERT ON CONFLICT DO NOTHING
        (PostgreSQL) so that pre-existing rows are silently skipped rather
        than raising a UniqueViolation error.

        Args:
            records: FK-resolved job dicts (extra keys such as
                     pbs_record_object are stripped before insert)

        Returns:
            Number of rows actually inserted (0 for skipped conflicts)
        """
        if not records:
            return 0

        col_names = {c.name for c in Job.__table__.columns}
        clean = [{k: v for k, v in r.items() if k in col_names} for r in records]

        dialect = self.session.get_bind().dialect.name
        if dialect == "postgresql":
            from sqlalchemy.dialects.postgresql import insert as pg_insert
            stmt = pg_insert(Job.__table__).values(clean).on_conflict_do_nothing(
                constraint="uq_jobs_job_id_submit"
            )
        else:  # sqlite (and any other dialect fallback)
            from sqlalchemy.dialects.sqlite import insert as sqlite_insert
            stmt = sqlite_insert(Job.__table__).values(clean).on_conflict_do_nothing()

        result = self.session.execute(stmt)
        self.session.flush()
        return result.rowcount

    def _update_batch(self, records: list[dict]) -> int:
        """Update existing job records with fresh-parsed field values.

        For each record that already exists in the DB (matched by job_id + submit):
        1. Updates UPDATABLE_JOB_FIELDS on the Job row.
        2. Deletes and re-inserts the JobCharge row (recalculates charges).
        3. Deletes and re-inserts the JobRecord row (replaces raw record).

        Args:
            records: FK-resolved records for existing jobs

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

            mapping = {'id': existing_job.id}
            for field in UPDATABLE_JOB_FIELDS:
                if field in r:
                    mapping[field] = r[field]
            update_mappings.append(mapping)
            db_ids.append(existing_job.id)

            raw = r.get('pbs_record_object') or r.get('slurm_record_object')
            if raw is not None:
                raw_record_map[existing_job.id] = raw

        # No matched jobs at all — nothing to do.
        if not db_ids:
            return 0

        # 1. Bulk-update Job fields (only when there are actual field changes).
        if update_mappings:
            self.session.bulk_update_mappings(Job, update_mappings)
            self.session.flush()

        # 2. Recalculate charges for ALL matched jobs (field-updated or not).
        #    For field-updated jobs: delete existing charges first so the fresh
        #    values (e.g. corrected numcpus/numgpus) are used in the recalc.
        #    For unmatched/missing charges: _upsert_charges inserts them without
        #    touching existing correct rows (ON CONFLICT DO UPDATE).
        if update_mappings:
            # Wipe and fully recalculate for jobs whose fields changed.
            self.session.execute(delete(JobCharge).where(JobCharge.job_id.in_(db_ids)))

        matched_jobs = self.session.query(Job).filter(Job.id.in_(db_ids)).all()
        charge_records = self._compute_charges_for_jobs(matched_jobs)
        if charge_records:
            self._upsert_charges(charge_records)

        # 3. Delete + re-insert JobRecord (replace raw scheduler record)
        if raw_record_map:
            self.session.execute(delete(JobRecord).where(JobRecord.job_id.in_(db_ids)))
            job_records = []
            for job in matched_jobs:
                raw = raw_record_map.get(job.id)
                if raw is not None:
                    job_records.append(JobRecord.from_pbs_record(job.id, raw))
            if job_records:
                self.session.add_all(job_records)

        self.session.commit()
        return len(update_mappings)
