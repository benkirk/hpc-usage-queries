"""qhist compatibility layer for DB-backed record retrieval.

Provides db_available() and db_get_records() as drop-in replacements
for the get_pbs_records() log-scanning call in qhist's main loop.

Usage in bin/jobhist:
    from job_history.jobhist_compat import db_available, db_get_records

    if machine and db_available(machine):
        jobs = db_get_records(machine, bounds[0], bounds[1], ...)
    else:
        jobs = get_pbs_records(...)  # fallback to log scanning
"""

import logging
import operator
from datetime import timedelta
from typing import Iterator

from sqlalchemy.orm import joinedload, aliased

from .database import get_db_path, get_session, VALID_MACHINES
from .models import Job, User, Account, Queue

logger = logging.getLogger(__name__)


def db_available(machine: str) -> bool:
    """Return True if a jobhist SQLite database exists for the given machine."""
    if machine not in VALID_MACHINES:
        return False
    try:
        return get_db_path(machine).exists()
    except Exception:
        return False


def _get_sql_column(u, a, q, field: str):
    """Map a data_filter field name to its SQLAlchemy column expression.

    Returns None if the field cannot be translated to SQL (must be
    evaluated in Python after decompression against the full PbsRecord).
    """
    if field == "user":
        return u.username
    elif field == "account":
        return a.account_name
    elif field == "queue":
        return q.queue_name
    elif field == "jobname":
        return Job.name
    elif field == "Exit_status":
        return Job.status
    return None


def _apply_sql_filters(query, u, a, q, id_filter, data_filters):
    """Apply SQL-translatable filters to the query.

    Args:
        query: SQLAlchemy query object
        u, a, q: Aliased User, Account, Queue tables for JOIN-based filtering
        id_filter: List of job ID strings (e.g. ["12345", "67890[28]"])
        data_filters: List of (negation, op, field, value) tuples from qhist

    Returns:
        (query, python_filters) where python_filters is a list of
        (negation, op, field, value) tuples that must be evaluated in
        Python against the decompressed PbsRecord.
    """
    python_filters = []

    # Job ID filter: map string IDs → integer short_id IN (...)
    if id_filter:
        numeric_ids = []
        for id_str in id_filter:
            try:
                # Strip array index "[N]" and server suffix ".hostname"
                numeric_ids.append(int(id_str.split("[")[0].split(".")[0]))
            except ValueError:
                logger.debug(f"Could not parse job ID as integer: {id_str!r}")
        if numeric_ids:
            query = query.filter(Job.short_id.in_(numeric_ids))

    # data_filters: translate string-comparable fields to SQL, defer rest to Python
    for negation, op_func, field, value in (data_filters or []):
        col = _get_sql_column(u, a, q, field)
        if col is None:
            python_filters.append((negation, op_func, field, value))
            continue

        if op_func is operator.eq:
            cond = col == value
        elif op_func is operator.ne:
            cond = col != value
        elif op_func is operator.contains:
            cond = col.contains(value)
        else:
            # Numeric comparison operators on text columns fall through to Python
            python_filters.append((negation, op_func, field, value))
            continue

        query = query.filter(~cond if negation else cond)

    return query, python_filters


def _passes_python_filter(record, negation, op_func, field, value):
    """Evaluate a single (negation, op, field, value) filter against a PbsRecord.

    Returns True if the record passes the filter, False if it should be skipped.
    Handles nested field access via bracket notation (e.g. "Resource_List[ncpus]").
    """
    try:
        if "[" in field:
            dict_field, dict_key = field.split("[", 1)
            dict_key = dict_key.rstrip("]")
            record_val = getattr(record, dict_field, {})[dict_key]
        else:
            record_val = getattr(record, field)

        # Coerce string value to numeric type when record attribute is numeric
        if isinstance(record_val, (int, float)) and not isinstance(value, (int, float)):
            try:
                value = type(record_val)(value)
            except (ValueError, TypeError):
                pass

        result = op_func(record_val, value)
        return (not result) if negation else result
    except (AttributeError, KeyError, TypeError, ValueError):
        return False


def db_get_records(
    machine: str,
    start_dt,
    end_dt,
    time_divisor: float = 3600.0,
    id_filter=None,
    host_filter=None,
    data_filters=None,
    time_filter=None,
    reverse: bool = False,
    chunk_size: int = 500,
) -> Iterator:
    """Query the DB and yield real PbsRecord objects for the given date range.

    Replaces the get_pbs_records() log-scanning call in qhist's main loop.
    Records are yielded with process_record() already applied so they are
    ready for qhist's tabular_output(), list_output(), csv_output(), etc.

    Two-phase filtering:
    - SQL phase: date bounds, job ID, user, account, queue, jobname, Exit_status
    - Python phase: host_filter, time_filter, numeric operators, exotic --filter fields

    Args:
        machine: Machine name ('derecho' or 'casper')
        start_dt: Start datetime (inclusive; from qhist get_time_bounds)
        end_dt: End datetime (start-of-day; extended by +1 day to include full end date)
        time_divisor: Seconds divisor for time fields (3600=hours, 60=min, 1=sec, 86400=days)
        id_filter: List of job ID strings to match against Job.short_id
        host_filter: List of hostnames (Python phase; requires record.get_nodes())
        data_filters: List of (negation, op, field, value) tuples from qhist arg parsing
        time_filter: Intra-day time bounds as list of datetime objects (Python phase)
        reverse: If True, yield records in descending end-time order
        chunk_size: SQLAlchemy yield_per chunk for memory-bounded streaming

    Yields:
        PbsRecord (or DerechoRecord) objects with process_record() applied,
        identical to what get_pbs_records() yields from log files.
    """
    session = get_session(machine)
    try:
        u = aliased(User)
        a = aliased(Account)
        q = aliased(Queue)

        # end_dt from qhist get_time_bounds() is start-of-day midnight for the
        # last requested date. Include all jobs that ended on that day.
        end_dt_exclusive = end_dt + timedelta(days=1)

        query = (
            session.query(Job)
            .options(joinedload(Job.job_record_obj))    # eager 1:1 load avoids N+1
            .outerjoin(u, Job.user_id == u.id)
            .outerjoin(a, Job.account_id == a.id)
            .outerjoin(q, Job.queue_id == q.id)
            .filter(Job.end >= start_dt, Job.end < end_dt_exclusive)
        )

        query, python_filters = _apply_sql_filters(query, u, a, q, id_filter, data_filters)

        order = Job.end.desc() if reverse else Job.end.asc()
        query = query.order_by(order).yield_per(chunk_size)

        for job in query:
            if job.job_record_obj is None:
                logger.debug(f"Job {job.job_id} has no stored PbsRecord; skipping")
                continue

            record = job.job_record_obj.to_pbs_record()
            if record is None:
                logger.warning(f"Failed to decompress PbsRecord for job {job.job_id}")
                continue

            # Apply time divisor and process raw record fields (type-convert, compute derived)
            record._divisor = time_divisor
            record.process_record()

            # --- Phase 2: Python filters (applied post-decompression) ---

            # host_filter: check exec_vnode via record.get_nodes()
            if host_filter:
                nodes = set(record.get_nodes())
                if not all(h in nodes for h in host_filter):
                    continue

            # time_filter: intra-day time range (e.g. from "20260115T090000-20260115T170000")
            if time_filter and len(time_filter) >= 2:
                start_time = time_filter[0].time()
                end_time = time_filter[1].time()
                try:
                    record_time = record.end.time()
                    if not (start_time <= record_time <= end_time):
                        continue
                except AttributeError:
                    pass

            # Python-deferred data_filters: numeric ops, exotic --filter fields
            if python_filters:
                if not all(_passes_python_filter(record, *f) for f in python_filters):
                    continue

            yield record

    finally:
        session.close()
