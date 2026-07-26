#!/usr/bin/env python3
"""Idempotent schema migration for job_history databases (SQLite + PostgreSQL).

Usage:
    bin/update_jobs_db.py                  # both machines
    bin/update_jobs_db.py casper           # single machine
    bin/update_jobs_db.py casper derecho   # explicit list

The active backend (SQLite or PostgreSQL) is determined by JOB_HISTORY_DB_BACKEND in
your .env file — no manual configuration needed here.
"""
import sys
from sqlalchemy import inspect, text
from job_history.database.models import Base
from job_history.database.session import (
    _ensure_db_triggers,
    _ensure_qos_seed_rows,
    _rename_jhublogin_to_uncharged,
    get_engine,
    VALID_MACHINES,
)

# ── Columns to ADD ──────────────────────────────────────────────────────────
# Each entry: (table, column, SQL type + default)
ADD_COLUMNS = [
    ("job_charges",   "qos_factor",      "REAL DEFAULT 1.0"),
    ("jobs",          "priority",        "TEXT"),
    ("jobs",          "qos_id",          "INTEGER"),
    ("daily_summary", "cpu_charges",     "REAL DEFAULT 0"),
    ("daily_summary", "gpu_charges",     "REAL DEFAULT 0"),
    ("daily_summary", "memory_charges",  "REAL DEFAULT 0"),
    ("daily_summary", "qos_id",          "INTEGER"),
    ("jobs",          "queued",          "TIMESTAMP"),
    ("jobs",          "eligible_secs",   "INTEGER"),
    ("jobs",          "run_count",       "INTEGER"),
]

# ── Columns to DROP ─────────────────────────────────────────────────────────
DROP_COLUMNS = [
    ("jobs", "cputime"),
    ("jobs", "cpupercent"),
    ("jobs", "avgcpu"),
    ("jobs", "count"),
]


def add_column_if_missing(conn, inspector, table, column, definition):
    existing = {c["name"] for c in inspector.get_columns(table)}
    if column in existing:
        print(f"  {table}.{column} already exists — skipping")
    else:
        print(f"  Adding {table}.{column} …")
        conn.execute(text(f"ALTER TABLE {table} ADD COLUMN {column} {definition}"))
        print("  Done.")


def drop_column_if_exists(conn, inspector, table, column):
    existing = {c["name"] for c in inspector.get_columns(table)}
    if column not in existing:
        print(f"  {table}.{column} not found — skipping")
    else:
        print(f"  Dropping {table}.{column} …")
        conn.execute(text(f"ALTER TABLE {table} DROP COLUMN {column}"))
        print("  Done.")


DEFAULT_BACKFILL_CHUNK_SIZE = 100_000


def backfill_qos_id(engine, *, chunk_size: int = DEFAULT_BACKFILL_CHUNK_SIZE):
    """Populate jobs.qos_id from existing priority + queue strings.

    Chunked by primary-key range with per-chunk commit, so the migration:
      - never holds a write lock on jobs for more than one chunk's duration
        (concurrent sync jobs interleave between chunks instead of queueing
        for the full multi-million-row UPDATE),
      - keeps WAL volume bounded (each commit lets WAL recycle),
      - is resumable: if interrupted partway, the next run skips chunks that
        already have the correct qos_id (the IS DISTINCT FROM guard filters
        them to zero work), then continues with the rest,
      - emits per-chunk progress so an operator can watch a multi-million-row
        backfill against production.

    Non-destructive: job_charges.qos_factor values are untouched.  Idempotent:
    re-running on a fully-backfilled table reports 0 rows updated across all
    chunks.
    """
    dialect = engine.dialect.name

    # Resolve seed-row ids once (cheap autocommit read).
    with engine.connect() as conn:
        rows = conn.execute(text("SELECT name, id FROM job_qos")).all()
    qos_ids = {name: qid for name, qid in rows}
    required = {"premium", "regular", "economy", "uncharged", "special"}
    missing = required - set(qos_ids)
    if missing:
        raise RuntimeError(f"Missing JobQoS seed rows: {missing}")

    base_params = {
        "unc":  qos_ids["uncharged"],
        "prem": qos_ids["premium"],
        "spec": qos_ids["special"],
        "econ": qos_ids["economy"],
        "reg":  qos_ids["regular"],
    }

    with engine.connect() as conn:
        bounds = conn.execute(text("SELECT MIN(id), MAX(id) FROM jobs")).one()
    lo, hi = bounds[0], bounds[1]
    if lo is None:
        print("  jobs table is empty — nothing to backfill.")
        return

    if dialect == "postgresql":
        update_sql = text("""
            UPDATE jobs j
            SET qos_id = sub.qid
            FROM (
                SELECT j2.id,
                       CASE
                         WHEN LOWER(COALESCE(q.queue_name, '')) = 'jhublogin' THEN :unc
                         WHEN LOWER(COALESCE(j2.priority, ''))  = 'premium'   THEN :prem
                         WHEN LOWER(COALESCE(j2.priority, ''))  = 'special'   THEN :spec
                         WHEN LOWER(COALESCE(j2.priority, ''))  = 'economy'   THEN :econ
                         ELSE :reg
                       END AS qid
                FROM jobs j2
                LEFT JOIN queues q ON q.id = j2.queue_id
                WHERE j2.id >= :lo AND j2.id < :hi
            ) sub
            WHERE j.id = sub.id AND j.qos_id IS DISTINCT FROM sub.qid;
        """)
    else:  # SQLite
        update_sql = text("""
            UPDATE jobs
            SET qos_id = CASE
                WHEN LOWER(COALESCE((SELECT queue_name FROM queues q WHERE q.id = jobs.queue_id), '')) = 'jhublogin' THEN :unc
                WHEN LOWER(COALESCE(jobs.priority, '')) = 'premium' THEN :prem
                WHEN LOWER(COALESCE(jobs.priority, '')) = 'special' THEN :spec
                WHEN LOWER(COALESCE(jobs.priority, '')) = 'economy' THEN :econ
                ELSE :reg
            END
            WHERE id >= :lo AND id < :hi
              AND (qos_id IS NULL OR qos_id != CASE
                WHEN LOWER(COALESCE((SELECT queue_name FROM queues q WHERE q.id = jobs.queue_id), '')) = 'jhublogin' THEN :unc
                WHEN LOWER(COALESCE(jobs.priority, '')) = 'premium' THEN :prem
                WHEN LOWER(COALESCE(jobs.priority, '')) = 'special' THEN :spec
                WHEN LOWER(COALESCE(jobs.priority, '')) = 'economy' THEN :econ
                ELSE :reg
              END);
        """)

    total_chunks = (hi - lo) // chunk_size + 1
    print(f"  Backfilling jobs.qos_id in {total_chunks:,} chunk(s) of "
          f"{chunk_size:,} ids over range [{lo:,}, {hi:,}] …")

    import time
    start = time.monotonic()
    total_updated = 0
    chunk_idx = 0
    chunk_lo = lo
    while chunk_lo <= hi:
        chunk_hi = chunk_lo + chunk_size
        chunk_idx += 1
        params = {**base_params, "lo": chunk_lo, "hi": chunk_hi}
        with engine.begin() as conn:
            result = conn.execute(update_sql, params)
        n = result.rowcount or 0
        total_updated += n
        # Progress: emit every chunk for small migrations; every 10th for
        # large ones; always emit the last chunk.
        is_last = chunk_hi > hi
        if total_chunks <= 20 or chunk_idx % 10 == 0 or is_last:
            elapsed = time.monotonic() - start
            print(f"    chunk {chunk_idx:>5,}/{total_chunks:<5,}  "
                  f"id [{chunk_lo:>12,}, {chunk_hi:>12,})  "
                  f"updated {n:>7,}  "
                  f"(total {total_updated:>10,}  "
                  f"elapsed {elapsed:6.1f}s)")
        chunk_lo = chunk_hi

    print(f"  Done. Updated {total_updated:,} jobs.qos_id row(s) total "
          f"in {time.monotonic() - start:.1f}s.")


def _executemany_update(conn, stmt, params):
    """Run a parameterized UPDATE over many parameter sets, efficiently.

    psycopg2's native ``executemany`` issues one server round-trip per row,
    which dominates runtime on a multi-million-row backfill.  SQLAlchemy's
    psycopg2 dialect only batches INSERT (``executemany_mode="values_only"``),
    so UPDATE falls back to that slow path.  Use psycopg2's ``execute_batch``,
    which coalesces many statements into a single round-trip.

    SQLite's executemany is in-process and already fast, so it takes the plain
    SQLAlchemy path.
    """
    if conn.dialect.name != "postgresql":
        conn.execute(stmt, params)
        return

    try:
        from psycopg2.extras import execute_batch
    except ImportError:  # psycopg3 or another driver — fall back
        conn.execute(stmt, params)
        return

    # Render the SQLAlchemy text() clause to the driver's paramstyle (pyformat).
    compiled = stmt.compile(dialect=conn.dialect)
    raw_cursor = conn.connection.cursor()
    try:
        execute_batch(raw_cursor, str(compiled), params, page_size=1000)
    finally:
        raw_cursor.close()


def backfill_pbs_time_fields(engine, *, chunk_size: int = DEFAULT_BACKFILL_CHUNK_SIZE):
    """Populate jobs.queued / jobs.eligible_secs / jobs.run_count from job_records.

    These three PBS accounting fields (``qtime``, ``eligible_time``,
    ``run_count``) were parsed but discarded before this migration.  They are
    recoverable without re-reading the accounting logs because ``job_records``
    archives the full raw PbsRecord per job -- which also means this runs
    against the production CNPG server with no log-archive access.

    Unlike backfill_qos_id() this cannot be pure SQL: each row's blob must be
    gunzipped and unpickled to reach the attributes.  The records are pickled
    *unprocessed* (sync never passes ``process=True``), so the attributes are
    raw strings and the SyncPBSLogs parsers apply directly.  Do NOT call
    ``PbsRecord.process_record()`` here -- it rewrites ``eligible_time``
    divided by ``_divisor`` (qhist's display time unit).

    Chunked by primary-key range with per-chunk commit, for the same reasons as
    backfill_qos_id: bounded lock duration so concurrent syncs interleave,
    bounded WAL, resumable, and progress visible on a multi-million-row run.

    Rows are considered "not yet processed" when all three columns are NULL, so
    re-running is cheap.  ``eligible_secs`` legitimately stays NULL where PBS
    never recorded it (derecho before 2025-01-08, when ``eligible_time_enable``
    was off); those rows are still marked done via ``queued``/``run_count``,
    which are present in every accounting record.
    """
    import gzip
    import pickle
    import time

    from job_history.sync.pbs import SyncPBSLogs
    from job_history.sync.utils import safe_int

    with engine.connect() as conn:
        bounds = conn.execute(text("SELECT MIN(id), MAX(id) FROM jobs")).one()
    lo, hi = bounds[0], bounds[1]
    if lo is None:
        print("  jobs table is empty — nothing to backfill.")
        return

    select_sql = text("""
        SELECT j.id, r.compressed_data
        FROM jobs j
        JOIN job_records r ON r.job_id = j.id
        WHERE j.id >= :lo AND j.id < :hi
          AND j.queued IS NULL
          AND j.eligible_secs IS NULL
          AND j.run_count IS NULL
    """)
    update_sql = text("""
        UPDATE jobs
        SET queued = :queued, eligible_secs = :eligible_secs, run_count = :run_count
        WHERE id = :id
    """)

    total_chunks = (hi - lo) // chunk_size + 1
    print(f"  Backfilling jobs.queued/eligible_secs/run_count in {total_chunks:,} chunk(s) "
          f"of {chunk_size:,} ids over range [{lo:,}, {hi:,}] …")

    start = time.monotonic()
    total_updated = total_undecodable = 0
    chunk_idx = 0
    chunk_lo = lo
    while chunk_lo <= hi:
        chunk_hi = chunk_lo + chunk_size
        chunk_idx += 1
        with engine.begin() as conn:
            rows = conn.execute(select_sql, {"lo": chunk_lo, "hi": chunk_hi}).all()
            params = []
            for job_pk, blob in rows:
                try:
                    rec = pickle.loads(gzip.decompress(blob))
                except Exception:
                    total_undecodable += 1
                    continue
                params.append({
                    "id": job_pk,
                    "queued": SyncPBSLogs.parse_pbs_timestamp(getattr(rec, "qtime", None)),
                    "eligible_secs": SyncPBSLogs.parse_pbs_time(getattr(rec, "eligible_time", None)),
                    "run_count": safe_int(getattr(rec, "run_count", None)),
                })
            if params:
                _executemany_update(conn, update_sql, params)
        n = len(params)
        total_updated += n

        is_last = chunk_hi > hi
        if total_chunks <= 20 or chunk_idx % 10 == 0 or is_last:
            elapsed = time.monotonic() - start
            print(f"    chunk {chunk_idx:>5,}/{total_chunks:<5,}  "
                  f"id [{chunk_lo:>12,}, {chunk_hi:>12,})  "
                  f"updated {n:>7,}  "
                  f"(total {total_updated:>10,}  "
                  f"elapsed {elapsed:6.1f}s)")
        chunk_lo = chunk_hi

    print(f"  Done. Updated {total_updated:,} jobs row(s) total "
          f"in {time.monotonic() - start:.1f}s.")
    if total_undecodable:
        print(f"  WARNING: {total_undecodable:,} job_records blob(s) could not be "
              f"decompressed/unpickled and were skipped.")


def migrate(machine):
    print(f"Updating: {machine}")
    engine = get_engine(machine)
    # Create any missing tables (picks up new job_qos table on existing DBs)
    Base.metadata.create_all(engine)
    # Idempotent column adds + drops
    with engine.begin() as conn:
        inspector = inspect(conn)
        for table, col, defn in ADD_COLUMNS:
            add_column_if_missing(conn, inspector, table, col, defn)
        for table, col in DROP_COLUMNS:
            drop_column_if_exists(conn, inspector, table, col)
    # Migrate legacy 'jhublogin' QoS row to 'uncharged' (idempotent), then
    # seed canonical QoS rows and re-assert triggers (defensive)
    _rename_jhublogin_to_uncharged(engine)
    _ensure_qos_seed_rows(engine)
    _ensure_db_triggers(engine)
    # Backfill jobs.qos_id from existing priority + queue strings
    backfill_qos_id(engine)
    # Backfill PBS timestamp/wait fields from archived raw records
    backfill_pbs_time_fields(engine)
    engine.dispose()
    print()


def main():
    # Line-buffer stdout so per-chunk backfill progress is visible while the
    # migration runs.  Python block-buffers when stdout is not a TTY, which
    # hides all progress behind a redirect or a pipe — exactly the case for a
    # long-running production backfill an operator wants to watch.
    sys.stdout.reconfigure(line_buffering=True)

    machines = sys.argv[1:] or sorted(VALID_MACHINES)
    for m in machines:
        if m not in VALID_MACHINES:
            print(f"ERROR: unknown machine {m!r}  (valid: {sorted(VALID_MACHINES)})")
            sys.exit(1)
        migrate(m)
    print("Migration complete.")


if __name__ == "__main__":
    main()
