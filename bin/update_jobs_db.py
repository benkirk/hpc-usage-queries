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


def backfill_qos_id(engine):
    """Populate jobs.qos_id from existing priority + queue strings.

    Non-destructive: job_charges.qos_factor values are untouched.  Restricted
    to rows where qos_id differs from the expected value, so re-running this
    matches zero rows after the first successful run.
    """
    dialect = engine.dialect.name
    with engine.begin() as conn:
        rows = conn.execute(text("SELECT name, id FROM job_qos")).all()
        qos_ids = {name: qid for name, qid in rows}
        required = {"premium", "regular", "economy", "jhublogin"}
        missing = required - set(qos_ids)
        if missing:
            raise RuntimeError(f"Missing JobQoS seed rows: {missing}")

        params = {
            "jhub": qos_ids["jhublogin"],
            "prem": qos_ids["premium"],
            "econ": qos_ids["economy"],
            "reg":  qos_ids["regular"],
        }

        if dialect == "postgresql":
            print("  Backfilling jobs.qos_id …")
            result = conn.execute(text("""
                UPDATE jobs j
                SET qos_id = sub.qid
                FROM (
                    SELECT j2.id,
                           CASE
                             WHEN LOWER(COALESCE(q.queue_name, '')) = 'jhublogin' THEN :jhub
                             WHEN LOWER(COALESCE(j2.priority, ''))  = 'premium'   THEN :prem
                             WHEN LOWER(COALESCE(j2.priority, ''))  = 'economy'   THEN :econ
                             ELSE :reg
                           END AS qid
                    FROM jobs j2
                    LEFT JOIN queues q ON q.id = j2.queue_id
                ) sub
                WHERE j.id = sub.id AND j.qos_id IS DISTINCT FROM sub.qid;
            """), params)
            print(f"  Updated {result.rowcount} jobs.qos_id row(s).")
        else:  # SQLite
            print("  Backfilling jobs.qos_id …")
            result = conn.execute(text("""
                UPDATE jobs
                SET qos_id = CASE
                    WHEN LOWER(COALESCE((SELECT queue_name FROM queues q WHERE q.id = jobs.queue_id), '')) = 'jhublogin' THEN :jhub
                    WHEN LOWER(COALESCE(jobs.priority, '')) = 'premium' THEN :prem
                    WHEN LOWER(COALESCE(jobs.priority, '')) = 'economy' THEN :econ
                    ELSE :reg
                END
                WHERE qos_id IS NULL OR qos_id != CASE
                    WHEN LOWER(COALESCE((SELECT queue_name FROM queues q WHERE q.id = jobs.queue_id), '')) = 'jhublogin' THEN :jhub
                    WHEN LOWER(COALESCE(jobs.priority, '')) = 'premium' THEN :prem
                    WHEN LOWER(COALESCE(jobs.priority, '')) = 'economy' THEN :econ
                    ELSE :reg
                END;
            """), params)
            print(f"  Updated {result.rowcount} jobs.qos_id row(s).")


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
    # Seed canonical QoS rows and re-assert triggers (defensive)
    _ensure_qos_seed_rows(engine)
    _ensure_db_triggers(engine)
    # Backfill jobs.qos_id from existing priority + queue strings
    backfill_qos_id(engine)
    engine.dispose()
    print()


def main():
    machines = sys.argv[1:] or sorted(VALID_MACHINES)
    for m in machines:
        if m not in VALID_MACHINES:
            print(f"ERROR: unknown machine {m!r}  (valid: {sorted(VALID_MACHINES)})")
            sys.exit(1)
        migrate(m)
    print("Migration complete.")


if __name__ == "__main__":
    main()
