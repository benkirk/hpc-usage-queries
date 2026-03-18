#!/usr/bin/env python3
"""Idempotent schema migration for job_history databases (SQLite + PostgreSQL).

Usage:
    bin/update_jobs_db.py                  # both machines
    bin/update_jobs_db.py casper           # single machine
    bin/update_jobs_db.py casper derecho   # explicit list

The active backend (SQLite or PostgreSQL) is determined by JH_DB_BACKEND in
your .env file — no manual configuration needed here.
"""
import sys
from sqlalchemy import inspect, text
from job_history.database.session import get_engine, VALID_MACHINES

# ── Columns to ADD ──────────────────────────────────────────────────────────
# Each entry: (table, column, SQL type + default)
ADD_COLUMNS = [
    ("job_charges",   "qos_factor",      "REAL DEFAULT 1.0"),
    ("jobs",          "priority",        "TEXT"),
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


def migrate(machine):
    print(f"Updating: {machine}")
    engine = get_engine(machine)
    with engine.begin() as conn:
        inspector = inspect(conn)
        for table, col, defn in ADD_COLUMNS:
            add_column_if_missing(conn, inspector, table, col, defn)
        for table, col in DROP_COLUMNS:
            drop_column_if_exists(conn, inspector, table, col)
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
