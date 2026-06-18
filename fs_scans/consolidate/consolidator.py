"""Consolidate a finished SQLite scan database into a PostgreSQL schema.

Strategy (one collection at a time):

  1. (Re)create a ``<collection>_staging`` schema and its tables.
  2. Drop the foreign keys so the bulk load is not validated row-by-row.
  3. Stream every table from the read-only SQLite source into the staging
     schema with PostgreSQL ``COPY`` (bounded memory).
  4. Re-add the foreign keys (optional, validated once), build the secondary
     indexes, ANALYZE — i.e. *deferred indexing*, mirroring the SQLite import.
  5. Grant SELECT to the read-only web role (if configured).
  6. Atomically swap the staging schema into place via ``ALTER SCHEMA RENAME``
     and drop the previous generation to reclaim disk.

This preserves the SQLite generation path unchanged and gives a non-disruptive
weekly swap (live readers on other collections are unaffected).
"""

import os
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path

from sqlalchemy import DateTime, text
from sqlalchemy.orm import sessionmaker

from ..cli.common import console
from ..core.config import FsScanConfig
from ..core.database import _ensure_pg_database, get_engine
from ..core.models import (
    AccessHistogram,
    Base,
    Directory,
    DirectoryStats,
    GroupInfo,
    GroupSummary,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram,
    UserInfo,
)
from ..importers.add_table_indexing import (
    add_directories_indexing,
    add_directory_stats_indexing,
)

# Tables in foreign-key dependency order: directories first (referenced by
# directory_stats and self-referenced via parent_id), then the rest.
_LOAD_ORDER = [
    Directory,
    DirectoryStats,
    ScanMetadata,
    OwnerSummary,
    GroupSummary,
    UserInfo,
    GroupInfo,
    AccessHistogram,
    SizeHistogram,
]


def _escape_text(value: str) -> str:
    return (
        value.replace("\\", "\\\\")
        .replace("\t", "\\t")
        .replace("\n", "\\n")
        .replace("\r", "\\r")
    )


def _encode_field(value) -> str:
    r"""Encode one value for the PostgreSQL COPY *text* format (``\t`` / ``\N``)."""
    if value is None:
        return r"\N"
    if isinstance(value, str):
        return _escape_text(value)
    return str(value)


def _encode_datetime(value) -> str:
    r"""Encode a value bound for a TIMESTAMP column.

    SQLite is dynamically typed, so a DateTime column can hold ISO-text
    timestamps, NULL, or an integer epoch sentinel (``0`` = "no access time",
    produced by MAX() over a directory with no recursive files).  Map integer
    epochs to a real timestamp (0 → 1970-01-01) so they keep sorting before
    every real timestamp, exactly as SQLite's numeric-vs-text ordering did.
    """
    if value is None:
        return r"\N"
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    return _escape_text(value)


def _make_encoders(model):
    """Return a per-column list of encoder callables for *model*."""
    encoders = []
    for col in model.__table__.columns:
        encoders.append(_encode_datetime if isinstance(col.type, DateTime) else _encode_field)
    return encoders


class _CopyStream:
    """Adapt an iterable of DB rows to a read()-able text stream for copy_expert."""

    def __init__(self, rows, encoders):
        self._gen = (
            "\t".join(enc(v) for enc, v in zip(encoders, row)) + "\n" for row in rows
        )
        self._buf = ""

    def read(self, size: int = -1) -> str:
        if size is None or size < 0:
            data = self._buf + "".join(self._gen)
            self._buf = ""
            return data
        buf = self._buf
        for line in self._gen:
            buf += line
            if len(buf) >= size:
                break
        self._buf = buf[size:]
        return buf[:size]


def _schema_exists(conn, schema: str) -> bool:
    return conn.execute(
        text("SELECT 1 FROM information_schema.schemata WHERE schema_name = :s"),
        {"s": schema},
    ).fetchone() is not None


def _sqlite_has_table(sconn, table: str) -> bool:
    return sconn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?", (table,)
    ).fetchone() is not None


def _drop_foreign_keys(conn, schema: str) -> None:
    """Drop every foreign-key constraint in *schema* (so COPY is unvalidated)."""
    rows = conn.execute(
        text(
            "SELECT conrelid::regclass::text AS tbl, conname "
            "FROM pg_constraint "
            "WHERE contype = 'f' AND connamespace = "
            "(SELECT oid FROM pg_namespace WHERE nspname = :schema)"
        ),
        {"schema": schema},
    ).fetchall()
    for tbl, conname in rows:
        conn.execute(text(f'ALTER TABLE {tbl} DROP CONSTRAINT "{conname}"'))


def _copy_table(raw_conn, sconn, schema: str, model) -> int:
    """COPY one table from the SQLite source into the staging schema.

    Returns the number of rows copied (0 if the source table is absent).
    """
    table = model.__tablename__
    if not _sqlite_has_table(sconn, table):
        console.print(f"    [yellow]skip[/yellow] {table}: not present in source")
        return 0

    cols = [c.name for c in model.__table__.columns]
    col_list = ", ".join(f'"{c}"' for c in cols)

    # Stream rows out of SQLite (the cursor fetches incrementally → bounded memory).
    cur = sconn.execute(f"SELECT {col_list} FROM {table}")
    stream = _CopyStream(cur, _make_encoders(model))

    pg_cur = raw_conn.cursor()
    pg_cur.copy_expert(
        f'COPY "{schema}"."{table}" ({col_list}) FROM STDIN', stream
    )
    rowcount = pg_cur.rowcount
    pg_cur.close()
    return rowcount


def consolidate_sqlite_to_postgres(
    sqlite_path: Path,
    collection: str,
    *,
    swap: bool = True,
    keep_old: bool = False,
    validate_fks: bool = True,
) -> dict:
    """Load a finished SQLite ``.db`` into a PostgreSQL schema for *collection*.

    Args:
        sqlite_path: Path to the finished source ``.db``.
        collection: Collection name (becomes the live schema, e.g. ``cgd``).
        swap: If True, atomically swap the staging schema into place.
        keep_old: If True, keep the previous ``<collection>_old`` schema instead
            of dropping it (uses more disk; useful for rollback).
        validate_fks: If True, re-add (and validate) the foreign keys after load.

    Returns:
        A dict of per-phase timings and row counts (for the perf pass).
    """
    if FsScanConfig.DB_BACKEND != "postgres":
        raise RuntimeError(
            "consolidate requires FS_SCAN_DB_BACKEND=postgres (target backend)."
        )
    sqlite_path = Path(sqlite_path)
    if not sqlite_path.exists():
        raise FileNotFoundError(f"Source database not found: {sqlite_path}")

    collection = collection.lower()
    staging = f"{collection}_staging"
    old = f"{collection}_old"
    FsScanConfig.validate_postgres()
    _ensure_pg_database()

    engine = get_engine(collection, schema=staging)
    timings: dict = {"collection": collection, "rows": {}}
    t0 = time.perf_counter()

    # 1. Fresh staging schema + tables.
    with engine.begin() as conn:
        conn.execute(text(f'DROP SCHEMA IF EXISTS "{staging}" CASCADE'))
        conn.execute(text(f'CREATE SCHEMA "{staging}"'))
    Base.metadata.create_all(engine)

    # 2. Drop FKs so the bulk COPY is not validated per-row.
    with engine.begin() as conn:
        _drop_foreign_keys(conn, staging)

    # 3. Bulk load via COPY (read-only SQLite source).
    t_copy = time.perf_counter()
    sconn = sqlite3.connect(f"file:{sqlite_path}?mode=ro", uri=True)
    raw_conn = engine.raw_connection()
    try:
        for model in _LOAD_ORDER:
            n = _copy_table(raw_conn, sconn, staging, model)
            timings["rows"][model.__tablename__] = n
            console.print(f"    copied {n:>12,} rows → {model.__tablename__}")
        raw_conn.commit()
    finally:
        raw_conn.close()
        sconn.close()
    timings["copy_s"] = time.perf_counter() - t_copy

    # 4. Deferred constraints + indexes + ANALYZE.
    if validate_fks:
        t_fk = time.perf_counter()
        with engine.begin() as conn:
            conn.execute(text(
                'ALTER TABLE directory_stats ADD CONSTRAINT directory_stats_dir_id_fkey '
                'FOREIGN KEY (dir_id) REFERENCES directories(dir_id)'
            ))
            conn.execute(text(
                'ALTER TABLE directories ADD CONSTRAINT directories_parent_id_fkey '
                'FOREIGN KEY (parent_id) REFERENCES directories(dir_id)'
            ))
        timings["fk_s"] = time.perf_counter() - t_fk

    t_idx = time.perf_counter()
    Session = sessionmaker(bind=engine)
    session = Session()
    try:
        # Larger maintenance_work_mem speeds up the post-load index builds
        # (notably for cgd's tens of millions of rows). USERSET, per-session.
        work_mem = os.getenv("FS_SCAN_PG_MAINTENANCE_WORK_MEM", "1GB")
        session.execute(text(f"SET maintenance_work_mem = '{work_mem}'"))
        add_directories_indexing(session)
        add_directory_stats_indexing(session)
    finally:
        session.close()
    timings["index_s"] = time.perf_counter() - t_idx

    t_analyze = time.perf_counter()
    with engine.begin() as conn:
        for model in _LOAD_ORDER:
            conn.execute(text(f'ANALYZE "{staging}"."{model.__tablename__}"'))
    timings["analyze_s"] = time.perf_counter() - t_analyze

    # 5. Grant read access to the web role, if configured.
    _grant_read_only(engine, staging)

    # 6. Atomic swap.
    if swap:
        t_swap = time.perf_counter()
        with engine.begin() as conn:
            conn.execute(text(f'DROP SCHEMA IF EXISTS "{old}" CASCADE'))
            if _schema_exists(conn, collection):
                conn.execute(text(f'ALTER SCHEMA "{collection}" RENAME TO "{old}"'))
            conn.execute(text(f'ALTER SCHEMA "{staging}" RENAME TO "{collection}"'))
        if not keep_old:
            # Reclaim disk immediately (do not carry 2× the data into next week).
            with engine.begin() as conn:
                conn.execute(text(f'DROP SCHEMA IF EXISTS "{old}" CASCADE'))
        timings["swap_s"] = time.perf_counter() - t_swap

    timings["total_s"] = time.perf_counter() - t0
    return timings


def _grant_read_only(engine, schema: str) -> None:
    """Grant USAGE + SELECT on *schema* to the configured read-only web role.

    No-op when ``FS_SCAN_PG_READONLY_ROLE`` is unset.
    """
    role = os.getenv("FS_SCAN_PG_READONLY_ROLE", "").strip()
    if not role:
        return
    with engine.begin() as conn:
        conn.execute(text(f'GRANT USAGE ON SCHEMA "{schema}" TO "{role}"'))
        conn.execute(text(f'GRANT SELECT ON ALL TABLES IN SCHEMA "{schema}" TO "{role}"'))
