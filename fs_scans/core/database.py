"""Database connection and session management for GPFS scan data.

Supports two backends, selected by the ``FS_SCAN_DB_BACKEND`` environment
variable (or a ``.env`` file loaded via python-dotenv):

  sqlite   (default) — per-collection .db files under the data directory
  postgres           — one database per filesystem on a shared PostgreSQL
                       (CNPG) server, with one schema per collection

See ``fs_scans/core/config.py`` and ``.env.example`` for configuration details.
"""

import os
import re
import threading
from pathlib import Path

from sqlalchemy import create_engine, Engine, text
from sqlalchemy.orm import sessionmaker

from .config import FsScanConfig
from .models import Base

# Default database directory (module directory + /data)
_DEFAULT_DATA_DIR = Path(__file__).parent.parent / "data"

# Module-level cache for the configured data directory (set via CLI)
_data_dir_override: Path | None = None

# Module-level engine cache with thread safety for parallel queries.
# Keyed by a tuple that captures the backend target (sqlite path, or postgres
# host/db/schema) so a change of backend or schema yields a distinct engine.
_engine_cache: dict[tuple, Engine] = {}
_engine_cache_lock = threading.Lock()


def get_data_dir() -> Path:
    """Get the current data directory for filesystem databases.

    Precedence:
        1. Module-level override (set via set_data_dir() from CLI)
        2. FS_SCAN_DATA_DIR environment variable
        3. Default: module directory / data

    Returns:
        Path to the data directory (created if it doesn't exist)
    """
    if _data_dir_override is not None:
        data_dir = _data_dir_override
    elif env_dir := os.environ.get("FS_SCAN_DATA_DIR"):
        data_dir = Path(env_dir)
    else:
        data_dir = _DEFAULT_DATA_DIR

    # Create directory if it doesn't exist
    try:
        data_dir.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        raise OSError(f"Failed to create data directory '{data_dir}': {e}") from e

    return data_dir


def set_data_dir(path: Path | None) -> None:
    """Set the data directory override (typically from CLI --data-dir).

    Args:
        path: Path to use as data directory, or None to clear override
    """
    global _data_dir_override
    _data_dir_override = path


def get_data_dir_info() -> tuple[Path, str]:
    """Get the current data directory and its source.

    Returns:
        Tuple of (data_dir_path, source_description)
        Source is one of: "CLI --data-dir", "FS_SCAN_DATA_DIR env var", "default"
    """
    if _data_dir_override is not None:
        return _data_dir_override, "CLI --data-dir"
    elif env_dir := os.environ.get("FS_SCAN_DATA_DIR"):
        return Path(env_dir), "FS_SCAN_DATA_DIR env var"
    else:
        return _DEFAULT_DATA_DIR, "default"


def extract_filesystem_from_filename(filename: str) -> str | None:
    """Extract filesystem name from a scan log filename.

    Expected patterns:
        GPFS format:
            20260111_csfs1_asp.list.list_all.log -> asp
            20260111_csfs1_cisl.list.list_all.log.xz -> cisl
        Lustre format:
            20260204_desc1_gdex.lfs-scan -> gdex
            20260204_desc1_glade_p_archive.lfs-scan -> glade_p_archive

    Args:
        filename: Name of the log file (with or without path)

    Returns:
        Filesystem name or None if pattern doesn't match
    """
    basename = Path(filename).name

    # Try GPFS pattern: YYYYMMDD_server_filesystem.list...
    match = re.match(r"\d{8}_[^_]+_([^.]+)\.list", basename)
    if match:
        return match.group(1)

    # Try Lustre pattern: YYYYMMDD_server_filesystem.lfs-scan
    match = re.match(r"\d{8}_[^_]+_([^.]+)\.lfs-scan", basename)
    if match:
        return match.group(1)

    return None


def extract_scan_timestamp(filename: str):
    """Extract scan timestamp from a GPFS scan log filename.

    Expected patterns:
        20260111_csfs1_asp.list.list_all.log -> datetime(2026, 1, 11)
        20260111_csfs1_cisl.list.list_all.log.xz -> datetime(2026, 1, 11)

    Args:
        filename: Name of the log file (with or without path)

    Returns:
        datetime object or None if pattern doesn't match
    """
    from datetime import datetime

    basename = Path(filename).name
    # Pattern: YYYYMMDD_...
    match = re.match(r"(\d{8})_", basename)
    if match:
        date_str = match.group(1)
        try:
            return datetime.strptime(date_str, "%Y%m%d")
        except ValueError:
            return None
    return None


def get_db_path(filesystem: str, db_path: Path | None = None) -> Path:
    """Get the SQLite database path for a specific filesystem/collection.

    Precedence for determining database path:
        1. Explicit db_path argument (highest priority - from CLI --db)
        2. FS_SCAN_DB environment variable
        3. get_data_dir() / f"{filesystem}.db" (default)

    Raises ``RuntimeError`` when the active backend is not SQLite — use
    ``get_db_url()`` for a backend-agnostic connection descriptor instead.

    Args:
        filesystem: Filesystem/collection name (e.g., 'asp', 'cisl', 'cgd')
        db_path: Explicit database path override (typically from CLI --db option)

    Returns:
        Path to the SQLite database file
    """
    # An explicit path is always honored (the consolidate step reads a finished
    # .db by path even when the active backend is postgres).
    if db_path is not None:
        return db_path

    if FsScanConfig.DB_BACKEND != "sqlite":
        raise RuntimeError(
            f"get_db_path() is only meaningful for the sqlite backend "
            f"(current backend: {FsScanConfig.DB_BACKEND!r}). "
            "Use get_db_url() instead, or pass an explicit db_path."
        )

    filesystem = filesystem.lower()

    # 2. Check FS_SCAN_DB environment variable
    if "FS_SCAN_DB" in os.environ:
        return Path(os.environ["FS_SCAN_DB"])

    # 3. Default: data_dir / filesystem.db
    return get_data_dir() / f"{filesystem}.db"


def get_db_url(filesystem: str, schema: str | None = None) -> str:
    """Return a connection descriptor for *filesystem* suitable for display.

    SQLite: the .db path string.  PostgreSQL: a URL with the password masked
    and the target schema appended as a fragment for clarity.
    """
    config = FsScanConfig
    if config.DB_BACKEND == "postgres":
        schema = schema or config.pg_schema_name(filesystem)
        return (
            f"postgresql+psycopg2://{config.PG_USER}:***@"
            f"{config.PG_HOST}:{config.PG_PORT}/{config.PG_DB_NAME}#schema={schema}"
        )
    return str(get_db_path(filesystem))


def get_engine(
    filesystem: str,
    echo: bool = False,
    db_path: Path | None = None,
    *,
    schema: str | None = None,
    database: str | None = None,
) -> Engine:
    """Create or retrieve a cached SQLAlchemy engine for a filesystem/collection.

    Engines are cached by backend target. Thread-safe for parallel queries.

    Args:
        filesystem: Filesystem/collection name (e.g., 'asp', 'cisl', 'cgd')
        echo: If True, log all SQL statements (only affects engine creation)
        db_path: Explicit SQLite database path override (sqlite backend only)
        schema: PostgreSQL schema to pin via ``search_path`` (postgres backend
            only). Defaults to ``FsScanConfig.pg_schema_name(filesystem)``;
            the consolidate step passes ``"<collection>_staging"`` here.
        database: PostgreSQL database name to connect to (postgres backend
            only). Defaults to ``FsScanConfig.PG_DB_NAME`` (the ``FS_SCAN_PG_DB``
            env var). Lets a single process query more than one CNPG database on
            the same cluster — e.g. ``campaign`` (Campaign_Store) and ``desc1``
            (Destor) — by passing a per-resource database here. It is part of
            the engine cache key, so engines for different databases never alias.

    Returns:
        SQLAlchemy Engine instance (may be cached)
    """
    config = FsScanConfig

    if config.DB_BACKEND == "postgres":
        config.validate_postgres()
        schema = schema or config.pg_schema_name(filesystem)
        db_name = database or config.PG_DB_NAME
        cache_key = (
            "postgres", config.PG_HOST, config.PG_PORT, config.PG_USER,
            db_name, schema, bool(config.PG_REQUIRE_SSL),
        )
        with _engine_cache_lock:
            if cache_key not in _engine_cache:
                # Pin search_path so the existing bare-table-name SQL resolves
                # to this collection's schema without any rewrite.
                connect_args = {"options": f"-csearch_path={schema},public"}
                if config.PG_REQUIRE_SSL:
                    connect_args["sslmode"] = "require"
                url = (
                    f"postgresql+psycopg2://{config.PG_USER}:{config.PG_PASSWORD}"
                    f"@{config.PG_HOST}:{config.PG_PORT}/{db_name}"
                )
                # pool_pre_ping validates each pooled connection with a cheap
                # liveness check at checkout and transparently reconnects a
                # dropped one — without it, an engine cached for the life of
                # the process (e.g. the webapp's per-collection engines) hands
                # out connections that CNPG/the network silently closed while
                # idle, surfacing as "SSL connection has been closed
                # unexpectedly" on the next query. pool_recycle caps connection
                # age so a very stale one is never reused (checkout-only, so
                # pre_ping does the heavy lifting; this is belt-and-suspenders).
                _engine_cache[cache_key] = create_engine(
                    url, echo=echo, connect_args=connect_args,
                    pool_pre_ping=True, pool_recycle=1800,
                )
            return _engine_cache[cache_key]

    # sqlite (default) — behavior unchanged from the single-backend era.
    resolved_path = get_db_path(filesystem, db_path)
    cache_key = ("sqlite", str(resolved_path))

    with _engine_cache_lock:
        if cache_key not in _engine_cache:
            # Ensure parent directory exists
            resolved_path.parent.mkdir(parents=True, exist_ok=True)
            _engine_cache[cache_key] = create_engine(
                f"sqlite:///{resolved_path}",
                echo=echo,
                connect_args={"check_same_thread": False},  # Thread safety for parallel queries
            )
        return _engine_cache[cache_key]


def clear_engine_cache() -> None:
    """Clear the engine cache. Primarily for testing.

    Disposes all cached engines before clearing to release connections.
    """
    with _engine_cache_lock:
        for engine in _engine_cache.values():
            engine.dispose()
        _engine_cache.clear()


def get_session(filesystem: str, engine=None, db_path: Path | None = None, *, schema: str | None = None, database: str | None = None):
    """Create and return a new database session for a filesystem/collection.

    Args:
        filesystem: Filesystem/collection name (e.g., 'asp', 'cisl', 'cgd')
        engine: Existing engine to use. If None, creates a new one.
        db_path: Explicit SQLite database path override (ignored if engine provided)
        schema: PostgreSQL schema to pin (ignored if engine provided)
        database: PostgreSQL database name (ignored if engine provided); defaults
            to ``FsScanConfig.PG_DB_NAME``. See :func:`get_engine`.

    Returns:
        SQLAlchemy Session instance
    """
    if engine is None:
        engine = get_engine(filesystem, db_path=db_path, schema=schema, database=database)

    Session = sessionmaker(bind=engine)
    return Session()


def db_available(filesystem: str) -> bool:
    """Return True if a database is reachable for *filesystem*.

    SQLite: the .db file exists.  PostgreSQL: credentials validate and a test
    connection (``SELECT 1``) succeeds, so a down server is reported as
    unavailable rather than raising.
    """
    try:
        if FsScanConfig.DB_BACKEND == "postgres":
            FsScanConfig.validate_postgres()
            engine = get_engine(filesystem)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            return True
        return get_db_path(filesystem).exists()
    except Exception:
        return False


# Schemas that are never user collections.
_PG_SYSTEM_SCHEMAS = {"public", "information_schema"}


def list_pg_schemas(database: str | None = None) -> list[str]:
    """Return the collection schemas in the PostgreSQL database.

    Excludes system schemas (``public``, ``information_schema``, ``pg_*``) and
    the transient ``*_staging`` / ``*_old`` schemas used during a swap.

    ``database`` selects which CNPG database to introspect (defaults to
    ``FsScanConfig.PG_DB_NAME``); pass e.g. ``"desc1"`` to discover Destor's
    collections separately from ``campaign``.
    """
    engine = get_engine("__discovery__", schema="public", database=database)
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT schema_name FROM information_schema.schemata")
        ).fetchall()
    return sorted(
        name
        for (name,) in rows
        if name not in _PG_SYSTEM_SCHEMAS
        and not name.startswith("pg_")
        and not name.endswith("_staging")
        and not name.endswith("_old")
    )


def filesystem_available(filesystem: str) -> bool:
    """Backend-aware existence check for a single collection.

    SQLite: the .db file exists.  PostgreSQL: a schema of that name exists.
    """
    if FsScanConfig.DB_BACKEND == "postgres":
        return filesystem.lower() in list_pg_schemas()
    return get_db_path(filesystem).exists()


def describe_databases() -> list[tuple[str, str, int | None]]:
    """Describe available collections for ``--show-config`` display.

    Returns a list of ``(name, location, size_bytes_or_None)`` tuples.  Size is
    the .db file size for SQLite and ``None`` for PostgreSQL (not file-backed).
    """
    from ..queries.query_engine import get_all_filesystems

    out: list[tuple[str, str, int | None]] = []
    for fs in get_all_filesystems():
        if FsScanConfig.DB_BACKEND == "postgres":
            out.append((fs, get_db_url(fs), None))
        else:
            path = get_db_path(fs)
            out.append((fs, str(path), path.stat().st_size if path.exists() else 0))
    return out


def _ensure_pg_database() -> None:
    """Create the PostgreSQL database (``FsScanConfig.PG_DB_NAME``) if missing.

    Connects to the ``postgres`` maintenance database with AUTOCOMMIT so that
    ``CREATE DATABASE`` can run outside a transaction.
    """
    config = FsScanConfig
    db_name = config.PG_DB_NAME
    admin_url = (
        f"postgresql+psycopg2://{config.PG_USER}:{config.PG_PASSWORD}"
        f"@{config.PG_HOST}:{config.PG_PORT}/postgres"
    )
    admin_engine = create_engine(admin_url, isolation_level="AUTOCOMMIT")
    try:
        with admin_engine.connect() as conn:
            result = conn.execute(
                text("SELECT 1 FROM pg_database WHERE datname = :db"),
                {"db": db_name},
            )
            if not result.fetchone():
                conn.execute(text(f'CREATE DATABASE "{db_name}"'))
    finally:
        admin_engine.dispose()


def init_db(filesystem: str, echo: bool = False, db_path: Path | None = None, *, schema: str | None = None):
    """Initialize a database by creating all tables.

    For the PostgreSQL backend this also creates the target database (once) and
    the target schema if they do not already exist; tables are created inside
    that schema (resolved via the engine's ``search_path``).

    Args:
        filesystem: Filesystem/collection name
        echo: If True, log all SQL statements
        db_path: Explicit SQLite database path override (sqlite backend only)
        schema: PostgreSQL schema to create tables in (postgres backend only)

    Returns:
        SQLAlchemy Engine instance
    """
    config = FsScanConfig
    if config.DB_BACKEND == "postgres":
        config.validate_postgres()
        schema = schema or config.pg_schema_name(filesystem)
        _ensure_pg_database()
        engine = get_engine(filesystem, echo=echo, schema=schema)
        with engine.begin() as conn:
            conn.execute(text(f'CREATE SCHEMA IF NOT EXISTS "{schema}"'))
        Base.metadata.create_all(engine)
        return engine

    engine = get_engine(filesystem, echo=echo, db_path=db_path)
    Base.metadata.create_all(engine)
    return engine


def drop_tables(filesystem: str, echo: bool = False, db_path: Path | None = None, *, schema: str | None = None):
    """Drop all tables in the database (within the target schema for postgres).

    Args:
        filesystem: Filesystem/collection name
        echo: If True, log all SQL statements
        db_path: Explicit SQLite database path override (sqlite backend only)
        schema: PostgreSQL schema to drop tables from (postgres backend only)

    Returns:
        SQLAlchemy Engine instance
    """
    engine = get_engine(filesystem, echo=echo, db_path=db_path, schema=schema)
    Base.metadata.drop_all(engine)
    return engine
