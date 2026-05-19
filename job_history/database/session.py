"""Database connection and session management.

Supports two backends, selected by the ``JOB_HISTORY_DB_BACKEND`` environment variable
(or a ``.env`` file loaded via python-dotenv):

  sqlite   (default) — individual .db files under ``SQLITE_DATA_DIR``
  postgres           — shared PostgreSQL server with per-machine databases

See ``job_history/config.py`` and ``.env.example`` for full configuration details.
"""

import threading
from typing import Any, Mapping, Optional

from sqlalchemy import create_engine, event, text
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from .config import JobHistoryConfig
from .models import Base

# Valid machine names
VALID_MACHINES = {"casper", "derecho"}

# Backward-compat alias: code that imported JOB_HISTORY_DATA_DIR directly still works.
JOB_HISTORY_DATA_DIR = JobHistoryConfig.SQLITE_DATA_DIR

# Engine cache — populated by get_engine(), cleared by clear_engine_cache().
# Long-lived consumers (Flask webapps, daemons) reuse a single Engine per
# (backend-target, pool_kwargs) so connection pools amortize across requests.
_engine_cache: dict[tuple, Engine] = {}
_engine_cache_lock = threading.Lock()


# ---------------------------------------------------------------------------
# SQLite helpers
# ---------------------------------------------------------------------------

def _set_sqlite_pragma(dbapi_conn, connection_record):
    """Configure SQLite for optimal performance.

    Registered per-engine (not globally) inside get_engine() so that it only
    fires on SQLite connections and never on other database engines (e.g. PostgreSQL).

    - WAL mode: Allows concurrent readers during writes
    - synchronous=NORMAL: Faster writes with acceptable durability
    - cache_size: 64MB cache for better query performance
    - temp_store: Keep temporary tables in memory
    - mmap_size: 256MB memory-mapped I/O for faster reads
    - foreign_keys: Enable foreign key constraints
    """
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA journal_mode=WAL")
    cursor.execute("PRAGMA synchronous=NORMAL")
    cursor.execute("PRAGMA cache_size=-64000")  # Negative = kibibytes
    cursor.execute("PRAGMA temp_store=MEMORY")
    cursor.execute("PRAGMA mmap_size=268435456")  # 256MB
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


# Keep the old name as an alias so callers that imported it directly still work.
set_sqlite_pragma = _set_sqlite_pragma


def get_db_path(machine: str):
    """Return the SQLite database path for *machine*.

    Raises ``RuntimeError`` when the active backend is not SQLite — use
    ``get_db_url()`` for a backend-agnostic connection descriptor instead.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        Path to the SQLite database file
    """
    if JobHistoryConfig.DB_BACKEND != "sqlite":
        raise RuntimeError(
            f"get_db_path() is only meaningful for the sqlite backend "
            f"(current backend: {JobHistoryConfig.DB_BACKEND!r}). "
            "Use get_db_url() instead."
        )

    from pathlib import Path

    machine = machine.lower()
    if machine not in VALID_MACHINES:
        raise ValueError(f"Unknown machine: {machine}. Must be one of: {VALID_MACHINES}")

    # Allow per-machine override
    import os
    env_var = f"QHIST_{machine.upper()}_DB"
    if env_var in os.environ:
        return Path(os.environ[env_var])

    return JobHistoryConfig.SQLITE_DATA_DIR / f"{machine}.db"


# ---------------------------------------------------------------------------
# Backend-agnostic helpers
# ---------------------------------------------------------------------------

def get_db_url(machine: str) -> str:
    """Return the connection URL (or path string) for *machine*'s database.

    Suitable for display in verbose CLI output for any backend.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        SQLite path string or PostgreSQL URL string
    """
    machine = machine.lower()
    if machine not in VALID_MACHINES:
        raise ValueError(f"Unknown machine: {machine}. Must be one of: {VALID_MACHINES}")

    config = JobHistoryConfig
    if config.DB_BACKEND == "postgres":
        db_name = config.pg_db_name(machine)
        return f"postgresql+psycopg2://{config.PG_USER}:***@{config.PG_HOST}:{config.PG_PORT}/{db_name}"
    else:
        return str(get_db_path(machine))


def db_available(machine: str) -> bool:
    """Return True if a jobhist database is available for the given machine.

    For the SQLite backend this checks that the .db file exists.
    For the PostgreSQL backend this validates credentials AND opens a test
    connection (SELECT 1), so transient network failures or a down server
    are correctly reported as unavailable.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        True if the database is accessible, False otherwise
    """
    if machine not in VALID_MACHINES:
        return False
    try:
        if JobHistoryConfig.DB_BACKEND == 'postgres':
            JobHistoryConfig.validate_postgres()
            engine = get_engine(machine)
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
        else:
            return get_db_path(machine).exists()
        return True
    except Exception:
        return False


def _engine_cache_key(machine: str, pool_kwargs: Optional[Mapping[str, Any]]) -> tuple:
    """Build a hashable cache key that captures backend target + pool config.

    A change to ``JobHistoryConfig.DB_BACKEND``, the PostgreSQL host/db, the
    SQLite path, or any ``pool_kwargs`` value produces a distinct key so the
    cache returns the right engine when configuration changes between calls
    (primarily relevant in tests and multi-tenant code).
    """
    cfg = JobHistoryConfig
    if cfg.DB_BACKEND == "postgres":
        target: tuple = (
            "postgres",
            cfg.PG_HOST, cfg.PG_PORT, cfg.PG_USER,
            cfg.pg_db_name(machine),
            bool(cfg.PG_REQUIRE_SSL),
        )
    else:
        target = ("sqlite", str(get_db_path(machine)))
    pk = frozenset((pool_kwargs or {}).items())
    return (target, pk)


def get_engine(
    machine: str,
    echo: bool = False,
    *,
    pool_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Return a SQLAlchemy engine for *machine*, creating and caching it on first use.

    Engines are memoized by ``(backend-target, pool_kwargs)`` so repeated calls
    from long-lived processes (Flask webapps, daemons) reuse one Engine and its
    connection pool. CLI usage that creates a fresh engine per command still
    works — the cache just makes the second call cheap.

    Args:
        machine: Machine name ('casper' or 'derecho')
        echo: If True, log all SQL statements (only honored on first creation
            for a given cache key)
        pool_kwargs: Optional mapping of ``create_engine`` pool parameters
            (e.g. ``pool_size``, ``max_overflow``, ``pool_pre_ping``,
            ``pool_recycle``). Forwarded verbatim to ``create_engine`` —
            primarily useful for the PostgreSQL backend; SQLite ignores most
            pool parameters.

    Returns:
        SQLAlchemy Engine instance (cached after first creation)
    """
    machine = machine.lower()
    if machine not in VALID_MACHINES:
        raise ValueError(f"Unknown machine: {machine}. Must be one of: {VALID_MACHINES}")

    config = JobHistoryConfig
    key = _engine_cache_key(machine, pool_kwargs)

    with _engine_cache_lock:
        cached = _engine_cache.get(key)
        if cached is not None:
            return cached

        extra: dict[str, Any] = dict(pool_kwargs or {})

        if config.DB_BACKEND == "postgres":
            config.validate_postgres()
            db_name = config.pg_db_name(machine)
            connect_args = {}
            if config.PG_REQUIRE_SSL:
                connect_args["sslmode"] = "require"
            url = (
                f"postgresql+psycopg2://{config.PG_USER}:{config.PG_PASSWORD}"
                f"@{config.PG_HOST}:{config.PG_PORT}/{db_name}"
            )
            engine = create_engine(url, echo=echo, connect_args=connect_args, **extra)
        else:
            db_path = get_db_path(machine)
            db_path.parent.mkdir(parents=True, exist_ok=True)
            engine = create_engine(f"sqlite:///{db_path}", echo=echo, **extra)
            event.listen(engine, "connect", _set_sqlite_pragma)

        _engine_cache[key] = engine
        return engine


def clear_engine_cache() -> None:
    """Dispose all cached engines and clear the cache.

    Primarily for tests and graceful shutdown. After clearing, the next call
    to ``get_engine()`` will create a fresh engine.
    """
    with _engine_cache_lock:
        for engine in _engine_cache.values():
            engine.dispose()
        _engine_cache.clear()


def get_session(
    machine: str,
    engine=None,
    *,
    pool_kwargs: Optional[Mapping[str, Any]] = None,
):
    """Create and return a new database session for *machine*.

    Args:
        machine: Machine name ('casper' or 'derecho')
        engine: Existing engine to use. If None, retrieves (or creates) the
            cached engine for *machine*.
        pool_kwargs: Forwarded to ``get_engine`` on cache miss; ignored when
            *engine* is provided.

    Returns:
        SQLAlchemy Session instance
    """
    if engine is None:
        engine = get_engine(machine, pool_kwargs=pool_kwargs)

    Session = sessionmaker(bind=engine)
    return Session()


# ---------------------------------------------------------------------------
# Database initialisation
# ---------------------------------------------------------------------------

def _ensure_pg_database(machine: str, config: type) -> None:
    """Create the PostgreSQL database for *machine* if it does not exist.

    Connects to the ``postgres`` maintenance database with AUTOCOMMIT so that
    ``CREATE DATABASE`` can run outside a transaction.
    """
    db_name = config.pg_db_name(machine)
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
                conn.execute(text(f"CREATE DATABASE {db_name}"))
    finally:
        admin_engine.dispose()


def _ensure_db_triggers(engine) -> None:
    """Create database triggers that enforce the 1:1 jobs ↔ job_charges invariant.

    After any INSERT on the ``jobs`` table a zero-value placeholder row is
    immediately inserted into ``job_charges`` (charge_version=0 signals a
    placeholder; the sync code overwrites it with version=1 after calculation).

    This prevents the historical situation where jobs were bulk-loaded without
    corresponding charge records, which caused daily_summary to be nearly empty.

    Idempotent: safe to call on every startup (uses CREATE OR REPLACE / IF NOT
    EXISTS semantics).
    """
    dialect = engine.dialect.name

    with engine.connect() as conn:
        if dialect == "postgresql":
            conn.execute(text("""
                CREATE OR REPLACE FUNCTION fn_ensure_job_charge()
                RETURNS TRIGGER LANGUAGE plpgsql AS $$
                BEGIN
                    INSERT INTO job_charges
                        (job_id, cpu_hours, gpu_hours, memory_hours, qos_factor, charge_version)
                    VALUES (NEW.id, 0.0, 0.0, 0.0, 1.0, 0)
                    ON CONFLICT (job_id) DO NOTHING;
                    RETURN NEW;
                END;
                $$;
            """))
            conn.execute(text("""
                DROP TRIGGER IF EXISTS trg_ensure_job_charge ON jobs;
            """))
            conn.execute(text("""
                CREATE TRIGGER trg_ensure_job_charge
                AFTER INSERT ON jobs
                FOR EACH ROW
                EXECUTE FUNCTION fn_ensure_job_charge();
            """))
        else:  # SQLite
            conn.execute(text("""
                CREATE TRIGGER IF NOT EXISTS trg_ensure_job_charge
                AFTER INSERT ON jobs
                BEGIN
                    INSERT OR IGNORE INTO job_charges
                        (job_id, cpu_hours, gpu_hours, memory_hours, qos_factor, charge_version)
                    VALUES (NEW.id, 0.0, 0.0, 0.0, 1.0, 0);
                END;
            """))
        conn.commit()


def init_db(machine: str | None = None, echo: bool = False):
    """Initialize database(s) by creating all tables.

    For the PostgreSQL backend this also creates the per-machine database on
    the server if it does not already exist.

    Args:
        machine: Machine name, or None to initialize all machines
        echo: If True, log all SQL statements

    Returns:
        Engine instance (if single machine) or dict of engines (if all)
    """
    config = JobHistoryConfig

    if machine is not None:
        if config.DB_BACKEND == "postgres":
            _ensure_pg_database(machine, config)
        engine = get_engine(machine, echo=echo)
        Base.metadata.create_all(engine)
        _ensure_db_triggers(engine)
        return engine

    # Initialize all machines
    engines = {}
    for m in VALID_MACHINES:
        if config.DB_BACKEND == "postgres":
            _ensure_pg_database(m, config)
        engines[m] = get_engine(m, echo=echo)
        Base.metadata.create_all(engines[m])
        _ensure_db_triggers(engines[m])
    return engines
