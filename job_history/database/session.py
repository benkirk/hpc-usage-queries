"""Database connection and session management.

Supports two backends, selected by the ``JH_DB_BACKEND`` environment variable
(or a ``.env`` file loaded via python-dotenv):

  sqlite   (default) — individual .db files under ``SQLITE_DATA_DIR``
  postgres           — shared PostgreSQL server with per-machine databases

See ``job_history/config.py`` and ``.env.example`` for full configuration details.
"""

from sqlalchemy import create_engine, event, text
from sqlalchemy.orm import sessionmaker

from .config import JobHistoryConfig
from .models import Base

# Valid machine names
VALID_MACHINES = {"casper", "derecho"}

# Backward-compat alias: code that imported JOB_HISTORY_DATA_DIR directly still works.
JOB_HISTORY_DATA_DIR = JobHistoryConfig.SQLITE_DATA_DIR


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
            try:
                with engine.connect() as conn:
                    conn.execute(text("SELECT 1"))
            finally:
                engine.dispose()
        else:
            return get_db_path(machine).exists()
        return True
    except Exception:
        return False


def get_engine(machine: str, echo: bool = False):
    """Create and return a SQLAlchemy engine for *machine*.

    The backend (SQLite or PostgreSQL) is determined by ``JobHistoryConfig.DB_BACKEND``.

    Args:
        machine: Machine name ('casper' or 'derecho')
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    machine = machine.lower()
    if machine not in VALID_MACHINES:
        raise ValueError(f"Unknown machine: {machine}. Must be one of: {VALID_MACHINES}")

    config = JobHistoryConfig

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
        return create_engine(url, echo=echo, connect_args=connect_args)

    # SQLite (default)
    db_path = get_db_path(machine)
    db_path.parent.mkdir(parents=True, exist_ok=True)
    engine = create_engine(f"sqlite:///{db_path}", echo=echo)
    event.listen(engine, "connect", _set_sqlite_pragma)
    return engine


def get_session(machine: str, engine=None):
    """Create and return a new database session for *machine*.

    Args:
        machine: Machine name ('casper' or 'derecho')
        engine: Existing engine to use. If None, creates a new one.

    Returns:
        SQLAlchemy Session instance
    """
    if engine is None:
        engine = get_engine(machine)

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
        return engine

    # Initialize all machines
    engines = {}
    for m in VALID_MACHINES:
        if config.DB_BACKEND == "postgres":
            _ensure_pg_database(m, config)
        engines[m] = get_engine(m, echo=echo)
        Base.metadata.create_all(engines[m])
    return engines
