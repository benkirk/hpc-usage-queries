"""Database connection and session management for GPFS scan data."""

import os
import re
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base

# Default database directory (same as where the module lives)
_DEFAULT_DATA_DIR = Path(__file__).parent

# Module-level cache for the configured data directory (set via CLI)
_data_dir_override: Path | None = None


def get_data_dir() -> Path:
    """Get the current data directory for filesystem databases.

    Precedence:
        1. Module-level override (set via set_data_dir() from CLI)
        2. FS_SCAN_DATA_DIR environment variable
        3. Default: module directory

    Returns:
        Path to the data directory
    """
    if _data_dir_override is not None:
        return _data_dir_override

    env_dir = os.environ.get("FS_SCAN_DATA_DIR")
    if env_dir:
        return Path(env_dir)

    return _DEFAULT_DATA_DIR


def set_data_dir(path: Path | None) -> None:
    """Set the data directory override (typically from CLI --data-dir).

    Args:
        path: Path to use as data directory, or None to clear override
    """
    global _data_dir_override
    _data_dir_override = path


def extract_filesystem_from_filename(filename: str) -> str | None:
    """Extract filesystem name from a GPFS scan log filename.

    Expected patterns:
        20260111_csfs1_asp.list.list_all.log -> asp
        20260111_csfs1_cisl.list.list_all.log.xz -> cisl

    Args:
        filename: Name of the log file (with or without path)

    Returns:
        Filesystem name or None if pattern doesn't match
    """
    basename = Path(filename).name
    # Pattern: YYYYMMDD_server_filesystem.list...
    match = re.match(r"\d{8}_[^_]+_([^.]+)\.list", basename)
    if match:
        return match.group(1)
    return None


def get_db_path(filesystem: str, db_path: Path | None = None) -> Path:
    """Get the database path for a specific filesystem.

    Precedence for determining database path:
        1. Explicit db_path argument (highest priority - from CLI --db)
        2. FS_SCAN_DB environment variable
        3. get_data_dir() / f"{filesystem}.db" (default)

    Args:
        filesystem: Filesystem name (e.g., 'asp', 'cisl', 'eol', 'hao')
        db_path: Explicit database path override (typically from CLI --db option)

    Returns:
        Path to the SQLite database file
    """
    # 1. Explicit path takes highest precedence
    if db_path is not None:
        return db_path

    filesystem = filesystem.lower()

    # 2. Check FS_SCAN_DB environment variable
    if "FS_SCAN_DB" in os.environ:
        return Path(os.environ["FS_SCAN_DB"])

    # 3. Default: data_dir / filesystem.db
    return get_data_dir() / f"{filesystem}.db"


def get_engine(filesystem: str, echo: bool = False, db_path: Path | None = None):
    """Create and return a SQLAlchemy engine for a specific filesystem.

    Args:
        filesystem: Filesystem name (e.g., 'asp', 'cisl')
        echo: If True, log all SQL statements
        db_path: Explicit database path override

    Returns:
        SQLAlchemy Engine instance
    """
    resolved_path = get_db_path(filesystem, db_path)

    # Ensure parent directory exists
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    return create_engine(f"sqlite:///{resolved_path}", echo=echo)


def get_session(filesystem: str, engine=None, db_path: Path | None = None):
    """Create and return a new database session for a specific filesystem.

    Args:
        filesystem: Filesystem name (e.g., 'asp', 'cisl')
        engine: Existing engine to use. If None, creates a new one.
        db_path: Explicit database path override (ignored if engine provided)

    Returns:
        SQLAlchemy Session instance
    """
    if engine is None:
        engine = get_engine(filesystem, db_path=db_path)

    Session = sessionmaker(bind=engine)
    return Session()


def init_db(filesystem: str, echo: bool = False, db_path: Path | None = None):
    """Initialize database by creating all tables.

    Args:
        filesystem: Filesystem name
        echo: If True, log all SQL statements
        db_path: Explicit database path override

    Returns:
        SQLAlchemy Engine instance
    """
    engine = get_engine(filesystem, echo=echo, db_path=db_path)
    Base.metadata.create_all(engine)
    return engine


def drop_tables(filesystem: str, echo: bool = False, db_path: Path | None = None):
    """Drop all tables in the database.

    Args:
        filesystem: Filesystem name
        echo: If True, log all SQL statements
        db_path: Explicit database path override

    Returns:
        SQLAlchemy Engine instance
    """
    engine = get_engine(filesystem, echo=echo, db_path=db_path)
    Base.metadata.drop_all(engine)
    return engine
