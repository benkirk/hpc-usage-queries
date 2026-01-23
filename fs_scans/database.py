"""Database connection and session management for GPFS scan data."""

import os
import re
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base

# Default database directory (same as where the log files live)
DATA_DIR = Path(__file__).parent


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


def get_db_path(filesystem: str) -> Path:
    """Get the database path for a specific filesystem.

    Args:
        filesystem: Filesystem name (e.g., 'asp', 'cisl', 'eol', 'hao')

    Returns:
        Path to the SQLite database file
    """
    filesystem = filesystem.lower()

    # Allow override via environment variable
    env_var = f"GPFS_{filesystem.upper()}_DB"
    if env_var in os.environ:
        return Path(os.environ[env_var])

    return DATA_DIR / f"{filesystem}.db"


def get_engine(filesystem: str, echo: bool = False):
    """Create and return a SQLAlchemy engine for a specific filesystem.

    Args:
        filesystem: Filesystem name (e.g., 'asp', 'cisl')
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    db_path = get_db_path(filesystem)

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return create_engine(f"sqlite:///{db_path}", echo=echo)


def get_session(filesystem: str, engine=None):
    """Create and return a new database session for a specific filesystem.

    Args:
        filesystem: Filesystem name (e.g., 'asp', 'cisl')
        engine: Existing engine to use. If None, creates a new one.

    Returns:
        SQLAlchemy Session instance
    """
    if engine is None:
        engine = get_engine(filesystem)

    Session = sessionmaker(bind=engine)
    return Session()


def init_db(filesystem: str, echo: bool = False):
    """Initialize database by creating all tables.

    Args:
        filesystem: Filesystem name
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    engine = get_engine(filesystem, echo=echo)
    Base.metadata.create_all(engine)
    return engine


def drop_tables(filesystem: str, echo: bool = False):
    """Drop all tables in the database.

    Args:
        filesystem: Filesystem name
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    engine = get_engine(filesystem, echo=echo)
    Base.metadata.drop_all(engine)
    return engine
