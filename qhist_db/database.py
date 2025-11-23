"""Database connection and session management."""

import os
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker

from .models import Base
from .charging import get_view_sql

# Default database directory
DATA_DIR = Path(__file__).parent.parent / "data"

# Valid machine names
VALID_MACHINES = {"casper", "derecho"}


def get_db_path(machine: str) -> Path:
    """Get the database path for a specific machine.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        Path to the SQLite database file
    """
    machine = machine.lower()
    if machine not in VALID_MACHINES:
        raise ValueError(f"Unknown machine: {machine}. Must be one of: {VALID_MACHINES}")

    # Allow override via environment variable
    env_var = f"QHIST_{machine.upper()}_DB"
    if env_var in os.environ:
        return Path(os.environ[env_var])

    return DATA_DIR / f"{machine}.db"


def get_engine(machine: str, echo: bool = False):
    """Create and return a SQLAlchemy engine for a specific machine.

    Args:
        machine: Machine name ('casper' or 'derecho')
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    db_path = get_db_path(machine)

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return create_engine(f"sqlite:///{db_path}", echo=echo)


def get_session(machine: str, engine=None):
    """Create and return a new database session for a specific machine.

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


def create_views(engine, machine: str):
    """Create charging views for a specific machine.

    Args:
        engine: SQLAlchemy engine
        machine: Machine name ('casper' or 'derecho')
    """
    view_sql = get_view_sql(machine)
    with engine.connect() as conn:
        # Drop existing view first (SQLite doesn't support CREATE OR REPLACE VIEW)
        conn.execute(text("DROP VIEW IF EXISTS v_jobs_charged"))
        conn.execute(text(view_sql))
        conn.commit()


def init_db(machine: str | None = None, echo: bool = False):
    """Initialize database(s) by creating all tables and views.

    Args:
        machine: Machine name, or None to initialize all machines
        echo: If True, log all SQL statements

    Returns:
        Engine instance (if single machine) or dict of engines (if all)
    """
    if machine is not None:
        engine = get_engine(machine, echo=echo)
        Base.metadata.create_all(engine)
        create_views(engine, machine)
        return engine

    # Initialize all machines
    engines = {}
    for m in VALID_MACHINES:
        engines[m] = get_engine(m, echo=echo)
        Base.metadata.create_all(engines[m])
        create_views(engines[m], m)
    return engines
