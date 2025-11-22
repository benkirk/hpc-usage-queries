"""Database connection and session management."""

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from .models import Base

# Default database path relative to project root
DEFAULT_DB_PATH = Path(__file__).parent.parent / "data" / "qhist.db"


def get_engine(db_path: str | Path | None = None, echo: bool = False):
    """Create and return a SQLAlchemy engine.

    Args:
        db_path: Path to SQLite database file. Defaults to data/qhist.db
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    if db_path is None:
        db_path = os.environ.get("QHIST_DB_PATH", DEFAULT_DB_PATH)

    db_path = Path(db_path)

    # Ensure parent directory exists
    db_path.parent.mkdir(parents=True, exist_ok=True)

    return create_engine(f"sqlite:///{db_path}", echo=echo)


def get_session(engine=None, db_path: str | Path | None = None):
    """Create and return a new database session.

    Args:
        engine: Existing engine to use. If None, creates a new one.
        db_path: Path to database (only used if engine is None)

    Returns:
        SQLAlchemy Session instance
    """
    if engine is None:
        engine = get_engine(db_path)

    Session = sessionmaker(bind=engine)
    return Session()


def init_db(db_path: str | Path | None = None, echo: bool = False):
    """Initialize the database by creating all tables.

    Args:
        db_path: Path to SQLite database file
        echo: If True, log all SQL statements

    Returns:
        SQLAlchemy Engine instance
    """
    engine = get_engine(db_path, echo=echo)
    Base.metadata.create_all(engine)
    return engine
