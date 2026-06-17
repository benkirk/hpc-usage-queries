"""SQLite → PostgreSQL consolidation for fs_scans.

Loads a finished per-collection SQLite ``.db`` into a PostgreSQL schema and
atomically swaps it into place, so the weekly scan data can be served from a
networked database without touching the fast SQLite generation pipeline.
"""

from .consolidator import consolidate_sqlite_to_postgres

__all__ = ["consolidate_sqlite_to_postgres"]
