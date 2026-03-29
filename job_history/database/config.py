"""Configuration for job_history database connections.

All env-var reading is centralised here.  Call load_dotenv() at import time
so the class attrs below pick up values from a .env file if present.

Supported backends:
  sqlite   (default) — per-machine .db files in SQLITE_DATA_DIR
  postgres           — per-machine databases on a shared PostgreSQL server

Quickstart:
  Copy .env.example → .env and set JOB_HISTORY_DB_BACKEND plus the appropriate vars.
"""

import os
from pathlib import Path

from dotenv import find_dotenv, load_dotenv

# Load .env on import.  Calling this multiple times is harmless.
load_dotenv(find_dotenv())

# Default SQLite data directory (relative to project root)
_DEFAULT_DATA_DIR = Path(__file__).parent.parent.parent / "data"


class JobHistoryConfig:
    # ------------------------------------------------------------ Backend
    # "sqlite" or "postgres"
    DB_BACKEND = os.getenv("JOB_HISTORY_DB_BACKEND", "sqlite").lower()

    # ------------------------------------------------------------ SQLite
    SQLITE_DATA_DIR = Path(os.getenv("JOB_HISTORY_DATA_DIR", _DEFAULT_DATA_DIR))

    # ---------------------------------------------------------- PostgreSQL
    PG_HOST = os.getenv("JOB_HISTORY_PG_HOST", "localhost")
    PG_PORT = int(os.getenv("JOB_HISTORY_PG_PORT", "5432"))
    PG_USER = os.getenv("JOB_HISTORY_PG_USER", "postgres")
    PG_PASSWORD = os.getenv("JOB_HISTORY_PG_PASSWORD", "")
    PG_REQUIRE_SSL = os.getenv("JOB_HISTORY_PG_REQUIRE_SSL", "false").lower() in ("true", "1", "yes")

    # ------------------------------------------------- Per-machine DB names
    @classmethod
    def pg_db_name(cls, machine: str) -> str:
        """Return the PostgreSQL database name for *machine*.

        Defaults to ``{machine}_jobs`` (e.g. ``derecho_jobs``).
        Override per-machine via ``JOB_HISTORY_PG_{MACHINE}_DB`` environment variable.
        """
        env_var = f"JOB_HISTORY_PG_{machine.upper()}_DB"
        return os.getenv(env_var, f"{machine}_jobs")

    # --------------------------------------------------- Site timezone
    # Used to determine day boundaries for daily_summary generation and
    # for --recalculate date-range queries.  Must be a valid IANA timezone
    # name (e.g. "America/Denver", "America/New_York", "UTC").
    SITE_TIMEZONE = os.getenv("JOB_HISTORY_SITE_TIMEZONE", "America/Denver")

    # ------------------------------------------------------------ Validate
    @classmethod
    def validate_postgres(cls):
        """Fail fast at startup if postgres backend is selected but credentials missing."""
        required = {
            "JOB_HISTORY_PG_HOST": cls.PG_HOST,
            "JOB_HISTORY_PG_USER": cls.PG_USER,
            "JOB_HISTORY_PG_PASSWORD": cls.PG_PASSWORD,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise EnvironmentError(
                "Missing required environment variables for postgres backend:\n"
                + "".join(f"  {k}\n" for k in missing)
                + "\nSee .env.example for a template."
            )
