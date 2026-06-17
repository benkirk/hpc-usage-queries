"""Configuration for fs_scans database connections.

All env-var reading is centralised here.  ``load_dotenv()`` is called at import
time so the class attrs below pick up values from a ``.env`` file if present.

Supported backends:
  sqlite   (default) — per-collection .db files under the data directory
  postgres           — one database per filesystem on a shared PostgreSQL
                       (CNPG) server, with one schema per collection

The SQLite data-directory precedence (CLI ``--data-dir`` → ``FS_SCAN_DATA_DIR``
→ default) is owned by ``fs_scans/core/database.py`` (``get_data_dir()``); this
module only handles backend selection and the PostgreSQL connection settings.

Quickstart:
  Copy .env.example → .env and set FS_SCAN_DB_BACKEND plus the appropriate vars.
"""

import os

from dotenv import find_dotenv, load_dotenv

# Load .env on import.  Calling this multiple times is harmless.
load_dotenv(find_dotenv())


class FsScanConfig:
    # ------------------------------------------------------------ Backend
    # "sqlite" or "postgres"
    DB_BACKEND = os.getenv("FS_SCAN_DB_BACKEND", "sqlite").lower()

    # ---------------------------------------------------------- PostgreSQL
    PG_HOST = os.getenv("FS_SCAN_PG_HOST", "localhost")
    PG_PORT = int(os.getenv("FS_SCAN_PG_PORT", "5432"))
    PG_USER = os.getenv("FS_SCAN_PG_USER", "postgres")
    PG_PASSWORD = os.getenv("FS_SCAN_PG_PASSWORD", "")
    PG_REQUIRE_SSL = os.getenv("FS_SCAN_PG_REQUIRE_SSL", "false").lower() in ("true", "1", "yes")

    # The PostgreSQL database name (one database per *filesystem*; the default
    # initial target is the /glade/campaign/ filesystem).  Collections live as
    # schemas inside this database.
    PG_DB_NAME = os.getenv("FS_SCAN_PG_DB", "campaign")

    # --------------------------------------------------- Per-collection schema
    @classmethod
    def pg_schema_name(cls, collection: str) -> str:
        """Return the PostgreSQL schema name for *collection*.

        Defaults to the lower-cased collection name (e.g. ``cgd``).
        Override per-collection via ``FS_SCAN_PG_{COLLECTION}_SCHEMA``.
        """
        collection = collection.lower()
        env_var = f"FS_SCAN_PG_{collection.upper()}_SCHEMA"
        return os.getenv(env_var, collection)

    # ------------------------------------------------------------ Validate
    @classmethod
    def validate_postgres(cls):
        """Fail fast if the postgres backend is selected but credentials are missing."""
        required = {
            "FS_SCAN_PG_HOST": cls.PG_HOST,
            "FS_SCAN_PG_USER": cls.PG_USER,
            "FS_SCAN_PG_PASSWORD": cls.PG_PASSWORD,
        }
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise EnvironmentError(
                "Missing required environment variables for postgres backend:\n"
                + "".join(f"  {k}\n" for k in missing)
                + "\nSee .env.example for a template."
            )
