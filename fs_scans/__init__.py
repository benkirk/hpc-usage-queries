"""fs_scans — filesystem metadata analysis (GPFS/Lustre scans).

Public import surface for programmatic use. External Python applications can::

    from fs_scans import FsScanQueries

    q = FsScanQueries(filesystems="all")        # or ["asp"], or "cgd"
    rows = q.owner_summary(limit=20)             # list[dict]
    dirs = q.list_directories(min_depth=4)       # list[dict]
    hist = q.access_history()                    # dict of histogram buckets

``FsScanQueries`` is the single source of truth shared with the ``fs-scans``
CLI; it is backend-agnostic (SQLite ``*.db`` files or the CNPG/PostgreSQL
backend, selected via ``FS_SCAN_DB_BACKEND``). This mirrors how ``job_history``
exposes :class:`~job_history.queries.JobQueries`.
"""

from .core.config import FsScanConfig
from .core.database import (
    clear_engine_cache,
    describe_databases,
    filesystem_available,
    get_data_dir,
    get_db_path,
    get_db_url,
    get_engine,
    get_session,
    init_db,
    list_pg_schemas,
    set_data_dir,
)
from .core.models import (
    AccessHistogram,
    Directory,
    DirectoryStats,
    GroupInfo,
    GroupSummary,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram,
    UserInfo,
)
from .queries.facade import FsScanQueries

__all__ = [
    # High-level query API
    "FsScanQueries",
    # Configuration
    "FsScanConfig",
    # Database / session helpers
    "get_session",
    "get_engine",
    "get_db_path",
    "get_db_url",
    "init_db",
    "clear_engine_cache",
    "get_data_dir",
    "set_data_dir",
    "describe_databases",
    "filesystem_available",
    "list_pg_schemas",
    # ORM models
    "Directory",
    "DirectoryStats",
    "ScanMetadata",
    "OwnerSummary",
    "GroupSummary",
    "UserInfo",
    "GroupInfo",
    "AccessHistogram",
    "SizeHistogram",
]
