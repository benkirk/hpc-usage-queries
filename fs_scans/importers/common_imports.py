import grp
import multiprocessing as mp
import os
import pwd
import sys
import time
from collections import defaultdict
from datetime import datetime
from functools import lru_cache
from pathlib import Path
from typing import Any, Callable, Generator, TextIO

from rich.progress import TextColumn
from sqlalchemy import func, insert, text
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from ..cli.common import console, create_progress_bar, format_size
from ..core.database import (
    extract_filesystem_from_filename,
    extract_scan_timestamp,
    get_db_path,
    get_session,
    init_db,
    set_data_dir,
    drop_tables,
)
from ..core.models import (
    AccessHistogram,
    Directory,
    DirectoryStats,
    DirStatsAccumulator,
    GroupInfo,
    GroupSummary,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram,
    HistAccumulator,
    UserInfo,
    classify_atime_bucket,
    classify_size_bucket,
)
from ..parsers.base import FilesystemParser
