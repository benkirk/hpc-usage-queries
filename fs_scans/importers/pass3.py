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
    GroupInfo,
    GroupSummary,
    OwnerSummary,
    ScanMetadata,
    SizeHistogram,
    UserInfo,
    classify_atime_bucket,
    classify_size_bucket,
)
from ..parsers.base import FilesystemParser
from .file_handling import *



def pass3_populate_summary_tables(
    session,
    input_file: Path,
    filesystem: str,
    metadata: dict,
) -> None:
    """
    Phase 3: Populate summary tables after main processing completes.

    Phase 3a: Populate UserInfo and GroupInfo - resolve UIDs to usernames and GIDs to groupnames
    Phase 3b: Compute OwnerSummary and GroupSummary - pre-aggregate per-owner and per-group statistics
    Phase 3c: Record ScanMetadata - store scan provenance info
    """
    console.print("\n[bold]Pass 3:[/bold] Populating summary tables...")

    # Phase 3a: Populate UserInfo and GroupInfo
    console.print("  [bold]Phase 3a:[/bold] Resolving user and group information...")

    @lru_cache(maxsize=10000)
    def resolve_uid(uid: int) -> tuple[str | None, str | None]:
        """Resolve UID to username and full name (GECOS)."""
        try:
            pw = pwd.getpwuid(uid)
            # GECOS field may contain comma-separated values; first is typically full name
            gecos = pw.pw_gecos.split(",")[0] if pw.pw_gecos else None
            return pw.pw_name, gecos
        except (KeyError, OverflowError):
            return None, None

    @lru_cache(maxsize=10000)
    def resolve_gid(gid: int) -> str | None:
        """Resolve GID to groupname."""
        try:
            return grp.getgrgid(gid).gr_name
        except (KeyError, OverflowError):
            return None

    # Get all distinct UIDs from directory_stats (excluding -1 and NULL)
    uids = session.execute(
        text("""
            SELECT DISTINCT owner_uid FROM directory_stats
            WHERE owner_uid IS NOT NULL AND owner_uid >= 0
        """)
    ).fetchall()

    user_count = 0
    if uids:
        user_inserts = []
        for (uid,) in uids:
            username, full_name = resolve_uid(uid)
            user_inserts.append({
                "uid": uid,
                "username": username,
                "full_name": full_name,
            })
            user_count += 1

        # Bulk upsert
        for item in user_inserts:
            session.execute(
                text("""
                    INSERT OR REPLACE INTO user_info (uid, username, full_name)
                    VALUES (:uid, :username, :full_name)
                """),
                item,
            )
        session.commit()

    console.print(f"    Resolved {user_count} unique UIDs")

    # Get all distinct GIDs from directory_stats (excluding -1 and NULL)
    gids = session.execute(
        text("""
            SELECT DISTINCT owner_gid FROM directory_stats
            WHERE owner_gid IS NOT NULL AND owner_gid >= 0
        """)
    ).fetchall()

    group_count = 0
    if gids:
        group_inserts = []
        for (gid,) in gids:
            groupname = resolve_gid(gid)
            group_inserts.append({
                "gid": gid,
                "groupname": groupname,
            })
            group_count += 1

        # Bulk upsert
        for item in group_inserts:
            session.execute(
                text("""
                    INSERT OR REPLACE INTO group_info (gid, groupname)
                    VALUES (:gid, :groupname)
                """),
                item,
            )
        session.commit()

    console.print(f"    Resolved {group_count} unique GIDs")

    # Phase 3b: Compute OwnerSummary and GroupSummary
    console.print("  [bold]Phase 3b:[/bold] Computing owner and group summaries...")

    # Clear existing summaries and recompute
    session.execute(text("DELETE FROM owner_summary"))
    session.execute(
        text("""
            INSERT INTO owner_summary (owner_uid, total_size, total_files, directory_count)
            SELECT
                owner_uid,
                SUM(total_size_nr) as total_size,
                SUM(file_count_nr) as total_files,
                COUNT(*) as directory_count
            FROM directory_stats
            WHERE owner_uid IS NOT NULL AND owner_uid >= 0
            GROUP BY owner_uid
        """)
    )
    session.commit()

    owner_count = session.execute(
        text("SELECT COUNT(*) FROM owner_summary")
    ).scalar()
    console.print(f"    Computed summaries for {owner_count} owners")

    session.execute(text("DELETE FROM group_summary"))
    session.execute(
        text("""
            INSERT INTO group_summary (owner_gid, total_size, total_files, directory_count)
            SELECT
                owner_gid,
                SUM(total_size_nr) as total_size,
                SUM(file_count_nr) as total_files,
                COUNT(*) as directory_count
            FROM directory_stats
            WHERE owner_gid IS NOT NULL AND owner_gid >= 0
            GROUP BY owner_gid
        """)
    )
    session.commit()

    group_summary_count = session.execute(
        text("SELECT COUNT(*) FROM group_summary")
    ).scalar()
    console.print(f"    Computed summaries for {group_summary_count} groups")

    # Phase 3c: Record ScanMetadata
    console.print("  [bold]Phase 3c:[/bold] Recording scan metadata...")

    scan_timestamp = extract_scan_timestamp(input_file.name)
    import_timestamp = datetime.now()

    # Get aggregate totals from root directories
    totals = session.execute(
        text("""
            SELECT
                COUNT(*) as dir_count,
                COALESCE(SUM(s.file_count_r), 0) as total_files,
                COALESCE(SUM(s.total_size_r), 0) as total_size
            FROM directories d
            JOIN directory_stats s USING (dir_id)
            WHERE d.parent_id IS NULL
        """)
    ).fetchone()

    total_directories = metadata.get("dir_count", 0)
    total_files = totals[1] if totals else 0
    total_size = totals[2] if totals else 0

    session.execute(
        text("""
            INSERT INTO scan_metadata
                (source_file, scan_timestamp, import_timestamp, filesystem,
                 total_directories, total_files, total_size)
            VALUES
                (:source_file, :scan_timestamp, :import_timestamp, :filesystem,
                 :total_directories, :total_files, :total_size)
        """),
        {
            "source_file": input_file.name,
            "scan_timestamp": scan_timestamp,
            "import_timestamp": import_timestamp,
            "filesystem": filesystem,
            "total_directories": total_directories,
            "total_files": total_files,
            "total_size": total_size,
        },
    )
    session.commit()

    console.print(f"    Recorded metadata for {input_file.name}")
