"""SQLAlchemy ORM models for GPFS scan directory statistics."""

from datetime import datetime
from sqlalchemy import (
    BigInteger,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()

# Denormalized "ancestor-at-depth" scope columns on directory_stats.
#
# For each row we precompute the dir_id of its ancestor at each fixed level
# *relative to the collection root* in columns anc_d1 .. anc_d{SCOPE_MAX_DEPTH}.
# A scoped subtree query for an ancestor X at relative level k then becomes
# `WHERE anc_d{k} = X` — a single indexed equality that replaces the recursive
# `parent_id` walk. See docs/plans/FS_SCANS_ANCESTOR_AT_DEPTH.md.
#
# RELATIVE, not absolute: levels are measured from each collection's own root
# (the shallowest directory, MIN(depth)) — root = level 1, one below = level 2,
# and so on. The stored `depth` column stays absolute; only this anc indexing is
# root-relative, so a collection rooted deep on disk
# (/glade/campaign/a/b/c/d == /gpfs/csfs1/a/b/c/d) gets the same useful band as a
# shallow one and never starves. Today every collection roots at depth 3, so
# relative == absolute-shifted and behavior is unchanged. pass2c
# (population) and resolve_scope (query) both derive the root from MIN(depth) of
# the same database, so they always agree.
#
# SCOPE_MAX_DEPTH is how many anc_d* columns (relative levels) pass2c populates.
# It is *headroom* beyond the indexed band: populating a couple of levels past it
# lets the band be widened later by reconsolidating (rebuilding the PG indexes)
# without a re-import. Set from the real data — the largest collection (cgd,
# 57.4M rows) has fat subtrees only through relative level 7, with subtree sizes
# falling off a cliff at level 8 (max ~45k, none >100k), so 9 (band max 7 + 2)
# is ample headroom; deeper scopes are thin and use the recursive fallback.
SCOPE_MAX_DEPTH = 9

# The fast path engages — and PostgreSQL covering scope indexes are built — only
# over this (selective) band of *relative* levels. Lower bound 2 skips level 1,
# the whole-collection root (non-selective: every row shares it, and it is served
# by the precomputed-summary fast path anyway). Upper bound is the deepest
# relative level the fast predicate answers; deeper scopes fall back to the
# recursive CTE (their subtrees are small, so the fallback is ~1s) — which also
# avoids an unindexed seq-scan on PG. SQLite builds none of these indexes (the
# local CLI relies on the fallback for out-of-band scopes). Must satisfy
# SCOPE_INDEX_MAX_DEPTH <= SCOPE_MAX_DEPTH. Tune on-machine.
SCOPE_INDEX_MIN_DEPTH = 2
SCOPE_INDEX_MAX_DEPTH = 7


class Directory(Base):
    """Directory entry in the normalized path hierarchy.

    Stores directory paths as normalized components, with parent references
    to enable efficient path reconstruction via recursive CTE queries.

    Example data (shared ancestors deduplicated):
        dir_id | parent_id | name     | depth
        1      | NULL      | gpfs     | 1
        2      | 1         | csfs1    | 2
        3      | 2         | asp      | 3
        4      | 3         | userA    | 4
        5      | 3         | userB    | 4   -- shares /gpfs/csfs1/asp with userA
    """

    __tablename__ = "directories"

    dir_id = Column(Integer, primary_key=True, autoincrement=True)
    parent_id = Column(Integer, ForeignKey("directories.dir_id"), nullable=True)
    name = Column(Text, nullable=False)  # component only, e.g. "username" not full path
    depth = Column(Integer, nullable=False)

    # Relationships
    stats = relationship("DirectoryStats", back_populates="directory", uselist=False)
    parent = relationship("Directory", remote_side=[dir_id], backref="children")

    def __repr__(self):
        return f"<Directory(dir_id={self.dir_id}, name='{self.name}', depth={self.depth})>"


class DirectoryStats(Base):
    """Statistics for a directory, including both recursive and non-recursive metrics.

    Non-recursive metrics count only direct children (files/dirs in this directory).
    Recursive metrics count all descendants (files/dirs in this directory and all subdirectories).

    Owner tracking:
        - owner_uid = -1: No files seen yet
        - owner_uid = NULL: Multiple owners detected
        - owner_uid = <uid>: Single owner (all files have this UID)

    Group tracking (same logic as owner_uid):
        - owner_gid = -1: No files seen yet
        - owner_gid = NULL: Multiple groups detected
        - owner_gid = <gid>: Single group (all files have this GID)
    """

    __tablename__ = "directory_stats"

    dir_id = Column(Integer, ForeignKey("directories.dir_id"), primary_key=True)

    # Non-recursive metrics (direct children only)
    file_count_nr = Column(BigInteger, nullable=False, default=0)
    total_size_nr = Column(BigInteger, nullable=False, default=0)
    dir_count_nr = Column(BigInteger, nullable=False, default=0)
    max_atime_nr = Column(DateTime)

    # Recursive metrics (all descendants)
    file_count_r = Column(BigInteger, nullable=False, default=0)
    total_size_r = Column(BigInteger, nullable=False, default=0)
    dir_count_r = Column(BigInteger, nullable=False, default=0)
    max_atime_r = Column(DateTime, nullable=True)

    # Owner/group tracking: -1=no files yet, NULL=multiple, else=single UID/GID.
    # BigInteger: 32-bit *unsigned* UIDs/GIDs (e.g. nobody=4294967294) exceed
    # PostgreSQL's 32-bit signed `integer` range.
    owner_uid = Column(BigInteger, nullable=True, default=-1)
    owner_gid = Column(BigInteger, nullable=True, default=-1)

    # Relationship
    directory = relationship("Directory", back_populates="stats")

    def __repr__(self):
        return (
            f"<DirectoryStats(dir_id={self.dir_id}, "
            f"files_r={self.file_count_r}, size_r={self.total_size_r})>"
        )


# Denormalized ancestor-at-depth columns anc_d1 .. anc_d{SCOPE_MAX_DEPTH}.
# Added post-hoc (SQLAlchemy declarative maps columns assigned to the class) so
# the column count always tracks SCOPE_MAX_DEPTH. Plain Integer (4 B) — dir_id
# stays well inside signed int32 — and nullable (rows shallower than k store
# NULL). No ForeignKey: the consolidator drops FKs for bulk COPY and this is a
# denormalized lookup, not a referential constraint.
for _k in range(1, SCOPE_MAX_DEPTH + 1):
    setattr(DirectoryStats, f"anc_d{_k}", Column(f"anc_d{_k}", Integer, nullable=True))
del _k


class DirStatsAccumulator:
    """Memory-efficient accumulator for directory statistics using __slots__."""
    __slots__ = ('nr_count', 'nr_size', 'nr_atime', 'nr_dirs', 'first_uid', 'first_gid')

    def __init__(self):
        self.nr_count = 0
        self.nr_size = 0
        self.nr_atime = None
        self.nr_dirs = 0
        self.first_uid = None
        self.first_gid = None


class ScanMetadata(Base):
    """Track scan provenance and aggregate totals.

    Records information about each imported scan file, including timestamps
    and aggregate statistics computed from the root directories.
    """

    __tablename__ = "scan_metadata"

    scan_id = Column(Integer, primary_key=True, autoincrement=True)
    source_file = Column(Text, nullable=False)  # e.g., "20260111_csfs1_asp.list.list_all.log"
    scan_timestamp = Column(DateTime)  # parsed from YYYYMMDD in filename
    import_timestamp = Column(DateTime)  # when imported
    filesystem = Column(Text, nullable=False)
    total_directories = Column(BigInteger, default=0)
    total_files = Column(BigInteger, default=0)
    total_size = Column(BigInteger, default=0)

    def __repr__(self):
        return (
            f"<ScanMetadata(scan_id={self.scan_id}, "
            f"source_file='{self.source_file}', filesystem='{self.filesystem}')>"
        )


class OwnerSummary(Base):
    """Pre-computed per-owner aggregates.

    Makes `--group-by owner` queries instant by storing pre-aggregated
    statistics for each owner UID. Populated during scan import.
    """

    __tablename__ = "owner_summary"

    owner_uid = Column(BigInteger, primary_key=True)
    total_size = Column(BigInteger, default=0)
    total_files = Column(BigInteger, default=0)
    directory_count = Column(Integer, default=0)

    def __repr__(self):
        return (
            f"<OwnerSummary(owner_uid={self.owner_uid}, "
            f"total_size={self.total_size}, total_files={self.total_files})>"
        )


class UserInfo(Base):
    """Cache UID-to-username mappings resolved during scan.

    Stores username and GECOS (full name) information for UIDs
    encountered during scan imports, reducing repeated passwd lookups.
    """

    __tablename__ = "user_info"

    uid = Column(BigInteger, primary_key=True)
    username = Column(Text)
    full_name = Column(Text)  # GECOS field

    def __repr__(self):
        return f"<UserInfo(uid={self.uid}, username='{self.username}')>"


class GroupInfo(Base):
    """Cache GID-to-groupname mappings resolved during scan.

    Stores group name information for GIDs encountered during scan
    imports, reducing repeated group lookups.
    """

    __tablename__ = "group_info"

    gid = Column(BigInteger, primary_key=True)
    groupname = Column(Text)

    def __repr__(self):
        return f"<GroupInfo(gid={self.gid}, groupname='{self.groupname}')>"


class GroupSummary(Base):
    """Pre-computed per-group aggregates.

    Makes `--group-by group` queries instant by storing pre-aggregated
    statistics for each group GID. Populated during scan import.
    """

    __tablename__ = "group_summary"

    owner_gid = Column(BigInteger, primary_key=True)
    total_size = Column(BigInteger, default=0)
    total_files = Column(BigInteger, default=0)
    directory_count = Column(Integer, default=0)

    def __repr__(self):
        return (
            f"<GroupSummary(owner_gid={self.owner_gid}, "
            f"total_size={self.total_size}, total_files={self.total_files})>"
        )


class AccessHistogram(Base):
    """Pre-computed access time histogram per user.

    Stores file count and total allocated size for each atime bucket per UID.
    Enables instant access history queries without scanning directory_stats.
    """

    __tablename__ = "access_histogram"

    owner_uid = Column(BigInteger, primary_key=True)
    bucket_index = Column(Integer, primary_key=True)  # 0-9 (maps to ATIME_BUCKETS)

    file_count = Column(BigInteger, default=0)
    total_size = Column(BigInteger, default=0)  # allocated bytes

    __table_args__ = (
        Index("ix_access_hist_uid", "owner_uid"),
        Index("ix_access_hist_bucket", "bucket_index"),
    )

    def __repr__(self):
        return (
            f"<AccessHistogram(owner_uid={self.owner_uid}, "
            f"bucket_index={self.bucket_index}, file_count={self.file_count})>"
        )
# Access Time Histogram (10 buckets)
# Tracks file distribution by last access time relative to scan date
ATIME_BUCKETS = [
    ("< 1 Month", 30),           # 0-30 days
    ("1-3 Months", 90),          # 30-90 days
    ("3-6 Months", 180),         # 90-180 days
    ("6-12 Months", 365),        # 180-365 days
    ("1-2 Years", 730),          # 1-2 years
    ("2-3 Years", 1095),         # 2-3 years
    ("3-4 Years", 1460),         # 3-4 years
    ("5-6 Years", 2190),         # 5-6 years
    ("6-7 Years", 2555),         # 6-7 years
    ("7+ Years", None),          # 7+ years
]

def classify_atime_bucket(atime: datetime | None, scan_date: datetime) -> int:
    """Classify file's access time into histogram bucket.

    Args:
        atime: File's last access time
        scan_date: Scan timestamp (extracted from filename)

    Returns:
        Bucket index (0-9)
    """
    if atime is None:
        return len(ATIME_BUCKETS) - 1  # Default to oldest bucket

    days_old = (scan_date - atime).days

    for idx, (_, max_days) in enumerate(ATIME_BUCKETS):
        if max_days is None:  # Last bucket (7+ years)
            return idx
        if days_old < max_days:
            return idx

    return len(ATIME_BUCKETS) - 1  # Fallback to oldest bucket


class SizeHistogram(Base):
    """Pre-computed file size histogram per user.

    Stores file count and total allocated size for each size bucket per UID.
    Enables analysis of file size distributions per user.
    """

    __tablename__ = "size_histogram"

    owner_uid = Column(BigInteger, primary_key=True)
    bucket_index = Column(Integer, primary_key=True)  # 0-9 (maps to SIZE_BUCKETS)

    file_count = Column(BigInteger, default=0)
    total_size = Column(BigInteger, default=0)  # allocated bytes

    __table_args__ = (
        Index("ix_size_hist_uid", "owner_uid"),
        Index("ix_size_hist_bucket", "bucket_index"),
    )

    def __repr__(self):
        return (
            f"<SizeHistogram(owner_uid={self.owner_uid}, "
            f"bucket_index={self.bucket_index}, file_count={self.file_count})>"
        )


# Size Histogram (10 buckets)
# Logarithmic scale covering practical file size ranges
SIZE_BUCKETS = [
    ("0 - 1 KiB", 0, 1024),
    ("1 KiB - 10 KiB", 1024, 10 * 1024),
    ("10 KiB - 100 KiB", 10 * 1024, 100 * 1024),
    ("100 KiB - 1 MiB", 100 * 1024, 1024 * 1024),
    ("1 MiB - 10 MiB", 1024 * 1024, 10 * 1024 * 1024),
    ("10 MiB - 100 MiB", 10 * 1024 * 1024, 100 * 1024 * 1024),
    ("100 MiB - 1 GiB", 100 * 1024 * 1024, 1024 * 1024 * 1024),
    ("1 GiB - 10 GiB", 1024 * 1024 * 1024, 10 * 1024 * 1024 * 1024),
    ("10 GiB - 100 GiB", 10 * 1024 * 1024 * 1024, 100 * 1024 * 1024 * 1024),
    ("100 GiB+", 100 * 1024 * 1024 * 1024, None),
]


def classify_size_bucket(size_bytes: int) -> int:
    """Classify file size into histogram bucket.

    Args:
        size_bytes: File size in bytes (allocated size)

    Returns:
        Bucket index (0-9)
    """
    for idx, (_, min_size, max_size) in enumerate(SIZE_BUCKETS):
        if max_size is None:  # Last bucket (100 GiB+)
            if size_bytes >= min_size:
                return idx
        elif min_size <= size_bytes < max_size:
            return idx

    return len(SIZE_BUCKETS) - 1  # Fallback to largest bucket


class HistAccumulator:
    """Memory-efficient accumulator for histogram statistics using __slots__."""
    __slots__ = ('atime_hist', 'size_hist', 'atime_size', 'size_size')

    def __init__(self):
        self.atime_hist = [0] * 10
        self.size_hist = [0] * 10
        self.atime_size = [0] * 10
        self.size_size = [0] * 10
