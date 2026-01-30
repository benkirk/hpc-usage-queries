"""SQLAlchemy ORM models for GPFS scan directory statistics."""

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
    parent_id = Column(
        Integer, ForeignKey("directories.dir_id"), nullable=True, index=True
    )
    name = Column(Text, nullable=False)  # component only, e.g. "username" not full path
    depth = Column(Integer, nullable=False, index=True)

    # Relationships
    stats = relationship("DirectoryStats", back_populates="directory", uselist=False)
    parent = relationship("Directory", remote_side=[dir_id], backref="children")

    __table_args__ = (
        UniqueConstraint("parent_id", "name", name="uq_dir_parent_name"),
    )

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

    dir_id = Column(
        Integer, ForeignKey("directories.dir_id"), primary_key=True
    )

    # Non-recursive metrics (direct children only)
    file_count_nr = Column(BigInteger, default=0)
    total_size_nr = Column(BigInteger, default=0)
    max_atime_nr = Column(DateTime)
    dir_count_nr = Column(BigInteger, default=0)

    # Recursive metrics (all descendants)
    file_count_r = Column(BigInteger, default=0)
    total_size_r = Column(BigInteger, default=0)
    max_atime_r = Column(DateTime)
    dir_count_r = Column(BigInteger, default=0)

    # Owner tracking: -1=no files yet, NULL=multiple owners, else=single owner UID
    owner_uid = Column(Integer, default=-1, index=True)
    # Group tracking: -1=no files yet, NULL=multiple groups, else=single group GID
    owner_gid = Column(Integer, default=-1, index=True)

    # Relationship
    directory = relationship("Directory", back_populates="stats")

    __table_args__ = (
        Index("ix_stats_size_r", "total_size_r"),
        Index("ix_stats_files_r", "file_count_r"),
        Index("ix_stats_size_nr", "total_size_nr"),
        Index("ix_stats_files_nr", "file_count_nr"),
        Index("ix_stats_dirs_r", "dir_count_r"),
        Index("ix_stats_dirs_nr", "dir_count_nr"),
        # Composite indexes for optimized owner-filtered queries
        Index("ix_stats_owner_size", "owner_uid", "total_size_r"),
        Index("ix_stats_owner_files", "owner_uid", "file_count_r"),
        # Composite indexes for optimized group-filtered queries
        Index("ix_stats_group_size", "owner_gid", "total_size_r"),
        Index("ix_stats_group_files", "owner_gid", "file_count_r"),
    )

    def __repr__(self):
        return (
            f"<DirectoryStats(dir_id={self.dir_id}, "
            f"files_r={self.file_count_r}, size_r={self.total_size_r})>"
        )


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

    owner_uid = Column(Integer, primary_key=True)
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

    uid = Column(Integer, primary_key=True)
    username = Column(Text)
    full_name = Column(Text)  # GECOS field

    def __repr__(self):
        return f"<UserInfo(uid={self.uid}, username='{self.username}')>"


class AccessHistogram(Base):
    """Pre-computed access time histogram per user.

    Stores file count and total allocated size for each atime bucket per UID.
    Enables instant access history queries without scanning directory_stats.
    """

    __tablename__ = "access_histogram"

    owner_uid = Column(Integer, primary_key=True)
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


class SizeHistogram(Base):
    """Pre-computed file size histogram per user.

    Stores file count and total allocated size for each size bucket per UID.
    Enables analysis of file size distributions per user.
    """

    __tablename__ = "size_histogram"

    owner_uid = Column(Integer, primary_key=True)
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
