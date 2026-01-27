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

    Non-recursive metrics count only direct children (files in this directory).
    Recursive metrics count all descendants (files in this directory and all subdirectories).

    Owner tracking:
        - owner_uid = -1: No files seen yet
        - owner_uid = NULL: Multiple owners detected
        - owner_uid = <uid>: Single owner (all files have this UID)
    """

    __tablename__ = "directory_stats"

    dir_id = Column(
        Integer, ForeignKey("directories.dir_id"), primary_key=True
    )

    # Non-recursive metrics (direct children only)
    file_count_nr = Column(BigInteger, default=0)
    total_size_nr = Column(BigInteger, default=0)
    max_atime_nr = Column(DateTime)

    # Recursive metrics (all descendants)
    file_count_r = Column(BigInteger, default=0)
    total_size_r = Column(BigInteger, default=0)
    max_atime_r = Column(DateTime)

    # Owner tracking: -1=no files yet, NULL=multiple owners, else=single owner UID
    owner_uid = Column(Integer, default=-1, index=True)

    # Relationship
    directory = relationship("Directory", back_populates="stats")

    __table_args__ = (
        Index("ix_stats_size_r", "total_size_r"),
        Index("ix_stats_files_r", "file_count_r"),
        Index("ix_stats_size_nr", "total_size_nr"),
        Index("ix_stats_files_nr", "file_count_nr"),
    )

    def __repr__(self):
        return (
            f"<DirectoryStats(dir_id={self.dir_id}, "
            f"files_r={self.file_count_r}, size_r={self.total_size_r})>"
        )
