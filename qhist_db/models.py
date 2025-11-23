"""SQLAlchemy ORM models for HPC job history data."""

from sqlalchemy import BigInteger, Column, Date, DateTime, Float, Index, Integer, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Job(Base):
    """Job record from an HPC cluster.

    Each machine (casper, derecho) has its own database file with a 'jobs' table.
    """

    __tablename__ = "jobs"

    # Auto-incrementing primary key (avoids job ID wrap-around issues)
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Full job ID from scheduler (e.g., "2712367.desched1" or "6049117[28].desched1")
    job_id = Column(Text, nullable=False, index=True)

    # Base job number as integer for efficient queries (array index stripped)
    short_id = Column(Integer, index=True)

    # Job identification
    name = Column(Text)
    user = Column(Text, index=True)
    account = Column(Text, index=True)

    # Queue and status
    queue = Column(Text, index=True)
    status = Column(Text, index=True)

    # Timestamps (stored in UTC)
    submit = Column(DateTime, index=True)
    eligible = Column(DateTime)
    start = Column(DateTime, index=True)
    end = Column(DateTime, index=True)

    # Time metrics (in seconds)
    elapsed = Column(Integer)
    walltime = Column(Integer)
    cputime = Column(Integer)

    # Resource allocation
    numcpus = Column(Integer)
    numgpus = Column(Integer)
    numnodes = Column(Integer)
    mpiprocs = Column(Integer)
    ompthreads = Column(Integer)

    # Memory (in bytes)
    reqmem = Column(BigInteger)
    memory = Column(BigInteger)
    vmemory = Column(BigInteger)

    # Resource types
    cputype = Column(Text)
    gputype = Column(Text)
    resources = Column(Text)
    ptargets = Column(Text)

    # Performance metrics
    cpupercent = Column(Float)
    avgcpu = Column(Float)
    count = Column(Integer)

    __table_args__ = (
        # Unique constraint: same job_id + submit time = same job
        # This handles job ID wrap-around across years
        UniqueConstraint("job_id", "submit", name="uq_jobs_job_id_submit"),
        # Existing composite indexes
        Index("ix_jobs_user_account", "user", "account"),
        Index("ix_jobs_submit_end", "submit", "end"),
        # Date-filtered aggregation indexes
        Index("ix_jobs_user_submit", "user", "submit"),
        Index("ix_jobs_account_submit", "account", "submit"),
        Index("ix_jobs_queue_submit", "queue", "submit"),
    )

    def __repr__(self):
        return f"<Job(id='{self.id}', user='{self.user}', status='{self.status}')>"

    def to_dict(self):
        """Convert job record to dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class DailySummary(Base):
    """Daily summary of job charges per user/account/queue.

    Aggregates charging data for fast retrieval of usage statistics.
    """

    __tablename__ = "daily_summary"

    # Auto-incrementing primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Summary dimensions
    date = Column(Date, nullable=False)
    user = Column(Text, nullable=False)
    account = Column(Text, nullable=False)
    queue = Column(Text, nullable=False)

    # Aggregated metrics
    job_count = Column(Integer, default=0)

    # Derecho uses charge_hours (core-hours or GPU-hours depending on queue)
    charge_hours = Column(Float, default=0)

    # Casper tracks CPU, GPU, and memory hours
    cpu_hours = Column(Float, default=0)
    gpu_hours = Column(Float, default=0)
    memory_hours = Column(Float, default=0)

    __table_args__ = (
        # Each (date, user, account, queue) combination is unique
        UniqueConstraint("date", "user", "account", "queue", name="uq_daily_summary"),
        # Index for date-based queries
        Index("ix_daily_summary_date", "date"),
        # Index for user/account lookups
        Index("ix_daily_summary_user_account", "user", "account"),
    )

    def __repr__(self):
        return f"<DailySummary(date='{self.date}', user='{self.user}', account='{self.account}')>"
