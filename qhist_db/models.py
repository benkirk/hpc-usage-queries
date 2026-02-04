"""SQLAlchemy ORM models for HPC job history data."""

from sqlalchemy import BigInteger, Column, Date, DateTime, Float, ForeignKey, ForeignKeyConstraint, Index, Integer, Text, UniqueConstraint
from sqlalchemy.orm import declarative_base, relationship

Base = declarative_base()


class User(Base):
    """Normalized user lookup table."""
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, autoincrement=True)
    username = Column(Text, unique=True, nullable=False, index=True)

    def __repr__(self):
        return f"<User(id={self.id}, username='{self.username}')>"


class Account(Base):
    """Normalized account lookup table."""
    __tablename__ = "accounts"
    id = Column(Integer, primary_key=True, autoincrement=True)
    account_name = Column(Text, unique=True, nullable=False, index=True)

    def __repr__(self):
        return f"<Account(id={self.id}, account_name='{self.account_name}')>"


class Queue(Base):
    """Normalized queue lookup table."""
    __tablename__ = "queues"
    id = Column(Integer, primary_key=True, autoincrement=True)
    queue_name = Column(Text, unique=True, nullable=False, index=True)

    def __repr__(self):
        return f"<Queue(id={self.id}, queue_name='{self.queue_name}')>"


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

    # Foreign keys for normalized schema (NULL until migrated)
    user_id = Column(Integer, ForeignKey('users.id'), index=True)
    account_id = Column(Integer, ForeignKey('accounts.id'), index=True)
    queue_id = Column(Integer, ForeignKey('queues.id'), index=True)

    # Relationships (only populated after migration)
    user_obj = relationship("User")
    account_obj = relationship("Account")
    queue_obj = relationship("Queue")

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

    def calculate_charges(self, machine: str) -> dict:
        """Calculate charging hours for this job.

        Args:
            machine: Either 'derecho' or 'casper'

        Returns:
            Dictionary with keys: cpu_hours, gpu_hours, memory_hours
        """
        from .charging import casper_charge, derecho_charge

        job_dict = self.to_dict()
        return derecho_charge(job_dict) if machine == 'derecho' else casper_charge(job_dict)


class JobCharge(Base):
    """Materialized charging calculations.

    Stores pre-computed charge hours for each job, avoiding recalculation
    every time charges are queried. The charge_version field allows tracking
    charging algorithm changes over time.
    """

    __tablename__ = "job_charges"

    job_id = Column(Integer, primary_key=True)
    cpu_hours = Column(Float, nullable=False, default=0.0)
    gpu_hours = Column(Float, nullable=False, default=0.0)
    memory_hours = Column(Float, nullable=False, default=0.0)
    charge_version = Column(Integer, default=1)

    __table_args__ = (ForeignKeyConstraint(['job_id'], ['jobs.id'], ondelete='CASCADE'),)

    def __repr__(self):
        return f"<JobCharge(job_id={self.job_id}, cpu={self.cpu_hours:.2f}, gpu={self.gpu_hours:.2f})>"


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

    # Foreign keys for normalized schema (NULL until migrated)
    user_id = Column(Integer, ForeignKey('users.id'), index=True)
    account_id = Column(Integer, ForeignKey('accounts.id'), index=True)
    queue_id = Column(Integer, ForeignKey('queues.id'), index=True)

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


# Build JobCharged class attributes by copying Job columns to avoid duplication
# This creates a dict of column definitions that can be used in class definition
_job_charged_attrs = {
    '__tablename__': 'v_jobs_charged',
    '__table_args__': {'extend_existing': True},
    '__doc__': """View of jobs with computed charging hours.

    This maps to the v_jobs_charged view which includes all Job columns
    plus computed cpu_hours, gpu_hours, and memory_hours based on
    machine-specific charging rules.

    Note: This is a read-only view. Use the Job model for inserts/updates.
    The view must be created manually via create_views() after creating tables.

    Column definitions are copied from Job to avoid duplication.
    """,
}

# Copy all Job columns using _copy() to avoid deprecation warnings
for column in Job.__table__.columns:
    _job_charged_attrs[column.name] = column._copy()

# Add computed charging columns (from view)
_job_charged_attrs['cpu_hours'] = Column(Float)
_job_charged_attrs['gpu_hours'] = Column(Float)
_job_charged_attrs['memory_hours'] = Column(Float)

# Add __repr__ method
def _job_charged_repr(self):
    return f"<JobCharged(id='{self.id}', user='{self.user}', cpu_hours={self.cpu_hours:.2f})>"

_job_charged_attrs['__repr__'] = _job_charged_repr

# Create the JobCharged class dynamically from the dictionary
JobCharged = type('JobCharged', (Base,), _job_charged_attrs)
