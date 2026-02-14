"""SQLAlchemy ORM models for HPC job history data."""

from datetime import datetime, timezone

from sqlalchemy import BigInteger, Column, Date, DateTime, Float, ForeignKey, ForeignKeyConstraint, Index, Integer, LargeBinary, Text, UniqueConstraint, select
from sqlalchemy.ext.hybrid import hybrid_property
from sqlalchemy.orm import declarative_base, declared_attr, relationship

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


class LookupMixin:
    """Mixin providing normalized user/account/queue FK columns and hybrid properties.

    Subclasses can set _null_sentinel to control what value is returned (and
    treated as NULL on set) when FKs are NULL.  Job uses None (default),
    DailySummary uses 'NO_JOBS'.
    """

    _null_sentinel = None

    # --- FK columns -----------------------------------------------------------

    @declared_attr
    def user_id(cls):
        return Column(Integer, ForeignKey('users.id'), index=True)

    @declared_attr
    def account_id(cls):
        return Column(Integer, ForeignKey('accounts.id'), index=True)

    @declared_attr
    def queue_id(cls):
        return Column(Integer, ForeignKey('queues.id'), index=True)

    # --- Relationships --------------------------------------------------------

    @declared_attr
    def user_obj(cls):
        return relationship("User")

    @declared_attr
    def account_obj(cls):
        return relationship("Account")

    @declared_attr
    def queue_obj(cls):
        return relationship("Queue")

    # --- user hybrid property -------------------------------------------------

    @hybrid_property
    def user(self):
        return self.user_obj.username if self.user_obj else self._null_sentinel

    @user.setter
    def user(self, username):
        if username is None or username == self._null_sentinel:
            self.user_id = None
            self.user_obj = None
            self._pending_username = None
            return
        self._pending_username = username

    @user.expression
    def user(cls):
        return select(User.username).where(User.id == cls.user_id).correlate(cls).scalar_subquery()

    # --- account hybrid property ----------------------------------------------

    @hybrid_property
    def account(self):
        return self.account_obj.account_name if self.account_obj else self._null_sentinel

    @account.setter
    def account(self, account_name):
        if account_name is None or account_name == self._null_sentinel:
            self.account_id = None
            self.account_obj = None
            self._pending_account_name = None
            return
        self._pending_account_name = account_name

    @account.expression
    def account(cls):
        return select(Account.account_name).where(Account.id == cls.account_id).correlate(cls).scalar_subquery()

    # --- queue hybrid property ------------------------------------------------

    @hybrid_property
    def queue(self):
        return self.queue_obj.queue_name if self.queue_obj else self._null_sentinel

    @queue.setter
    def queue(self, queue_name):
        if queue_name is None or queue_name == self._null_sentinel:
            self.queue_id = None
            self.queue_obj = None
            self._pending_queue_name = None
            return
        self._pending_queue_name = queue_name

    @queue.expression
    def queue(cls):
        return select(Queue.queue_name).where(Queue.id == cls.queue_id).correlate(cls).scalar_subquery()


class Job(LookupMixin, Base):
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

    # Relationships to associated data (1:1)
    job_record_obj = relationship("JobRecord", uselist=False, back_populates="job")
    job_charge_obj = relationship("JobCharge", uselist=False, back_populates="job")

    # Queue and status
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

    # Performance metrics
    cpupercent = Column(Float)
    avgcpu = Column(Float)
    count = Column(Integer)

    __table_args__ = (
        # Unique constraint: same job_id + submit time = same job
        # This handles job ID wrap-around across years
        UniqueConstraint("job_id", "submit", name="uq_jobs_job_id_submit"),
        # Existing composite indexes (using FKs)
        Index("ix_jobs_user_account", "user_id", "account_id"),
        Index("ix_jobs_submit_end", "submit", "end"),
        # Date-filtered aggregation indexes (using FKs)
        Index("ix_jobs_user_submit", "user_id", "submit"),
        Index("ix_jobs_account_submit", "account_id", "submit"),
        Index("ix_jobs_queue_submit", "queue_id", "submit"),
    )

    def __repr__(self):
        return f"<Job(id='{self.id}', user='{self.user}', status='{self.status}')>"

    def to_dict(self):
        """Convert job record to dictionary.

        Explicitly includes hybrid properties (user, account, queue) that
        are not part of __table__.columns.
        """
        result = {c.name: getattr(self, c.name) for c in self.__table__.columns}
        # Add hybrid properties
        result['user'] = self.user
        result['account'] = self.account
        result['queue'] = self.queue
        return result

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

    @property
    def pbs_record(self):
        """Retrieve and unpickle the original PbsRecord object.

        Returns None if no JobRecord exists (historical data imported before
        JobRecord feature was added).

        Uses SQLAlchemy relationship for lazy loading and instance-level caching
        to avoid repeated decompression of the same PbsRecord.

        Returns:
            PbsRecord object or None
        """
        # Check instance cache first (avoid re-decompressing on multiple accesses)
        if hasattr(self, '_cached_pbs_record'):
            return self._cached_pbs_record

        # Use relationship to get JobRecord (SQLAlchemy handles lazy loading)
        if self.job_record_obj is None:
            self._cached_pbs_record = None
            return None

        # Decompress and cache the PbsRecord object
        pbs_record_obj = self.job_record_obj.to_pbs_record()
        self._cached_pbs_record = pbs_record_obj
        return pbs_record_obj


class JobCharge(Base):
    """Materialized charging calculations.

    Stores pre-computed charge hours for each job, avoiding recalculation
    every time charges are queried. The charge_version field allows tracking
    charging algorithm changes over time.

    Has a 1:1 relationship with Job (every job gets charges calculated during import).
    """

    __tablename__ = "job_charges"

    job_id = Column(Integer, primary_key=True)
    cpu_hours = Column(Float, nullable=False, default=0.0)
    gpu_hours = Column(Float, nullable=False, default=0.0)
    memory_hours = Column(Float, nullable=False, default=0.0)
    charge_version = Column(Integer, default=1)

    # Relationship back to Job
    job = relationship("Job", back_populates="job_charge_obj")

    __table_args__ = (ForeignKeyConstraint(['job_id'], ['jobs.id'], ondelete='CASCADE'),)

    def __repr__(self):
        return f"<JobCharge(job_id={self.job_id}, cpu={self.cpu_hours:.2f}, gpu={self.gpu_hours:.2f})>"


class JobRecord(Base):
    """Compressed, pickled PbsRecord storage.

    Stores the raw PBS accounting record object for jobs imported from local
    PBS logs. Now that SSH sync is removed, all new jobs will have a JobRecord.

    Has a 1:1 relationship with Job (accessible via job.job_record_obj or
    job.pbs_record for the decompressed object).
    """

    __tablename__ = "job_records"

    # Primary key = foreign key (true 1-to-1, matches JobCharge pattern)
    job_id = Column(Integer, primary_key=True)

    # Gzip-compressed pickle of PbsRecord object
    compressed_data = Column(LargeBinary, nullable=False)

    # Metadata for debugging/auditing
    created_at = Column(DateTime, default=lambda: datetime.now(timezone.utc))

    # Relationship back to Job
    job = relationship("Job", back_populates="job_record_obj")

    __table_args__ = (
        ForeignKeyConstraint(['job_id'], ['jobs.id'], ondelete='CASCADE'),
    )

    @classmethod
    def from_pbs_record(cls, job_id: int, pbs_record) -> 'JobRecord':
        """Create a JobRecord from a PbsRecord object.

        Args:
            job_id: Database ID of the associated Job
            pbs_record: PbsRecord object to compress and store

        Returns:
            JobRecord instance ready for insertion
        """
        import gzip
        import pickle

        pickled = pickle.dumps(pbs_record, protocol=pickle.HIGHEST_PROTOCOL)
        compressed = gzip.compress(pickled, compresslevel=6)

        return cls(job_id=job_id, compressed_data=compressed)

    def to_pbs_record(self):
        """Decompress and unpickle the stored PbsRecord.

        Returns:
            PbsRecord object or None if decompression/unpickling fails
        """
        import gzip
        import pickle

        try:
            decompressed = gzip.decompress(self.compressed_data)
            return pickle.loads(decompressed)
        except Exception as e:
            from .log_config import get_logger
            logger = get_logger(__name__)
            logger.error(f"Failed to decompress/unpickle JobRecord for job {self.job_id}: {e}")
            return None

    def __repr__(self):
        return f"<JobRecord(job_id={self.job_id})>"


class DailySummary(LookupMixin, Base):
    """Daily summary of job charges per user/account/queue.

    Aggregates charging data for fast retrieval of usage statistics.
    """

    _null_sentinel = 'NO_JOBS'

    __tablename__ = "daily_summary"

    # Auto-incrementing primary key
    id = Column(Integer, primary_key=True, autoincrement=True)

    # Summary dimensions
    date = Column(Date, nullable=False)

    # Aggregated metrics
    job_count = Column(Integer, default=0)

    # Derecho uses charge_hours (core-hours or GPU-hours depending on queue)
    charge_hours = Column(Float, default=0)

    # Casper tracks CPU, GPU, and memory hours
    cpu_hours = Column(Float, default=0)
    gpu_hours = Column(Float, default=0)
    memory_hours = Column(Float, default=0)

    __table_args__ = (
        # Each (date, user_id, account_id, queue_id) combination is unique
        UniqueConstraint("date", "user_id", "account_id", "queue_id", name="uq_daily_summary"),
        # Index for date-based queries
        Index("ix_daily_summary_date", "date"),
        # Index for user/account lookups (using FKs)
        Index("ix_daily_summary_user_account", "user_id", "account_id"),
    )

    def __repr__(self):
        return f"<DailySummary(date='{self.date}', user='{self.user}', account='{self.account}')>"


# Event listeners to ensure foreign keys are populated from relationship objects


from sqlalchemy import event
from sqlalchemy.orm import Session as SessionClass


@event.listens_for(SessionClass, 'before_flush')
def ensure_lookup_tables_before_flush(session, flush_context, instances):
    """Ensure user/account/queue FKs are set from pending names before flush."""
    # Check if we have any Job or DailySummary objects to process
    has_job_related_objects = any(
        isinstance(obj, LookupMixin)
        for obj in session.new
    )
    if not has_job_related_objects:
        return

    # Check if lookup tables exist in this database
    try:
        # Build a cache of lookup objects to avoid duplicates
        lookup_cache = {
            'users': {},  # username -> User object
            'accounts': {},  # account_name -> Account object
            'queues': {},  # queue_name -> Queue object
        }

        # First pass: catalog existing lookup objects in DB
        for user in session.query(User).all():
            lookup_cache['users'][user.username] = user
        for account in session.query(Account).all():
            lookup_cache['accounts'][account.account_name] = account
        for queue in session.query(Queue).all():
            lookup_cache['queues'][queue.queue_name] = queue
    except Exception:
        # Tables don't exist in this database (e.g., filesystem scan DB)
        # Skip processing
        return

    # Process new objects to resolve pending names
    for obj in list(session.new):
        if isinstance(obj, LookupMixin):
            # Handle user
            if hasattr(obj, '_pending_username') and obj._pending_username:
                username = obj._pending_username
                if username not in lookup_cache['users']:
                    user_obj = User(username=username)
                    session.add(user_obj)
                    lookup_cache['users'][username] = user_obj
                obj.user_obj = lookup_cache['users'][username]
                del obj._pending_username  # Clean up

            # Handle account
            if hasattr(obj, '_pending_account_name') and obj._pending_account_name:
                account_name = obj._pending_account_name
                if account_name not in lookup_cache['accounts']:
                    account_obj = Account(account_name=account_name)
                    session.add(account_obj)
                    lookup_cache['accounts'][account_name] = account_obj
                obj.account_obj = lookup_cache['accounts'][account_name]
                del obj._pending_account_name  # Clean up

            # Handle queue
            if hasattr(obj, '_pending_queue_name') and obj._pending_queue_name:
                queue_name = obj._pending_queue_name
                if queue_name not in lookup_cache['queues']:
                    queue_obj = Queue(queue_name=queue_name)
                    session.add(queue_obj)
                    lookup_cache['queues'][queue_name] = queue_obj
                obj.queue_obj = lookup_cache['queues'][queue_name]
                del obj._pending_queue_name  # Clean up
