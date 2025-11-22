"""SQLAlchemy ORM models for HPC job history data."""

from sqlalchemy import BigInteger, Column, DateTime, Float, Index, Integer, Text
from sqlalchemy.orm import declarative_base, declared_attr

Base = declarative_base()


class JobMixin:
    """Mixin class defining common job fields for Casper and Derecho tables."""

    # Primary key - unique job identifier per machine
    short_id = Column(Integer, primary_key=True, autoincrement=False)

    # Job identification
    id = Column(Text, index=True)
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

    @declared_attr
    def __table_args__(cls):
        return (
            Index(f"ix_{cls.__tablename__}_user_account", "user", "account"),
            Index(f"ix_{cls.__tablename__}_submit_end", "submit", "end"),
        )

    def __repr__(self):
        return f"<{self.__class__.__name__}(short_id={self.short_id}, user='{self.user}', status='{self.status}')>"

    def to_dict(self):
        """Convert job record to dictionary."""
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class CasperJob(JobMixin, Base):
    """Job records from the Casper cluster."""

    __tablename__ = "casper_jobs"


class DerechoJob(JobMixin, Base):
    """Job records from the Derecho cluster."""

    __tablename__ = "derecho_jobs"


# Mapping of machine names to model classes
MACHINE_MODELS = {
    "casper": CasperJob,
    "derecho": DerechoJob,
}


def get_model_for_machine(machine: str):
    """Get the appropriate model class for a machine name.

    Args:
        machine: Machine name ('casper' or 'derecho')

    Returns:
        The corresponding SQLAlchemy model class

    Raises:
        ValueError: If machine name is not recognized
    """
    machine = machine.lower()
    if machine not in MACHINE_MODELS:
        raise ValueError(f"Unknown machine: {machine}. Must be one of: {list(MACHINE_MODELS.keys())}")
    return MACHINE_MODELS[machine]
