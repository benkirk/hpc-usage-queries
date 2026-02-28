"""Shared fixtures for jobhist tests."""

import types

import pytest
from sqlalchemy import create_engine, event
from sqlalchemy.engine import Engine
from sqlalchemy.orm import sessionmaker

from job_history.database import Base


# Apply SQLite optimizations for test databases
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_conn, connection_record):
    """Configure SQLite for optimal performance in tests."""
    cursor = dbapi_conn.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


@pytest.fixture
def in_memory_engine():
    """Create an in-memory SQLite database engine for testing.

    Creates all tables including normalized schema (users, accounts, queues, job_charges).
    """
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()


@pytest.fixture
def in_memory_session(in_memory_engine):
    """Create a session bound to the in-memory database."""
    Session = sessionmaker(bind=in_memory_engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_job_record():
    """Sample qhist JSON record (raw format from qhist -J)."""
    return {
        "short_id": "123456",
        "user": "testuser",
        "account": "NCAR0001",
        "queue": "main",
        "Exit_status": "0",
        "jobname": "test_job",
        "ctime": "2025-01-15T10:00:00",
        "etime": "2025-01-15T10:01:00",
        "start": "2025-01-15T10:02:00",
        "end": "2025-01-15T11:02:00",
        "Resource_List": {
            "walltime": 2.0,  # hours
            "ncpus": 256,
            "ngpus": 0,
            "nodect": 2,
            "mem": 100,  # GB
            "select": "2:ncpus=128:mpiprocs=128:ompthreads=1",
        },
        "resources_used": {
            "walltime": 1.0,  # hours (elapsed)
            "cput": 0.5,  # hours
            "mem": 50,  # GB
            "vmem": 60,  # GB
            "cpupercent": 50.0,
            "avgcpu": 45.0,
        },
        "run_count": 1,
    }


@pytest.fixture
def sample_parsed_record():
    """Sample parsed job record (after parse_job_record)."""
    from datetime import datetime, timezone

    return {
        "job_id": "123456.desched1",
        "short_id": 123456,
        "name": "test_job",
        "user": "testuser",
        "account": "NCAR0001",
        "queue": "main",
        "status": "0",
        "submit": datetime(2025, 1, 15, 17, 0, 0, tzinfo=timezone.utc),  # MST+7
        "eligible": datetime(2025, 1, 15, 17, 1, 0, tzinfo=timezone.utc),
        "start": datetime(2025, 1, 15, 17, 2, 0, tzinfo=timezone.utc),
        "end": datetime(2025, 1, 15, 18, 2, 0, tzinfo=timezone.utc),
        "elapsed": 3600,  # seconds
        "walltime": 7200,  # seconds
        "cputime": 1800,  # seconds
        "numcpus": 256,
        "numgpus": 0,
        "numnodes": 2,
        "mpiprocs": 128,
        "ompthreads": 1,
        "reqmem": 107374182400,  # 100 GB in bytes
        "memory": 53687091200,  # 50 GB in bytes
        "vmemory": 64424509440,  # 60 GB in bytes
        "cputype": None,
        "gputype": None,
        "resources": "2:ncpus=128:mpiprocs=128:ompthreads=1",
        "ptargets": None,
        "cpupercent": 50.0,
        "avgcpu": 45.0,
        "count": 1,
    }


@pytest.fixture
def derecho_cpu_job():
    """Derecho CPU production job for charging tests."""
    return types.SimpleNamespace(
        elapsed=3600,  # 1 hour
        numnodes=2,
        numcpus=256,
        numgpus=0,
        memory=107374182400,  # 100 GB
        queue="main",
    )


@pytest.fixture
def derecho_gpu_job():
    """Derecho GPU production job for charging tests."""
    return types.SimpleNamespace(
        elapsed=3600,  # 1 hour
        numnodes=2,
        numcpus=256,
        numgpus=8,
        memory=107374182400,  # 100 GB
        queue="main@desched1:gpudev",
    )


@pytest.fixture
def derecho_cpu_dev_job():
    """Derecho CPU dev job for charging tests."""
    return types.SimpleNamespace(
        elapsed=3600,  # 1 hour
        numnodes=1,
        numcpus=32,
        numgpus=0,
        memory=32212254720,  # 30 GB
        queue="develop",
    )


@pytest.fixture
def casper_job():
    """Casper job for charging tests."""
    return types.SimpleNamespace(
        elapsed=3600,  # 1 hour
        numcpus=8,
        numgpus=2,
        memory=32212254720,  # 30 GB
    )
