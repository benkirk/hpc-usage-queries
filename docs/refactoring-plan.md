# QHist-Queries Refactoring Plan

This document outlines a phased approach to improving the qhist-queries codebase. Each phase builds on the previous, with early phases focused on foundation and testing, and later phases on features and scalability.

## Current State Assessment

**Project Size**: ~1,200 lines of Python across 7 files
**Database Size**: ~6GB combined (Casper + Derecho)
**Architecture**: SQLite per machine, SQLAlchemy ORM, SSH-based data fetching

### Strengths
- Clean separation of concerns across modules
- Good documentation (README, schema.md, docstrings)
- Type hints throughout
- Idempotent sync operations with duplicate detection
- Graceful handling of missing accounting data

### Areas for Improvement
- No test coverage
- Not pip-installable (no pyproject.toml)
- Large monolithic sync.py (528 lines)
- Duplicate charging logic (Python + SQL)
- Timezone handling bug (local times stored as UTC)

---

## Phase 1: Foundation

**Goal**: Establish proper Python packaging and fix critical issues.

### 1.1 Add pyproject.toml

Create a modern Python package configuration:

```toml
[project]
name = "qhist-db"
version = "0.1.0"
description = "SQLite database toolkit for NCAR HPC job history"
requires-python = ">=3.10"
dependencies = [
    "sqlalchemy>=2.0",
]

[project.optional-dependencies]
dev = [
    "pytest>=7.0",
    "pytest-cov",
]

[project.scripts]
qhist-sync = "scripts.sync_jobs:main"
```

### 1.2 Add requirements.txt

For users who prefer pip install:

```
sqlalchemy>=2.0
```

### 1.3 Fix Timezone Handling

**Location**: `qhist_db/sync.py`, lines 77-84

**Current Bug**: Local times are incorrectly marked as UTC:
```python
# Current (wrong)
dt = dt.replace(tzinfo=timezone.utc)
```

**Fix**: Properly convert Mountain Time to UTC:
```python
from zoneinfo import ZoneInfo

mountain = ZoneInfo("America/Denver")
dt = dt.replace(tzinfo=mountain).astimezone(timezone.utc)
```

### 1.4 Clean Up Dead Code

**Location**: `qhist_db/charging.py`, lines 9-60

The Python functions `derecho_charge()` and `casper_charge()` are defined but never called - only the SQL views are used. Keep for potential future use (e.g., ad-hoc calculations), but document that SQL views are the primary charging source.

### 1.5 Fix Path Handling

**Location**: `scripts/sync_jobs.py`, line 9

**Current (fragile)**:
```python
sys.path.insert(0, str(__file__).rsplit("/", 2)[0])
```

**Fix (cross-platform)**:
```python
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))
```

---

## Phase 2: Testing Infrastructure

**Goal**: Establish comprehensive test coverage for high-risk areas.

### 2.1 Test Directory Structure

```
tests/
├── conftest.py           # Shared fixtures
├── test_parsers.py       # Timestamp/type conversion tests
├── test_charging.py      # Charging calculation tests
├── test_models.py        # ORM model tests
├── test_summary.py       # Summary generation tests
└── test_sync.py          # Sync integration tests
```

### 2.2 Priority Test Areas

**High Priority** (complex logic, high risk):
| Area | File | Lines | Risk |
|------|------|-------|------|
| Timestamp parsing | sync.py | 54-91 | Multiple formats, timezone edge cases |
| Type conversion | sync.py | 94-128 | Integer overflow, null handling |
| Charging calculations | charging.py | 64-91 | Financial accuracy |
| Duplicate detection | models.py | 69-72 | Data integrity |

**Medium Priority**:
- Date range iteration
- Batch insert with conflict handling
- Summary aggregation

### 2.3 Test Fixtures

```python
# conftest.py
import pytest
from sqlalchemy import create_engine
from qhist_db.models import Base

@pytest.fixture
def in_memory_db():
    """Create an in-memory SQLite database for testing."""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    yield engine
    engine.dispose()

@pytest.fixture
def sample_job_record():
    """Sample qhist JSON record."""
    return {
        "id": "123456.desched1",
        "short_id": 123456,
        "user": "testuser",
        "account": "NCAR0001",
        "queue": "main",
        "status": "F",
        "elapsed": 3600,
        "numnodes": 2,
        "numcpus": 256,
    }
```

### 2.4 Example Tests

```python
# test_charging.py
import pytest
from qhist_db.charging import derecho_charge

class TestDerechoCharging:
    def test_cpu_production_queue(self):
        """CPU production: elapsed * numnodes * 128 / 3600"""
        job = {"elapsed": 3600, "numnodes": 2, "numcpus": 256, "queue": "main"}
        assert derecho_charge(job) == 256.0  # 1 hour * 2 nodes * 128 cores

    def test_gpu_production_queue(self):
        """GPU production: elapsed * numnodes * 4 / 3600"""
        job = {"elapsed": 3600, "numnodes": 2, "numgpus": 8, "queue": "gpudev"}
        # gpudev has both "gpu" and "dev" - should use actual GPUs
        assert derecho_charge(job) == 8.0

    def test_cpu_dev_queue(self):
        """CPU dev: elapsed * numcpus / 3600"""
        job = {"elapsed": 3600, "numnodes": 1, "numcpus": 32, "queue": "develop"}
        assert derecho_charge(job) == 32.0
```

---

## Phase 3: Code Organization

**Goal**: Split large modules, reduce complexity, improve maintainability.

### 3.1 Split sync.py

Current `sync.py` (528 lines) has multiple responsibilities. Split into:

**parsers.py** (~150 lines) - Field parsing and type conversion:
- `QHIST_FIELDS` constant
- `parse_timestamp()` function
- `parse_integer()`, `parse_bytes()`, `parse_duration()` functions
- `parse_job_record()` function

**remote.py** (~80 lines) - SSH command execution:
- `run_qhist_command()` function
- `fetch_jobs_ssh()` generator
- SSH timeout handling

**sync.py** (~300 lines) - Orchestration only:
- `sync_jobs_bulk()` function
- `_sync_single_day()` function
- `_insert_batch()` function
- Date range helpers

### 3.2 Module Dependencies After Split

```
sync_jobs.py (CLI)
    └── qhist_db/
        ├── sync.py (orchestration)
        │   ├── remote.py (SSH)
        │   │   └── parsers.py (data parsing)
        │   ├── models.py (ORM)
        │   └── summary.py (aggregation)
        ├── database.py (connections)
        └── charging.py (formulas)
```

### 3.3 Add SSH Timeout

**Current** (`sync.py` line 266):
```python
result = subprocess.run(cmd, capture_output=True, text=True)
```

**Improved**:
```python
result = subprocess.run(
    cmd,
    capture_output=True,
    text=True,
    timeout=300,  # 5 minute timeout
)
```

### 3.4 Consolidate Date Utilities

Merge duplicate date parsing:
- `validate_date()` in `sync_jobs.py`
- `date_range()` in `sync.py`

Into a single `dates.py` or add to `parsers.py`:

```python
def parse_date_string(date_str: str) -> date:
    """Parse YYYYMMDD string to date object."""
    return datetime.strptime(date_str, "%Y%m%d").date()

def date_range(start: date, end: date) -> Iterator[date]:
    """Generate dates from start to end inclusive."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)
```

---

## Phase 4: Quality Improvements

**Goal**: Improve observability, configurability, and code quality.

### 4.1 Add Logging Framework

Replace `print()` with structured logging:

```python
# qhist_db/logging.py
import logging

def get_logger(name: str) -> logging.Logger:
    """Get a logger with standard configuration."""
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    return logger
```

Usage:
```python
# sync.py
logger = get_logger(__name__)
logger.info(f"Syncing {machine} for {target_date}")
logger.warning(f"Missing accounting data for {target_date}")
```

### 4.2 Progress Bars

For long sync operations, add optional progress display:

```python
# Optional dependency: rich or tqdm
try:
    from tqdm import tqdm
except ImportError:
    tqdm = None

def sync_with_progress(dates, ...):
    iterator = tqdm(dates) if tqdm and verbose else dates
    for date in iterator:
        ...
```

### 4.3 Extract Constants

**Location**: `qhist_db/charging.py`

```python
# Machine-specific constants
DERECHO_CORES_PER_NODE = 128
DERECHO_GPUS_PER_NODE = 4
CASPER_MEMORY_UNIT_GB = 1024 * 1024 * 1024  # bytes per GB
```

### 4.4 Consolidate Charging Logic

Option A: **Generate SQL from Python** (recommended)

```python
def generate_derecho_view_sql() -> str:
    """Generate Derecho charging view SQL from Python constants."""
    return f"""
    CREATE VIEW IF NOT EXISTS v_jobs_charged AS
    SELECT *,
      CASE
        WHEN queue LIKE '%gpu%' AND queue LIKE '%dev%'
          THEN elapsed * COALESCE(numgpus, 0) / 3600.0
        WHEN queue LIKE '%dev%'
          THEN elapsed * COALESCE(numcpus, 0) / 3600.0
        WHEN queue LIKE '%gpu%'
          THEN elapsed * COALESCE(numnodes, 0) * {DERECHO_GPUS_PER_NODE} / 3600.0
        ELSE elapsed * COALESCE(numnodes, 0) * {DERECHO_CORES_PER_NODE} / 3600.0
      END AS charge_hours
    FROM jobs;
    """
```

---

## Phase 5: Features & Scalability (Future)

**Goal**: Add user-requested features and prepare for growth.

### 5.1 Query Interface

Add a Python API for common queries:

```python
# qhist_db/queries.py
class JobQueries:
    def __init__(self, session: Session):
        self.session = session

    def jobs_by_user(self, user: str, start: date = None, end: date = None):
        """Get all jobs for a user, optionally filtered by date range."""
        query = self.session.query(Job).filter(Job.user == user)
        if start:
            query = query.filter(Job.end >= start)
        if end:
            query = query.filter(Job.end <= end)
        return query.all()

    def usage_summary(self, account: str, start: date, end: date):
        """Get usage summary for an account over a date range."""
        ...
```

### 5.2 Export Functionality

Add CSV/JSON export commands:

```bash
# Proposed CLI
qhist-export --machine derecho --format csv --output usage.csv \
    --start 20250101 --end 20250131 --query "SELECT * FROM daily_summary"
```

### 5.3 Data Validation

Add Pydantic models for incoming qhist records:

```python
from pydantic import BaseModel, validator

class QhistRecord(BaseModel):
    id: str
    short_id: int | None
    user: str | None
    elapsed: int | None

    @validator("elapsed")
    def elapsed_non_negative(cls, v):
        if v is not None and v < 0:
            raise ValueError("elapsed time cannot be negative")
        return v
```

### 5.4 Schema Migrations

Add Alembic for database versioning:

```
alembic/
├── env.py
├── versions/
│   └── 001_initial_schema.py
└── alembic.ini
```

This enables:
- Safe schema changes to existing databases
- Version tracking
- Rollback capability

---

## Implementation Priority

| Phase | Effort | Impact | Priority |
|-------|--------|--------|----------|
| Phase 1: Foundation | Low | High | **Now** |
| Phase 2: Testing | Medium | High | **Next** |
| Phase 3: Code Organization | Medium | Medium | When refactoring |
| Phase 4: Quality | Low-Medium | Medium | Ongoing |
| Phase 5: Features | High | Varies | As needed |

---

## Success Metrics

After completing Phases 1-3:
- [ ] Package installable via `pip install -e .`
- [ ] Test coverage > 80% for core modules
- [ ] No single module > 300 lines
- [ ] All timestamps correctly in UTC
- [ ] CI/CD running tests on push (optional)

---

## Appendix: Technical Debt Inventory

| Issue | Location | Phase | Status |
|-------|----------|-------|--------|
| No pyproject.toml | root | 1 | Pending |
| Timezone bug | sync.py:77-84 | 1 | Pending |
| Dead Python functions | charging.py:9-60 | 1 | Pending |
| Fragile path handling | sync_jobs.py:9 | 1 | Pending |
| No tests | project-wide | 2 | Pending |
| Large sync.py | sync.py (528 lines) | 3 | Pending |
| No SSH timeout | sync.py:266 | 3 | Pending |
| Duplicate date parsing | sync.py, sync_jobs.py | 3 | Pending |
| Print statements | multiple | 4 | Pending |
| Magic numbers | charging.py:34,37 | 4 | Pending |
| Duplicate charging logic | charging.py | 4 | Pending |
