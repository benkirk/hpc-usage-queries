# QHist-Queries Refactoring Plan

This document tracks the remaining work for the qhist-queries codebase after completing Phases 1-4.

## Completed Work (Phases 1-4)

### Phase 1: Foundation ✓
- Added `pyproject.toml` for modern Python packaging
- Added `requirements.txt` for pip users
- Fixed timezone handling (Mountain Time → UTC conversion using `zoneinfo`)
- Documented charging functions as available for ad-hoc use
- Fixed cross-platform path handling in sync_jobs.py

### Phase 2: Testing Infrastructure ✓
- Created `tests/` directory with pytest configuration
- Added 57 tests covering:
  - Timestamp parsing and timezone conversion
  - Type conversion (int, float, job ID)
  - Charging calculations for Derecho and Casper
  - ORM models and constraints
  - Summary generation
  - Date range utilities

### Phase 3: Code Organization ✓
- Split `sync.py` (528→280 lines) into:
  - `parsers.py` (258 lines) - Field parsing and type conversion
  - `remote.py` (100 lines) - SSH command execution with 5-minute timeout
  - `sync.py` (280 lines) - Orchestration only
- Consolidated date utilities in `parsers.py`
- All modules now under 300 lines

### Phase 4: Quality Improvements ✓
- Added `logging.py` module with configurable logger
- Extracted machine constants:
  - `DERECHO_CORES_PER_NODE = 128`
  - `DERECHO_GPUS_PER_NODE = 4`
  - `BYTES_PER_GB`, `SECONDS_PER_HOUR`
- SQL views now generated from Python constants (single source of truth)

---

## Current State

**Project Size**: ~1,770 lines of Python across 11 modules
**Test Coverage**: 78 tests, covering all core functionality
**Module Sizes**: All modules ≤ 500 lines

### Module Structure
```
qhist_db/
├── __init__.py      (17 lines)  - Package exports
├── charging.py     (185 lines)  - Charging rules and SQL generation
├── database.py     (112 lines)  - Connection management
├── logging.py       (58 lines)  - Logging configuration
├── models.py       (128 lines)  - SQLAlchemy ORM models
├── parsers.py      (258 lines)  - Field parsing utilities
├── queries.py      (500 lines)  - High-level query interface
├── remote.py       (100 lines)  - SSH command execution
├── summary.py      (134 lines)  - Daily summary generation
└── sync.py         (280 lines)  - Sync orchestration
```

---

## Phase 5: Features & Scalability

### 5.1 Query Interface ✓

Added a comprehensive Python API for common queries in `qhist_db/queries.py`:

**JobQueries class provides:**
- `jobs_by_user()` - Get jobs for a user with date/status/queue filtering
- `jobs_by_account()` - Get jobs for an account with date filtering
- `jobs_by_queue()` - Get jobs for a queue with date filtering
- `usage_summary()` - Aggregate usage metrics for an account
- `user_summary()` - Aggregate usage metrics for a user
- `top_users_by_jobs()` - Get top users by job count
- `queue_statistics()` - Get statistics by queue
- `daily_summary_by_account()` - Get daily summaries for an account
- `daily_summary_by_user()` - Get daily summaries for a user

**Testing:**
- Added 21 comprehensive tests in `tests/test_queries.py`
- All tests pass (78 total tests in suite)

**Documentation:**
- Added Python Query Interface section to README.md with examples
- Included `if __name__ == "__main__":` examples in queries.py
- Documented all methods with docstrings

**Benefits:**
- High-level API eliminates need for raw SQL in most cases
- Type hints and docstrings for better IDE support
- Tested and documented for production use

### 5.2 CI/CD (Future)

Consider extending current GitHub Action for code coverage reporting

---

## Success Metrics (Completed)

After completing Phases 1-4:
- [x] Package installable via `pip install -e .`
- [x] Test coverage for core modules (57 tests)
- [x] No single module > 300 lines
- [x] All timestamps correctly in UTC
- [ ] CI/CD running tests on push (optional - Phase 5)
