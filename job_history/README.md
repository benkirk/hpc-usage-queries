# QHist Queries

A SQLite/PostgreSQL database and Python toolkit for collecting and analyzing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

This project parses PBS accounting logs from NCAR's HPC systems, stores records in local databases, and provides a foundation for usage analysis.

**Features:**
- Optimized schema with normalization and composite indexes (5-10× query speed)
- Separate database per machine for independent management
- Pre-computed charging calculations in materialized table (`qos_factor` support built in)
- Bulk sync with duplicate detection and foreign key resolution
- Handles job arrays (e.g., `6049117[28]`)
- Python query interface for common usage analysis patterns
- Daily summary tables for fast historical queries
- SQLite (default) and PostgreSQL backends

## Quick Start

**SQLite (default)** — no server required:
```bash
# Initialize databases (creates casper.db and derecho.db)
make init-db

# Sync a date range
jobhist sync -m derecho -l ./data/pbs_logs/derecho --start 2025-08-01 --end 2025-11-23

# Run history/resource reports
jobhist history --start-date 2025-11-01 --end-date 2025-11-30 daily-summary
jobhist resource --start-date 2025-11-01 --end-date 2025-11-30 cpu-job-sizes
```

**PostgreSQL** — shared server, same commands:
```bash
# Install the postgres extras and start a server (or use compose.yaml)
pip install 'hpc-usage-queries[postgres]'
docker compose up -d          # starts postgres:18 on localhost:5432

# Point the tool at it (copy .env.example → .env and fill in credentials)
export JH_DB_BACKEND=postgres
export JH_PG_PASSWORD=...

# Initialize (auto-creates derecho_jobs and casper_jobs databases)
python -c "from job_history import init_db; init_db()"

# Everything else is identical
jobhist sync -m derecho -l ./data/pbs_logs/derecho --start 2025-08-01 --end 2025-11-23
```

## Configuration

| Variable | Default | Purpose |
|----------|---------|---------|
| `JH_DB_BACKEND` | `sqlite` | `sqlite` or `postgres` |
| `JOB_HISTORY_DATA_DIR` | `data/` | SQLite: directory for `{machine}.db` files |
| `QHIST_DERECHO_DB` | `{data_dir}/derecho.db` | SQLite: override Derecho DB path |
| `QHIST_CASPER_DB` | `{data_dir}/casper.db` | SQLite: override Casper DB path |
| `JH_PG_HOST` | `localhost` | PostgreSQL host |
| `JH_PG_PORT` | `5432` | PostgreSQL port |
| `JH_PG_USER` | `postgres` | PostgreSQL user |
| `JH_PG_PASSWORD` | — | PostgreSQL password |
| `JH_PG_DERECHO_DB` | `derecho_jobs` | Override Derecho database name |
| `JH_PG_CASPER_DB` | `casper_jobs` | Override Casper database name |
| `JH_PG_REQUIRE_SSL` | `false` | Require SSL/TLS for PostgreSQL |

Copy `.env.example` → `.env` for non-default configuration.

## Project Structure

```
job_history/
  __init__.py              Public API surface
  cli.py                   Unified jobhist CLI (Click)
  exporters.py             Data export formats (JSON, CSV, markdown, dat)
  log_config.py            Logging helpers
  qhist_plugin.py          DB-backed record retrieval for bin/qhist-db

  database/                ── database management ──────────────────────
    __init__.py            Re-exports all public names
    config.py              JobHistoryConfig — env-var / dotenv loading
    models.py              ORM models: Job, JobCharge, JobRecord,
                             DailySummary, User, Account, Queue,
                             LookupCache, LookupMixin
    session.py             Engine/session factory, PRAGMA tuning, init_db

  sync/                    ── data ingestion ────────────────────────────
    __init__.py            Re-exports + sync_pbs_logs_bulk compat wrapper
    base.py                SyncBase ABC
    importer.py            SyncPBSLogs, JobImporter
    pbs.py                 PBS parsers + log-file reader (merged)
    charging.py            derecho_charge(), casper_charge()
    summary.py             generate_daily_summary(), get_summarized_dates()
    utils.py               normalize_datetime_to_naive(), safe_int/float, …
    cli.py                 `jobhist sync` Click command + shared decorators
    slurm.py               SyncSLURMLogs stub (future)

  queries/                 ── analytics ─────────────────────────────────
    __init__.py            Re-exports JobQueries, QueryConfig, builders
    jobs.py                JobQueries — high-level query API
    builders.py            PeriodGrouper, ResourceTypeResolver

  wrappers/                ── selective-deployment entry points ─────────
    jobhist_sync.py        jobhist-sync  → jobhist sync
    jobhist_history.py     jobhist-history → jobhist history
    jobhist_resource.py    jobhist-resource → jobhist resource

  tests/                   ── test suite ────────────────────────────────
    conftest.py            Shared fixtures (in-memory DB, job data)
    fixtures/              PBS log fixtures for integration tests
    test_*.py              163 tests, run with pytest

bin/
  qhist-db                 qhist-compatible frontend, DB-backed via qhist_plugin
data/
  casper.db                Casper jobs (gitignored)
  derecho.db               Derecho jobs (gitignored)
```

## Database Schema

### Core Tables

**jobs** — Job records with FK lookups
- Auto-increment PK (handles scheduler ID wrap-around)
- FKs: `user_id`, `account_id`, `queue_id` → lookup tables
- Hybrid properties `user`, `account`, `queue` — read/write text transparently
- Resource fields: `numcpus`, `numgpus`, `numnodes`, `memory`, `cputype`, `gputype`
- UTC timestamps: `submit`, `eligible`, `start`, `end`, `elapsed`
- Unique constraint on `(job_id, submit)` prevents duplicates

**users / accounts / queues** — Normalized lookup tables (integer FK joins)

**job_charges** — Materialized charging calculations (1:1 with jobs)
- Pre-computed: `cpu_hours`, `gpu_hours`, `memory_hours`
- `qos_factor` (default 1.0) reserved for future QoS scaling

**job_records** — Gzip-compressed pickled `PbsRecord` objects (1:1 with jobs)
- Enables `bin/qhist-db` to return original PBS records without re-scanning logs

**daily_summary** — Pre-aggregated by `(date, user_id, account_id, queue_id)`
- NULL FKs = NO_JOBS marker rows for empty days
- Fast historical queries without scanning the full jobs table

### Performance

**SQLite PRAGMA settings** (per-engine, registered in `database/session.py`):
- WAL mode, 64 MB cache, 256 MB memory-mapped I/O, foreign key enforcement

**Composite indexes**: `ix_jobs_user_account`, `ix_jobs_submit_end`, `ix_jobs_{user,account,queue}_submit`, `ix_daily_summary_date`, `ix_daily_summary_user_account`

See [SCHEMA.md](SCHEMA.md) for full schema documentation.

## Sync

### CLI

```bash
# Single date
jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29 -v

# Date range
jobhist sync -m casper -l ./data/pbs_logs/casper --start 2026-01-01 --end 2026-01-31

# Dry run (parse, don't insert)
jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29 --dry-run -v

# Force re-sync of already-summarized dates
jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2026-01-29 --force
```

PBS log sync populates `cpu_type` and `gpu_type` from PBS select strings — these fields are unavailable from other sources.

### Python API

```python
from job_history import get_session, init_db
from job_history.sync import SyncPBSLogs, sync_pbs_logs_bulk

# OOP interface (preferred)
engine = init_db("derecho")
session = get_session("derecho", engine)
syncer = SyncPBSLogs(session, "derecho")
stats = syncer.sync("./data/pbs_logs/derecho", start_date="2026-01-01", end_date="2026-01-31")

# Backward-compat function wrapper
stats = sync_pbs_logs_bulk(session, "derecho", "./data/pbs_logs/derecho",
                           start_date="2026-01-01", end_date="2026-01-31")
# stats: {fetched, inserted, errors, days_summarized, days_failed, days_skipped}
```

### Sync pipeline (per day)

1. Parse PBS accounting log → stream `PbsRecord` objects
2. Resolve FKs (get-or-create user/account/queue via `LookupCache`)
3. Bulk-insert new jobs (duplicate detection via unique constraint)
4. Calculate and store charges in `job_charges`
5. Compress-pickle raw `PbsRecord` into `job_records`
6. Generate `daily_summary` aggregation

## Charging Rules

Computed at import time by `sync/charging.py`, stored in `job_charges`.

**Derecho:**
- Production CPU: `numnodes × 128 cores/node × elapsed_hours`
- Production GPU: `numnodes × 4 GPUs/node × elapsed_hours`
- Dev queues: actual resources (`numcpus` / `numgpus`)
- Memory: `memory_gb × elapsed_hours`

**Casper:**
- CPU: `numcpus × elapsed_hours`
- GPU: `numgpus × elapsed_hours`
- Memory: `memory_gb × elapsed_hours`

## Query API

```python
from datetime import date, timedelta
from job_history import get_session, JobQueries

session = get_session("derecho")
queries = JobQueries(session, "derecho")

end = date.today()
start = end - timedelta(days=30)

# Usage by group (user / account / queue)
cpu_by_user = queries.usage_by_group('cpu', 'user', start, end)
# [{'label': username, 'usage_hours': float, 'job_count': int}, ...]

# Job size / wait distributions
job_sizes = queries.job_sizes_by_resource('cpu', 'node', start, end)
job_waits = queries.job_waits_by_resource('gpu', 'gpu', start, end)

# Duration histograms, usage history time series
durations = queries.job_durations('gpu', start, end, period='month')
history   = queries.usage_history(start, end, period='quarter')

# Daily summary
rows = queries.daily_summary_report(start=start, end=end)
# [{'date', 'user', 'account', 'queue', 'job_count', 'cpu_hours', 'gpu_hours', 'memory_hours'}, ...]

session.close()
```

Key methods: `usage_by_group`, `job_sizes_by_resource`, `job_waits_by_resource`, `job_durations`, `job_memory_per_rank`, `usage_history`, `daily_summary_report`, `jobs_by_entity_period`, `unique_users_by_period`, `unique_projects_by_period`.

See `queries/jobs.py` for complete API.

### Period helpers (`queries/builders.py`)

```python
from job_history.queries.builders import PeriodGrouper, ResourceTypeResolver

# Day / month / quarter / year grouping for SQLAlchemy queries
func = PeriodGrouper.get_period_func('quarter', Job.end)

# Aggregate monthly→quarterly in Python
quarterly = PeriodGrouper.aggregate_quarters(monthly_rows, 'job_count')

# Resolve 'cpu'/'gpu'/'all' to queue names + charge field
queues, hours_field = ResourceTypeResolver.resolve('gpu', 'derecho', JobCharge)
```

## History and Resource Reports (CLI)

```bash
# History subcommands
jobhist history -m derecho --start-date 2026-01-01 --end-date 2026-01-31 \
    [daily-summary | jobs-per-user | jobs-per-project | unique-users | unique-projects]

# Resource subcommands  (writes .dat/.json/.csv/.md files)
jobhist resource -m derecho --start-date 2026-01-01 --end-date 2026-01-31 \
    [cpu-job-sizes | gpu-job-sizes | job-sizes
     cpu-job-waits | gpu-job-waits | job-waits | memory-job-waits
     cpu-job-durations | gpu-job-durations
     cpu-job-memory-per-rank | gpu-job-memory-per-rank
     pie-user-cpu | pie-user-gpu | pie-proj-cpu | pie-proj-gpu
     pie-group-cpu | pie-group-gpu | usage-history | memory-job-sizes]

# Output format (default: dat)
jobhist resource --format json --start-date … pie-user-cpu
```

## Convenience Wrappers

Thin entry points for selective deployment (e.g., restrict sync to admins):

| Command | Equivalent |
|---------|-----------|
| `jobhist-sync` | `jobhist sync` |
| `jobhist-history` | `jobhist history` |
| `jobhist-resource` | `jobhist resource` |

## `bin/qhist-db` Frontend

Drop-in replacement for `qhist` that replaces day-by-day PBS log scanning with a single memory-bounded SQLAlchemy streaming query. Transparent fallback to log scanning when no DB exists.

```bash
export QHIST_MACHINE=derecho   # enables DB mode
bin/qhist-db -p 20260115             # tabular, single day
bin/qhist-db -p 20260101-20260131    # date range
bin/qhist-db -p 20260115 -u jsmith   # user filter (SQL phase)
bin/qhist-db -p 20260115 -H dec0001  # host filter (Python phase, post-decompression)
```

Two-phase filtering: SQL-translatable fields (date, job ID, user, account, queue, jobname, exit status) are pushed into WHERE clauses; everything else (host, intra-day time, numeric operators) is applied in Python after decompressing the stored `PbsRecord`.

Key file: `job_history/qhist_plugin.py` — `db_available()` + `db_get_records()` generator.

## Requirements

- Python 3.10+
- SQLAlchemy, click, rich, python-dotenv, pbsparse
- PostgreSQL extras: `pip install 'job_history[postgres]'`

## License

Internal NCAR tool.
