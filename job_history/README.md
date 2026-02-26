# QHist Queries

A SQLite database and Python toolkit for collecting and analyzing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

This project parses PBS accounting logs from NCAR's HPC systems, stores records in local SQLite databases, and provides a foundation for usage analysis.

**Features:**
- Optimized schema with 5-10x query performance via normalization and composite indexes
- Separate database per machine for independent management
- Pre-computed charging calculations in materialized table (`qos_factor` support built in)
- Bulk sync with duplicate detection and foreign key resolution
- Handles job arrays (e.g., `6049117[28]`)
- Python query interface for common usage analysis patterns
- Daily summary tables for fast historical queries

## Quick Start

```bash
# Initialize databases (creates both casper.db and derecho.db)
make init-db

# Sync a date range
make sync-all START=20250801 END=20251123

# Run the built-in examples with your database
python -m job_history.queries
```

## Project Structure

```
hpc-usage-queries/
├── bin/
│   └── jobhist              # qhist-compatible job query frontend (DB-backed)
├── job_history/             # Python package
│   ├── cli.py               # Main CLI entry point (Click-based)
│   ├── sync_cli/            # Sync command implementations
│   │   ├── common.py        # Shared Click decorators/utilities
│   │   └── sync.py          # jobhist sync command
│   ├── wrappers/            # Convenience entry points for selective deployment
│   │   ├── jobhist_sync.py      # jobhist-sync → jobhist sync
│   │   ├── jobhist_history.py   # jobhist-history → jobhist history
│   │   └── jobhist_resource.py  # jobhist-resource → jobhist resource
│   ├── tests/               # Test suite (pytest)
│   │   ├── conftest.py      # Shared fixtures (in-memory DB, job data)
│   │   └── fixtures/        # PBS log fixtures for integration tests
│   ├── models.py            # SQLAlchemy ORM models
│   ├── database.py          # Engine/session management with PRAGMA optimizations
│   ├── jobhist_compat.py    # DB-backed record retrieval for qhist frontend
│   ├── sync.py              # FK resolution, charge calculation
│   ├── queries.py           # High-level query interface
│   ├── charging.py          # Machine-specific charging rules
│   ├── summary.py           # Daily summary generation
│   ├── pbs_parsers.py       # PBS field/date parsers and record transformation
│   ├── pbs_read_logs.py     # Local PBS log file scanning and streaming
│   ├── exporters.py         # Data export formats (JSON, CSV, markdown)
│   ├── SCHEMA.md            # Complete schema documentation
│   └── log_config.py        # Logging configuration
├── data/
│   ├── casper.db            # Casper jobs (gitignored)
│   └── derecho.db           # Derecho jobs (gitignored)
└── Makefile                 # Convenience targets
```

## Database Schema

### Core Tables

**jobs**: Job records with foreign keys to normalized lookup tables
- Auto-increment primary key (handles scheduler ID wrap-around)
- Foreign keys: `user_id`, `account_id`, `queue_id`
- Hybrid properties: `user`, `account`, `queue` (transparently fetch from lookup tables)
- Resource allocations: `numcpus`, `numgpus`, `numnodes`, `memory`
- Timestamps in UTC: `submit`, `start`, `end`, `elapsed`
- Unique constraint on `(job_id, submit)` prevents duplicates

**users, accounts, queues**: Normalized lookup tables
- Map IDs to names for efficient integer-based joins
- ~3,500 users, ~1,300 accounts, ~150 queues

**job_charges**: Materialized charging calculations
- Pre-computed: `cpu_hours`, `gpu_hours`, `memory_hours`
- `qos_factor` (default 1.0) reserved for future QoS-based scaling
- 1:1 with jobs table for instant charge lookups
- Eliminates on-the-fly calculation overhead

**daily_summary**: Aggregated usage by date/user/account/queue
- Fast historical queries without scanning full jobs table
- Uses foreign keys with hybrid properties for backward compatibility
- NULL FKs represent 'NO_JOBS' markers for empty days

### Performance Optimizations

**Composite indexes** for common query patterns:
- `ix_jobs_queue_end` - Primary query: filter by queue and date range
- `ix_jobs_queue_user_end`, `ix_jobs_queue_account_end` - User/account filtering
- `ix_daily_summary_*_date` - Fast daily summary lookups

**SQLite PRAGMA settings**:
- WAL mode for concurrent reads during writes
- 64MB cache, 256MB memory-mapped I/O
- Foreign key enforcement enabled

See [SCHEMA.md](SCHEMA.md) for complete details.

## SQL Query Examples

```sql
-- Top users by CPU hours in date range
SELECT u.username, SUM(jc.cpu_hours) as total_cpu_hours
FROM jobs j
JOIN users u ON j.user_id = u.id
JOIN job_charges jc ON j.id = jc.job_id
JOIN queues q ON j.queue_id = q.id
WHERE q.queue_name IN ('cpu', 'cpudev')
  AND j.end >= '2025-01-01' AND j.end < '2025-02-01'
GROUP BY u.username
ORDER BY total_cpu_hours DESC
LIMIT 10;

-- GPU usage by account with job counts
SELECT a.account_name,
       COUNT(*) as job_count,
       SUM(jc.gpu_hours) as total_gpu_hours
FROM jobs j
JOIN accounts a ON j.account_id = a.id
JOIN job_charges jc ON j.id = jc.job_id
JOIN queues q ON j.queue_id = q.id
WHERE q.queue_name IN ('gpu', 'gpudev')
  AND j.end >= '2025-01-01'
GROUP BY a.account_name;

-- Average wait time by queue
SELECT q.queue_name,
       AVG(strftime('%s', j.start) - strftime('%s', j.submit))/60.0 as avg_wait_min
FROM jobs j
JOIN queues q ON j.queue_id = q.id
WHERE j.start IS NOT NULL
GROUP BY q.queue_name;
```

## CLI Sync Usage

The `jobhist sync` command parses PBS accounting logs into the local database:

```bash
# Sync a single date
jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2025-11-21 -v

# Sync a date range
jobhist sync -m casper -l ./data/pbs_logs/casper --start 2025-11-01 --end 2025-11-30 -v

# Dry run (parse but don't insert)
jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2025-11-21 --dry-run -v

# Force re-sync of already-summarized dates
jobhist sync -m derecho -l ./data/pbs_logs/derecho -d 2025-11-21 --force -v
```

**Note:** PBS log parsing populates `cpu_type` and `gpu_type` fields from PBS select strings.

During sync, the system:
1. Parses job data from local PBS accounting logs
2. Resolves foreign keys (creates new users/accounts/queues as needed)
3. Inserts jobs with duplicate detection
4. Calculates and stores charges immediately
5. Updates daily summary table

## Charging Rules

Charges are computed during import using machine-specific rules and stored in the `job_charges` table.

**Derecho:**
- Production CPU queues: `numnodes × 128 cores/node × elapsed_hours`
- Production GPU queues: `numnodes × 4 GPUs/node × elapsed_hours`
- Dev queues: actual resources used (not full-node)
- Memory-hours: `memory_gb × elapsed_hours`

**Casper:**
- CPU-hours: `numcpus × elapsed_hours`
- GPU-hours: `numgpus × elapsed_hours`
- Memory-hours: `memory_gb × elapsed_hours`

## Python Query Interface

The `JobQueries` class provides a high-level Python API:

```python
from datetime import date, timedelta
from job_history import get_session, JobQueries

# Connect to database
session = get_session("derecho")
queries = JobQueries(session, "derecho")

# Usage by group (user, account, or queue)
end = date.today()
start = end - timedelta(days=30)
cpu_by_user = queries.usage_by_group('cpu', 'user', start, end)

# Each result: {'label': username, 'usage_hours': float, 'job_count': int}
for result in sorted(cpu_by_user, key=lambda x: x['usage_hours'], reverse=True)[:5]:
    print(f"{result['label']}: {result['usage_hours']:,.0f} CPU-hours ({result['job_count']:,} jobs)")

# Job size/wait distributions
job_sizes = queries.job_sizes_by_resource('cpu', start, end)
job_waits = queries.job_waits_by_resource('gpu', start, end)

session.close()
```

**Available methods:**
- `usage_by_group(resource, group_by, start, end)` - Aggregate usage by user/account/queue
- `job_sizes_by_resource(resource, start, end)` - Job size distributions
- `job_waits_by_resource(resource, start, end)` - Queue wait time distributions
- `job_durations(resource, start, end)` - Runtime distributions
- `usage_history(resource, group_by, start, end, period)` - Time series data

See `job_history/queries.py` for complete API documentation.

## Daily Summary Programmatic Access

The pre-aggregated `daily_summary` table supports fast date-range queries by user, account, and queue:

```python
from datetime import date
from job_history import get_session
from job_history.queries import JobQueries

session = get_session("derecho")
queries = JobQueries(session)

rows = queries.daily_summary_report(
    start=date(2026, 2, 1),
    end=date(2026, 2, 28),
)

# Each row: {'date', 'user', 'account', 'queue',
#            'job_count', 'cpu_hours', 'gpu_hours', 'memory_hours'}
for row in rows:
    print(f"{row['date']}  {row['user']:15s}  {row['account']:12s}  "
          f"{row['queue']:10s}  {row['job_count']:5d}  "
          f"{row['cpu_hours']:10.1f} CPU-h  {row['gpu_hours']:8.1f} GPU-h")

session.close()
```

CLI equivalent:

```bash
jobhist history --start-date 2026-02-01 --end-date 2026-02-28 daily-summary
```

## qhist Frontend (bin/jobhist)

`bin/jobhist` is a drop-in replacement for the `qhist` job query tool that replaces
day-by-day PBS log scanning with a single, memory-bounded SQLAlchemy streaming query.
When the database is unavailable it falls back transparently to standard log scanning.

### Machine selection

```bash
export QHIST_MACHINE=derecho   # or casper
```

With `QHIST_MACHINE` set and the corresponding `.db` file present, all queries go to
the database.  Without it (or when the DB is missing) the tool falls back to log
scanning with a stderr warning.

### Usage

```bash
# Same flags as qhist — DB used when QHIST_MACHINE is set
QHIST_MACHINE=derecho bin/jobhist -p 20250115             # single day, tabular
QHIST_MACHINE=derecho bin/jobhist -p 20250101-20250131    # date range
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -u jsmith   # user filter
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -q gpu -A PROJ0001  # queue + account
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -r          # reverse order
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -l          # list mode
QHIST_MACHINE=derecho bin/jobhist -p 20250115 --csv       # CSV output
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -J          # JSON output
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -j 7362988  # specific job ID
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -H dec0001  # host filter (Python phase)
QHIST_MACHINE=derecho bin/jobhist -p 20250115 -w -a       # wide + averages
```

### Two-phase filtering

| Phase | Filters | How |
|-------|---------|-----|
| SQL (pre-decompression) | date range, job ID, user, account, queue, jobname, Exit_status | WHERE clauses on indexed columns |
| Python (post-decompression) | host (`-H`), waittime (`-W`), numeric `--filter` ops, exotic fields | Applied to the live PbsRecord after `to_pbs_record()` |

The stored `PbsRecord`/`DerechoRecord` is returned directly — no adapter class —
so qhist's output functions receive the same object type they always expect.

### Key implementation files

| File | Role |
|------|------|
| `bin/jobhist` | Entrypoint: arg parsing, config loading, output dispatch, DB/fallback routing |
| `job_history/jobhist_compat.py` | `db_available()`, `db_get_records()` generator with two-phase filtering |
| `job_history/models.py` | `JobRecord.to_pbs_record()` — decompress + unpickle stored record |

---

## CLI Tool

The `jobhist` command-line tool provides a unified interface for syncing data and generating reports:

```bash
jobhist --help

Commands:
  history   Time history view of job data
  resource  Resource-centric view of job data
  sync      Sync jobs from local PBS accounting logs
```

### History Reports

```bash
# Unique users/projects over time
jobhist history --start-date 2025-11-01 --end-date 2025-11-30 unique-users
jobhist history --group-by quarter unique-projects

# Jobs per user per account
jobhist history --start-date 2025-11-01 --end-date 2025-11-07 jobs-per-user

# Daily usage summary (user × account × queue; end-date defaults to today)
jobhist history --start-date 2026-02-01 daily-summary
jobhist history --start-date 2026-02-01 --end-date 2026-02-28 daily-summary
```

### Resource Reports

```bash
# Job size/wait distributions
jobhist resource --start-date 2025-11-01 --end-date 2025-11-30 cpu-job-sizes
jobhist resource --start-date 2025-11-01 --end-date 2025-11-30 gpu-job-waits

# Usage summaries (multiple formats)
jobhist resource --format json pie-user-cpu
jobhist resource --format csv pie-proj-gpu
jobhist resource --format md usage-history

# Available subcommands:
# - job-sizes, job-waits, job-durations (generic)
# - cpu-job-{sizes,waits,durations}, gpu-job-{sizes,waits,durations}
# - memory-job-{sizes,waits}
# - pie-user-{cpu,gpu}, pie-proj-{cpu,gpu}, pie-group-{cpu,gpu}
# - usage-history
```


## Convenience Wrappers

Three thin entry points expose each subcommand group independently, mirroring the
`fs-scans-{import,query,analyze}` pattern. This allows selective deployment — for
example, restricting `jobhist-sync` to administrators while making `jobhist-history`
and `jobhist-resource` available to all users.

| Command | Equivalent to | Defined in |
|---------|--------------|------------|
| `jobhist-sync` | `jobhist sync` | `job_history/wrappers/jobhist_sync.py` |
| `jobhist-history` | `jobhist history` | `job_history/wrappers/jobhist_history.py` |
| `jobhist-resource` | `jobhist resource` | `job_history/wrappers/jobhist_resource.py` |

```bash
# These are equivalent:
jobhist history --start-date 2026-02-01 daily-summary
jobhist-history --start-date 2026-02-01 daily-summary

jobhist resource --start-date 2026-01-01 pie-user-cpu
jobhist-resource --start-date 2026-01-01 pie-user-cpu
```

## Requirements

- Python 3.10+
- SQLAlchemy
- Access to PBS accounting log files

## Performance

Query performance improvements from normalized schema:
- **GPU queries**: ~0.2s for full-year aggregations
- **CPU queries**: ~3-4s for full-year aggregations across thousands of users
- **Daily summaries**: Instant lookups via pre-aggregated table
- **Complex joins**: 0.1-0.2s with composite index usage verified

Database size: ~24% larger than denormalized schema due to materialized charges and indexes, but eliminates runtime computation overhead.

## License

Internal NCAR tool.
