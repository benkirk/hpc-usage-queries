# QHist Queries

A SQLite database and Python toolkit for collecting and analyzing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

This project fetches job history from HPC systems via the `qhist` command over SSH, stores records in local SQLite databases, and provides a foundation for usage analysis.

**Features:**
- Optimized schema with 5-10x query performance via normalization and composite indexes
- Separate database per machine for independent management
- Pre-computed charging calculations in materialized table
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
python -m qhist_db.queries
```

## Project Structure

```
qhist-queries/
├── qhist_db/              # Python package
│   ├── models.py          # SQLAlchemy ORM models
│   ├── database.py        # Engine/session management with PRAGMA optimizations
│   ├── sync.py            # SSH fetch, FK resolution, charge calculation
│   ├── queries.py         # High-level query interface
│   ├── charging.py        # Machine-specific charging rules
│   ├── summary.py         # Daily summary generation
│   ├── parsers.py         # qhist output parsers
│   ├── remote.py          # SSH remote execution
│   └── log_config.py      # Logging configuration
├── scripts/
│   └── sync_jobs.py       # CLI sync script
├── tests/                 # Test suite
├── docs/
│   └── schema.md          # Complete schema documentation
├── data/
│   ├── casper.db          # Casper jobs (gitignored)
│   └── derecho.db         # Derecho jobs (gitignored)
└── Makefile               # Convenience targets
```

## Database Schema

### Core Tables

**jobs**: Job records with foreign keys to normalized lookup tables
- Auto-increment primary key (handles scheduler ID wrap-around)
- Foreign keys: `user_id`, `account_id`, `queue_id`
- Text fields preserved: `user`, `account`, `queue`
- Resource allocations: `numcpus`, `numgpus`, `numnodes`, `memory`
- Timestamps in UTC: `submit`, `start`, `end`, `elapsed`
- Unique constraint on `(job_id, submit)` prevents duplicates

**users, accounts, queues**: Normalized lookup tables
- Map IDs to names for efficient integer-based joins
- ~3,500 users, ~1,300 accounts, ~150 queues

**job_charges**: Materialized charging calculations
- Pre-computed: `cpu_hours`, `gpu_hours`, `memory_hours`
- 1:1 with jobs table for instant charge lookups
- Eliminates on-the-fly calculation overhead

**daily_summary**: Aggregated usage by date/user/account/queue
- Fast historical queries without scanning full jobs table
- Includes both text fields and foreign keys

### Performance Optimizations

**Composite indexes** for common query patterns:
- `ix_jobs_queue_end` - Primary query: filter by queue and date range
- `ix_jobs_queue_user_end`, `ix_jobs_queue_account_end` - User/account filtering
- `ix_daily_summary_*_date` - Fast daily summary lookups

**SQLite PRAGMA settings**:
- WAL mode for concurrent reads during writes
- 64MB cache, 256MB memory-mapped I/O
- Foreign key enforcement enabled

See [docs/schema.md](../docs/schema.md) for complete details.

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

```bash
# Sync specific date
python scripts/sync_jobs.py -m derecho -d 20251121 -v

# Sync date range
python scripts/sync_jobs.py -m casper --start 20250801 --end 20250831 -v

# Dry run (fetch but don't insert)
python scripts/sync_jobs.py -m derecho -d 20251121 --dry-run -v
```

During sync, the system:
1. Fetches job data via SSH + qhist
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
from qhist_db import get_session, JobQueries

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

See `qhist_db/queries.py` for complete API documentation.

## CLI Tool

The `qhist-report` command-line tool provides convenient data export:

### History Reports

```bash
# Unique users/projects over time
qhist-report history --start-date 2025-11-01 --end-date 2025-11-30 unique-users
qhist-report history --group-by quarter unique-projects

# Jobs per user per account
qhist-report history --start-date 2025-11-01 --end-date 2025-11-07 jobs-per-user
```

### Resource Reports

```bash
# Job size/wait distributions
qhist-report resource --start-date 2025-11-01 --end-date 2025-11-30 cpu-job-sizes
qhist-report resource --start-date 2025-11-01 --end-date 2025-11-30 gpu-job-waits

# Usage summaries (multiple formats)
qhist-report resource --format json pie-user-cpu
qhist-report resource --format csv pie-proj-gpu
qhist-report resource --format md usage-history

# Available subcommands:
# - job-sizes, job-waits, job-durations (generic)
# - cpu-job-{sizes,waits,durations}, gpu-job-{sizes,waits,durations}
# - memory-job-{sizes,waits}
# - pie-user-{cpu,gpu}, pie-proj-{cpu,gpu}, pie-group-{cpu,gpu}
# - usage-history
```

## Requirements

- Python 3.10+
- SQLAlchemy
- SSH access to casper/derecho with `qhist` command available

## Performance

Query performance improvements from normalized schema:
- **GPU queries**: ~0.2s for full-year aggregations
- **CPU queries**: ~3-4s for full-year aggregations across thousands of users
- **Daily summaries**: Instant lookups via pre-aggregated table
- **Complex joins**: 0.1-0.2s with composite index usage verified

Database size: ~24% larger than denormalized schema due to materialized charges and indexes, but eliminates runtime computation overhead.

## License

Internal NCAR tool.
