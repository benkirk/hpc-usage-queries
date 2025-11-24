# QHist Queries

A SQLite database and Python toolkit for collecting and analyzing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

This project fetches job history from HPC systems via the `qhist` command over SSH, stores records in local SQLite databases, and provides a foundation for usage analysis.

**Features:**
- Separate database file per machine for easy management
- SQLAlchemy ORM models for job records
- Bulk sync with duplicate detection
- Handles job arrays (e.g., `6049117[28]`)
- Day-by-day fetching for large date ranges
- Charging views with computed resource hours (planned)
- Daily summary tables for fast usage queries (planned)
- Smart sync that skips already-processed days (planned)

## Quick Start

```bash
# Initialize databases (creates both casper.db and derecho.db)
make init-db

# Sync jobs for a specific date
make sync-derecho DATE=20251121
make sync-casper DATE=20251121

# Sync a date range
make sync-all START=20250801 END=20250831
```

## Project Structure

```
qhist-queries/
├── qhist_db/              # Python package
│   ├── models.py          # SQLAlchemy ORM (Job class)
│   ├── database.py        # Engine/session management
│   └── sync.py            # SSH fetch and bulk insert
├── scripts/
│   └── sync_jobs.py       # CLI sync script
├── docs/
│   └── schema.md          # Database schema documentation
├── data/
│   ├── casper.db          # Casper jobs (gitignored)
│   └── derecho.db         # Derecho jobs (gitignored)
└── Makefile               # Convenience targets
```

## Database Schema

Each machine has its own database file with a `jobs` table:

| Column | Type | Description |
|--------|------|-------------|
| `id` | TEXT (PK) | Full job ID including array index |
| `short_id` | INTEGER | Base job number for queries |
| `user` | TEXT | Submitting user |
| `account` | TEXT | Account charged |
| `queue` | TEXT | Queue/partition |
| `status` | TEXT | Exit status |
| `submit` | DATETIME | Submission time (UTC) |
| `start` | DATETIME | Execution start (UTC) |
| `end` | DATETIME | Job end (UTC) |
| `elapsed` | INTEGER | Runtime (seconds) |
| `walltime` | INTEGER | Requested walltime (seconds) |
| `numcpus` | INTEGER | CPUs allocated |
| `numgpus` | INTEGER | GPUs allocated |
| `numnodes` | INTEGER | Nodes allocated |
| `memory` | BIGINT | Memory used (bytes) |
| `reqmem` | BIGINT | Memory requested (bytes) |

See [docs/schema.md](docs/schema.md) for the complete schema.

## Python Query Interface

The `JobQueries` class provides a high-level Python API for common queries:

```python
from datetime import date, timedelta
from qhist_db import get_session, JobQueries

# Connect to a machine's database
session = get_session("derecho")
queries = JobQueries(session)

# Get jobs for a specific user
end_date = date.today()
start_date = end_date - timedelta(days=7)
jobs = queries.jobs_by_user("username", start=start_date, end=end_date)

# Get usage summary for an account (uses charging view for accurate hours)
summary = queries.usage_summary("NCAR0001", start=start_date, end=end_date)
print(f"Job count: {summary['job_count']}")
print(f"Total CPU-hours: {summary['total_cpu_hours']:,.2f}")
print(f"Total GPU-hours: {summary['total_gpu_hours']:,.2f}")
print(f"Total Memory-hours: {summary['total_memory_hours']:,.2f}")
print(f"Users: {', '.join(summary['users'])}")

# Get top users by job count
top_users = queries.top_users_by_jobs(start=start_date, end=end_date, limit=10)
for user_stat in top_users:
    print(f"{user_stat['user']}: {user_stat['job_count']} jobs")

# Get queue statistics
stats = queries.queue_statistics(start=start_date, end=end_date)
for stat in stats:
    print(f"{stat['queue']}: {stat['job_count']} jobs, "
          f"avg {stat['avg_elapsed_seconds']/3600:.2f} hours")

# Get daily summaries (if available)
daily = queries.daily_summary_by_user("username", start=start_date, end=end_date)
for summary in daily:
    print(f"{summary.date}: {summary.job_count} jobs")

session.close()
```

**Available query methods:**
- `jobs_by_user(user, start, end, status, queue)` - Get jobs for a user
- `jobs_by_account(account, start, end, status)` - Get jobs for an account
- `jobs_by_queue(queue, start, end)` - Get jobs for a queue
- `usage_summary(account, start, end)` - Aggregate usage for an account
- `user_summary(user, start, end)` - Aggregate usage for a user
- `top_users_by_jobs(start, end, limit)` - Get top users by job count
- `queue_statistics(start, end)` - Get statistics by queue
- `daily_summary_by_account(account, start, end)` - Get daily summaries
- `daily_summary_by_user(user, start, end)` - Get daily summaries

See `qhist_db/queries.py` for complete API documentation and examples.

## SQL Query Examples

```bash
# Query Derecho jobs
sqlite3 data/derecho.db "SELECT user, COUNT(*) as jobs FROM jobs GROUP BY user LIMIT 10;"

# Query Casper jobs
sqlite3 data/casper.db "SELECT queue, COUNT(*) FROM jobs GROUP BY queue;"
```

```sql
-- Jobs by user for August 2025 (run against derecho.db)
SELECT user, COUNT(*) as jobs, SUM(elapsed)/3600.0 as total_hours
FROM jobs
WHERE submit >= '2025-08-01' AND submit < '2025-09-01'
GROUP BY user
ORDER BY total_hours DESC
LIMIT 10;

-- Average wait time by queue
SELECT queue,
       AVG(strftime('%s', start) - strftime('%s', eligible))/60.0 as avg_wait_min
FROM jobs
WHERE start IS NOT NULL AND submit IS NOT NULL
GROUP BY queue;

-- Memory efficiency (used vs requested)
SELECT user,
       AVG(CAST(memory AS REAL) / NULLIF(reqmem, 0)) * 100 as mem_efficiency_pct
FROM jobs
WHERE memory IS NOT NULL AND reqmem > 0
GROUP BY user
HAVING COUNT(*) > 100;
```

## CLI Usage

```bash
# Sync with options
python scripts/sync_jobs.py -m derecho -d 20251121 -v
python scripts/sync_jobs.py -m casper --start 20250801 --end 20250831 -v

# Dry run (fetch but don't insert)
python scripts/sync_jobs.py -m derecho -d 20251121 --dry-run -v
```

## Charging (Planned)

The project will include charging views and daily summary tables. See [docs/charging-views-plan.md](docs/charging-views-plan.md) for details.

**Derecho charging:**
- CPU queues: core-hours = `elapsed * numnodes * 128 / 3600`
- GPU queues: GPU-hours = `elapsed * numnodes * 4 / 3600`
- Development queues: actual resources used

**Casper charging:**
- CPU-hours = `elapsed * numcpus / 3600`
- Memory-hours = `elapsed * memory_gb / 3600`

## Requirements

- Python 3.10+
- SQLAlchemy
- SSH access to casper/derecho with `qhist` command available

## License

Internal NCAR tool.
