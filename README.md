# QHist Queries

A SQLite database and Python toolkit for collecting and analyzing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

This project fetches job history from HPC systems via the `qhist` command over SSH, stores records in a local SQLite database, and provides a foundation for usage analysis.

**Features:**
- SQLAlchemy ORM models for job records
- Bulk sync with duplicate detection
- Handles job arrays (e.g., `6049117[28]`)
- Day-by-day fetching for large date ranges

## Quick Start

```bash
# Initialize the database
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
│   ├── models.py          # SQLAlchemy ORM (CasperJob, DerechoJob)
│   ├── database.py        # Engine/session management
│   └── sync.py            # SSH fetch and bulk insert
├── scripts/
│   └── sync_jobs.py       # CLI sync script
├── docs/
│   └── schema.md          # Database schema documentation
├── data/
│   └── qhist.db           # SQLite database (gitignored)
└── Makefile               # Convenience targets
```

## Database Schema

Two tables with identical structure: `casper_jobs` and `derecho_jobs`

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

## Example Queries

```sql
-- Jobs by user for August 2025
SELECT user, COUNT(*) as jobs, SUM(elapsed)/3600.0 as total_hours
FROM derecho_jobs
WHERE submit >= '2025-08-01' AND submit < '2025-09-01'
GROUP BY user
ORDER BY total_hours DESC
LIMIT 10;

-- Average wait time by queue
SELECT queue,
       AVG(strftime('%s', start) - strftime('%s', submit))/60.0 as avg_wait_min
FROM casper_jobs
WHERE start IS NOT NULL AND submit IS NOT NULL
GROUP BY queue;

-- Memory efficiency (used vs requested)
SELECT user,
       AVG(CAST(memory AS REAL) / NULLIF(reqmem, 0)) * 100 as mem_efficiency_pct
FROM derecho_jobs
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

## Requirements

- Python 3.10+
- SQLAlchemy
- SSH access to casper/derecho with `qhist` command available

## License

Internal NCAR tool.
