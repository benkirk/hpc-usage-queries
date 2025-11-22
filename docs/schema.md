# QHist Database Schema

This document describes the database schema for storing historical job data from NCAR's Casper and Derecho supercomputing resources.

## Overview

- **Database**: SQLite (`data/qhist.db`)
- **Tables**: `casper_jobs`, `derecho_jobs` (identical structure, separate epochs)
- **Primary Key**: `short_id` (INTEGER) - unique job identifier per machine
- **Timestamps**: Stored in UTC

## Design Decisions

### Separate Tables for Each Machine

Casper and Derecho have independent job numbering systems ("epochs"). While `short_id` values may occasionally overlap between machines, using separate tables:

- Avoids the need for composite keys
- Optimizes queries that target a single machine
- Leverages SQLite's INTEGER PRIMARY KEY optimization (direct rowid mapping)

### Primary Key Choice

`short_id` (INTEGER) was chosen over `id` (TEXT) because:

- SQLite optimizes INTEGER PRIMARY KEY as the internal rowid
- Faster comparisons and index lookups
- Smaller storage footprint
- Natural duplicate detection during sync operations

The full `id` string is retained as an indexed column for reference.

### Timestamp Handling

All timestamps are converted to UTC before storage for consistency, regardless of the timezone reported by qhist.

## Schema Definition

### casper_jobs / derecho_jobs

| Column | Type | Nullable | Index | Description |
|--------|------|----------|-------|-------------|
| `short_id` | INTEGER | NO | PK | Unique job identifier |
| `id` | TEXT | YES | YES | Full job ID string |
| `name` | TEXT | YES | NO | Job name |
| `user` | TEXT | YES | YES | Username who submitted the job |
| `account` | TEXT | YES | YES | Account/allocation charged |
| `queue` | TEXT | YES | YES | Queue/partition used |
| `status` | TEXT | YES | YES | Job completion status |
| `submit` | DATETIME | YES | YES | When job was submitted (UTC) |
| `eligible` | DATETIME | YES | NO | When job became eligible to run (UTC) |
| `start` | DATETIME | YES | YES | When job started executing (UTC) |
| `end` | DATETIME | YES | YES | When job finished (UTC) |
| `elapsed` | INTEGER | YES | NO | Actual runtime in seconds |
| `walltime` | INTEGER | YES | NO | Requested walltime in seconds |
| `cputime` | INTEGER | YES | NO | Total CPU time used in seconds |
| `numcpus` | INTEGER | YES | NO | Number of CPUs allocated |
| `numgpus` | INTEGER | YES | NO | Number of GPUs allocated |
| `numnodes` | INTEGER | YES | NO | Number of nodes allocated |
| `mpiprocs` | INTEGER | YES | NO | Number of MPI processes |
| `ompthreads` | INTEGER | YES | NO | Number of OpenMP threads |
| `reqmem` | BIGINT | YES | NO | Requested memory in bytes |
| `memory` | BIGINT | YES | NO | Actual memory used in bytes |
| `vmemory` | BIGINT | YES | NO | Virtual memory used in bytes |
| `cputype` | TEXT | YES | NO | CPU type (e.g., milan) |
| `gputype` | TEXT | YES | NO | GPU type (e.g., a100) |
| `resources` | TEXT | YES | NO | Resource specification string |
| `ptargets` | TEXT | YES | NO | Placement targets |
| `cpupercent` | REAL | YES | NO | CPU utilization percentage |
| `avgcpu` | REAL | YES | NO | Average CPU usage |
| `count` | INTEGER | YES | NO | Job array count |

## Derived Metrics

These metrics can be computed from stored fields for analysis:

| Metric | Calculation | Description |
|--------|-------------|-------------|
| Wait Time | `start - submit` | Time spent in queue |
| Eligible Wait | `start - eligible` | Time from eligible to running |
| Memory Efficiency | `memory / reqmem` | Ratio of used vs requested memory |
| CPU Efficiency | `cputime / (elapsed * numcpus)` | CPU utilization ratio |
| Walltime Efficiency | `elapsed / walltime` | Ratio of used vs requested time |

## Example Queries

### Jobs by User
```sql
SELECT user, COUNT(*) as job_count, SUM(elapsed) as total_runtime
FROM derecho_jobs
GROUP BY user
ORDER BY total_runtime DESC;
```

### Average Wait Time by Queue
```sql
SELECT queue,
       AVG(strftime('%s', start) - strftime('%s', submit)) as avg_wait_seconds
FROM casper_jobs
WHERE start IS NOT NULL AND submit IS NOT NULL
GROUP BY queue;
```

### Memory Efficiency Analysis
```sql
SELECT user, account,
       AVG(CAST(memory AS REAL) / NULLIF(reqmem, 0)) as avg_mem_efficiency
FROM derecho_jobs
WHERE memory IS NOT NULL AND reqmem > 0
GROUP BY user, account;
```

## Data Sources

Data is fetched from the `qhist` command available on Casper and Derecho:

```bash
ssh derecho qhist --period 20251121 --json --format="id,short_id,..."
```

The sync process:
1. Connects via SSH to the target machine
2. Runs qhist with JSON output and specified date range
3. Parses JSON and converts timestamps to UTC
4. Inserts new records (skips existing based on `short_id`)
