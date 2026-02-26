# HPC Usage Queries

A collection of user-facing tools for analyzing resource usage on NSF NCAR HPC systems. This repository contains two complementary toolkits for understanding compute and storage utilization patterns.

## Overview

**QHist Queries** - Track and analyze HPC job history across NCAR's supercomputing resources
**FS Scans** - Analyze filesystem usage patterns from metadata scans

Both tools use SQLite databases for efficient querying and provide Python APIs with command-line interfaces.

## QHist Queries

Historical job data tracking for Casper and Derecho HPC systems.

**Key Features:**
- Optimized SQLite schema with high-performance queries
- Pre-computed charging calculations (CPU-hours, GPU-hours, memory-hours)
- Daily summary tables for fast historical queries
- Python query interface and CLI reporting tools
- Local PBS log sync with duplicate detection

**Quick Start:**
```bash
make init-db
jobhist sync -m derecho -l ./data/pbs_logs/derecho --start 2026-01-01
```

**Convenience wrappers** allow selective deployment:
- `jobhist-sync` — sync only (restrict to administrators)
- `jobhist-history` — time history reports
- `jobhist-resource` — resource-centric reports

**Common Use Cases:**
- Track resource consumption by user, account, or queue
- Analyze job size and wait time distributions
- Generate usage reports for allocation management
- Historical trend analysis

See [job_history/README.md](job_history/README.md) for complete documentation.

## FS Scans

Filesystem usage analysis from GPFS, Lustre, and POSIX metadata scans.

**Key Features:**
- Directory-level aggregation (recursive and non-recursive statistics)
- Pre-computed histograms for instant access-age and size-distribution queries
- Single-owner/single-group directory detection
- Handles multi-billion file filesystems efficiently
- Unified CLI with import, query, and analyze commands

**Quick Start:**
```bash
cd fs_scans
pip install -e .
fs-scans import scan.log
fs-scans query asp --min-size 10GiB
fs-scans analyze --access-history
```

**Common Use Cases:**
- Identify cold data for archival or cleanup
- Find large directories by user or path
- Track storage inefficiencies (tiny files, abandoned data)
- Generate per-user storage reports

See [fs_scans/README.md](fs_scans/README.md) for complete documentation.

## Requirements

- Python 3.10+
- SQLAlchemy
- Access to PBS accounting log files

## License

Internal NCAR tool.
