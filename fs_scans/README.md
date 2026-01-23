# GPFS Policy Scan Tools

Tools for parsing GPFS policy scan log files and computing **directory-level metrics**. This includes both a streaming parser for quick analysis and a database importer for persistent storage and querying.

## Tools

| Tool | Purpose |
|------|---------|
| `parse_gpfs_scan.py` | Streaming parser - quick analysis, no persistence |
| `scan_to_db.py` | Database importer - SQLite storage for complex queries |
| `query_db.py` | Query interface for the SQLite database |

## Overview

Both tools process GPFS scan logs and aggregate statistics at the directory level:

| Metric | Non-Recursive | Recursive |
|--------|---------------|-----------|
| File count | Direct children only | All descendants |
| Total size | Direct children only | All descendants |
| Max access time | Direct children only | All descendants |

Additionally, it tracks **single-owner directories** - directories where all recursive contents share a single owner (user_id).

## Installation

Requires Python 3.10+ with `click` and `rich`. From the project root:

```bash
source etc/config_env.sh
```

## Usage

```bash
python fs_scans/parse_gpfs_scan.py <input_file> [options]
```

### Input Files

- Plain text log files (`.log`)

### Options

| Option | Description |
|--------|-------------|
| `-o, --output FILE` | Write results to TSV file |
| `-d, --min-depth N` | Only report directories at depth >= N (default: 3) |
| `-s, --single-owner-only` | Only report single-owner directories |
| `-u, --owner-id UID` | Filter to directories owned entirely by UID |
| `-n, --max-results N` | Limit output to N directories |
| `--sort-by FIELD` | Sort by: `size_recursive`, `size`, `files_recursive`, `files`, `atime_recursive`, `atime`, `path` |
| `-p, --progress-interval N` | Progress reporting interval (default: 1M lines) |

### Examples

```bash
# Basic usage - show top 50 directories by recursive size
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log

# Export to TSV for analysis
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log -o results.tsv

# Find single-owner directories for a specific user
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log \
    --owner-id 12345 --min-depth 4

# Sort by file count, limit results
python fs_scans/parse_gpfs_scan.py fs_scans/20260111_csfs1_asp.list.list_all.log \
    --sort-by files_recursive --max-results 100
```

## Log File Format

The parser expects GPFS policy scan output with lines in this format:

```
<node> inode gen snapshot  key=value pairs -- /path
```

Key fields extracted:
- `s=` FILE_SIZE (bytes)
- `u=` USER_ID (numeric)
- `p=` permissions (first char: `-`=file, `d`=directory)
- `ac=` ACCESS_TIME (timestamp)

Directory entries are skipped; only files contribute to statistics.

## Memory Efficiency

The parser uses a streaming approach with constant memory per directory:
- ~64 bytes per directory (6 integers, 2 datetimes, 1 boolean)
- No file-level data stored
- Single-owner tracking uses O(1) space (one int + one bool per directory)

For a filesystem with millions of files but hundreds of thousands of directories, memory usage remains manageable.

## Output Formats

### Table (stdout)

```
Directory Statistics (20 directories)
┏━━━━━━━━━━━━┳━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━━━━┳━━━━━━━━━┳━━━━━━━┓
┃ Directory  ┃ Files ┃ Size    ┃ Max Atime┃ Files(R)┃ Size (R) ┃Max At(R)┃ Owner ┃
┡━━━━━━━━━━━━╇━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━━━━╇━━━━━━━━━╇━━━━━━━┩
│ /gpfs/...  │ 1,234 │ 45.6 GB │ 2024-01..│  56,789 │   1.2 TB │ 2025-12.│ 12345 │
└────────────┴───────┴─────────┴──────────┴─────────┴──────────┴─────────┴───────┘
```

### TSV (--output)

Tab-separated values with columns:
```
directory  file_count  total_size  max_atime  file_count_recursive  total_size_recursive  max_atime_recursive  owner_id  single_owner
```

## Available Data Files

| Filesystem | Size | Lines (approx) |
|------------|------|----------------|
| asp | 3 GB | ~10M |
| cisl | 36 GB | ~100M+ |
| eol | 6 GB | ~20M |
| hao | 40 GB | ~100M+ |

Data files should be decompressed before processing.

---

## Database Importer (scan_to_db.py)

For persistent storage and complex queries, use the database importer to load scan data into SQLite.

### Usage

```bash
python -m fs_scans.scan_to_db <input_file> [options]
```

### Options

| Option | Description |
|--------|-------------|
| `--db PATH` | Override database path (default: `fs_scans/<filesystem>.db`) |
| `-f, --filesystem NAME` | Override filesystem name (default: extracted from filename) |
| `--batch-size N` | Batch size for DB updates (default: 10000) |
| `-p, --progress-interval N` | Progress reporting interval (default: 1M lines) |
| `--replace` | Drop and recreate tables before import |
| `-w, --workers N` | Number of worker processes for parsing (default: 1) |
| `--echo` | Echo SQL statements (for debugging) |

### Examples

```bash
# Import a scan file (database auto-created as fs_scans/asp.db)
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log

# Import compressed file with custom database path
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log --db /tmp/asp.db

# Replace existing data
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log --replace

# Use parallel workers for faster parsing (best with uncompressed files)
python -m fs_scans.scan_to_db fs_scans/20260111_csfs1_asp.list.list_all.log --workers 4
```

### Two-Pass Algorithm

The importer uses a two-pass algorithm:

**Pass 1: Directory Discovery** - Identifies all directories and builds a normalized hierarchy in the database.

**Pass 2: Statistics Accumulation** - Re-scans the file to accumulate file statistics into each directory.

### Pass 1 Implementation

Since GPFS scan files explicitly list all directories as separate lines, Pass 1 uses a memory-optimized two-phase approach with SQLite staging:

| Phase | Operation | Data Structures |
|-------|-----------|-----------------|
| 1a | Stream to staging | SQLite `staging_dirs` table (batch inserts) |
| 1b | Insert to DB | Read from staging ORDER BY depth, build `path_to_id` |

**How it works:**

1. **Phase 1a** - Streams directories to a temporary SQLite staging table with batch inserts (10K rows per batch). Extracts inode and fileset_id for unique identification.
2. **Phase 1b** - Reads from staging table ordered by depth (ensuring parents exist before children), inserts into directories table, and builds `path_to_id` dictionary.
3. Staging table is dropped after use to reclaim space.

**Memory optimization:** By using SQLite staging instead of an in-memory list, peak memory is reduced from ~220 bytes/dir to ~120 bytes/dir (only `path_to_id` dict in memory). For 1M directories, this saves ~100MB of peak memory.

**Progress tracking:** Phase 1 reports line count, directory count, and inferred file count. Pass 2 uses the known line count for a determinate progress bar with percentage completion.

No deduplication or parent directory discovery is needed since all directories are explicitly listed in the scan output.

### Parallel Processing

Both Phase 1a (directory discovery) and Pass 2 (stats accumulation) support parallel processing with the `--workers` flag:

- Workers handle CPU-bound regex parsing of log lines
- Main process handles file I/O and database writes (SQLite single-writer constraint)
- Queue-based communication between workers and main process
- Phase 1b remains sequential (parent-child ordering requirement)

**Note:** Parallel workers are most effective when the input file is stored on fast local storage.

### Database Schema

The importer creates two tables:

**directories** - Normalized directory hierarchy
- `dir_id` - Primary key
- `parent_id` - Foreign key to parent directory
- `name` - Directory basename
- `depth` - Depth in hierarchy (for efficient queries)

**directory_stats** - Aggregated statistics per directory
- `dir_id` - Foreign key to directories
- `file_count_nr` / `file_count_r` - Non-recursive / recursive file counts
- `total_size_nr` / `total_size_r` - Non-recursive / recursive sizes
- `max_atime_nr` / `max_atime_r` - Non-recursive / recursive max access times
- `owner_uid` - Single owner UID (NULL if multiple owners)
